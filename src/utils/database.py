from typing import Optional, List, Tuple
import sqlite3
from sqlite3.dbapi2 import Connection
from contextlib import contextmanager
from datetime import datetime
import pandas as pd
from pathlib import Path
from queue import Queue, Empty
from threading import Lock

from src.interfaces import DatabaseInterface, DatabaseError
from src.utils.logger import setup_logger

logger = setup_logger()

class ConnectionPool:
    """A simple connection pool for SQLite connections."""
    
    def __init__(self, db_path: str, max_connections: int = 5):
        """Initialize the connection pool.
        
        Args:
            db_path: Path to the SQLite database file
            max_connections: Maximum number of connections to maintain
        """
        self.db_path = db_path
        self.max_connections = max_connections
        self.connections: Queue[Connection] = Queue(maxsize=max_connections)
        self.lock = Lock()
        self._initialize_pool()
    
    def _initialize_pool(self) -> None:
        """Create initial connections in the pool."""
        for _ in range(self.max_connections):
            conn = sqlite3.connect(self.db_path)
            # Enable foreign key support
            conn.execute("PRAGMA foreign_keys = ON")
            # Set journal mode to WAL for better concurrency
            conn.execute("PRAGMA journal_mode = WAL")
            self.connections.put(conn)
    
    @contextmanager
    def get_connection(self) -> Connection:
        """Get a connection from the pool.
        
        Yields:
            Connection: A database connection
            
        Raises:
            DatabaseError: If unable to get a connection
        """
        connection = None
        try:
            connection = self.connections.get(timeout=5)
            yield connection
        except Empty:
            raise DatabaseError("Timeout waiting for available database connection")
        except Exception as e:
            raise DatabaseError(f"Error getting database connection: {str(e)}")
        finally:
            if connection:
                self.connections.put(connection)
    
    def close_all(self) -> None:
        """Close all connections in the pool."""
        with self.lock:
            while not self.connections.empty():
                conn = self.connections.get()
                conn.close()

class DatabaseManager(DatabaseInterface):
    """Manages database operations with connection pooling and error handling."""
    
    MIGRATIONS = [
        """
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS grid_data (
            interval_start_utc TIMESTAMP PRIMARY KEY,
            interval_end_utc TIMESTAMP,
            load_mw REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_interval_end 
        ON grid_data(interval_end_utc)
        """
    ]
    
    def __init__(self, db_path: str = "data/grid_data.db"):
        """Initialize the database manager.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self._ensure_data_dir()
        self.pool = ConnectionPool(db_path)
        self._run_migrations()
    
    def _ensure_data_dir(self) -> None:
        """Ensure the data directory exists."""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
    
    def _run_migrations(self) -> None:
        """Run database migrations."""
        try:
            with self.pool.get_connection() as conn:
                cursor = conn.cursor()
                
                # Create schema version table if it doesn't exist
                cursor.execute(self.MIGRATIONS[0])
                
                # Get current schema version
                cursor.execute("SELECT MAX(version) FROM schema_version")
                current_version = cursor.fetchone()[0] or 0
                
                # Apply any new migrations
                for version, migration in enumerate(self.MIGRATIONS[1:], start=1):
                    if version > current_version:
                        try:
                            cursor.execute(migration)
                            cursor.execute(
                                "INSERT INTO schema_version (version) VALUES (?)",
                                (version,)
                            )
                            conn.commit()
                            logger.info(f"Applied migration version {version}")
                        except Exception as e:
                            conn.rollback()
                            raise DatabaseError(f"Migration {version} failed: {str(e)}")
                
                logger.info("Database migrations completed successfully")
        except Exception as e:
            raise DatabaseError(f"Error running migrations: {str(e)}")
    
    def get_latest_timestamp(self) -> Optional[datetime]:
        """Get the most recent interval_end_utc from the database.
        
        Returns:
            Optional[datetime]: The latest timestamp or None if no data exists
            
        Raises:
            DatabaseError: If there is an error accessing the database
        """
        try:
            with self.pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT MAX(interval_end_utc) FROM grid_data")
                result = cursor.fetchone()[0]
                return datetime.fromisoformat(result) if result else None
        except Exception as e:
            raise DatabaseError(f"Error getting latest timestamp: {str(e)}")
    
    def upsert_data(self, df: pd.DataFrame) -> int:
        """Upsert data from a pandas DataFrame into the database.
        
        Args:
            df: DataFrame containing the data to upsert
            
        Returns:
            int: Number of records affected
            
        Raises:
            DatabaseError: If there is an error upserting the data
        """
        if df.empty:
            logger.info("No new data to upsert")
            return 0

        try:
            with self.pool.get_connection() as conn:
                # Rename the load column if needed
                if 'load.comed' in df.columns:
                    df = df.rename(columns={'load.comed': 'load_mw'})

                # Convert DataFrame to list of tuples
                records: List[Tuple[str, str, float]] = [
                    (
                        row['interval_start_utc'].isoformat(),
                        row['interval_end_utc'].isoformat(),
                        float(row['load_mw'] if 'load_mw' in df.columns else row['load.comed'])
                    )
                    for _, row in df.iterrows()
                ]
                
                cursor = conn.cursor()
                cursor.executemany("""
                    INSERT OR REPLACE INTO grid_data 
                    (interval_start_utc, interval_end_utc, load_mw)
                    VALUES (?, ?, ?)
                """, records)
                
                conn.commit()
                rows_affected = cursor.rowcount
                logger.info(f"Upserted {rows_affected} records into database")
                return rows_affected
        except Exception as e:
            raise DatabaseError(f"Error upserting data: {str(e)}")
    
    def get_data_since(self, start_time: str) -> pd.DataFrame:
        """Retrieve data from the database since a given timestamp.
        
        Args:
            start_time: ISO format timestamp string
            
        Returns:
            pd.DataFrame: DataFrame containing the requested data
            
        Raises:
            DatabaseError: If there is an error retrieving the data
        """
        try:
            with self.pool.get_connection() as conn:
                query = """
                    SELECT interval_start_utc, interval_end_utc, load_mw
                    FROM grid_data
                    WHERE interval_start_utc >= ?
                    ORDER BY interval_start_utc
                """
                df = pd.read_sql_query(query, conn, params=(start_time,))
                
                # Parse timestamps and ensure they're UTC
                for col in ['interval_start_utc', 'interval_end_utc']:
                    df[col] = pd.to_datetime(df[col])
                    if df[col].dt.tz is None:
                        df[col] = df[col].dt.tz_localize('UTC')
                
                # Rename load_mw back to load.comed for compatibility
                df = df.rename(columns={'load_mw': 'load.comed'})
                
                return df
        except Exception as e:
            raise DatabaseError(f"Error retrieving data: {str(e)}")
    
    def cleanup_old_data(self, days: int = 90) -> int:
        """Clean up data older than specified days.
        
        Args:
            days: Number of days of data to keep
            
        Returns:
            int: Number of records deleted
            
        Raises:
            DatabaseError: If there is an error cleaning up data
        """
        try:
            with self.pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    DELETE FROM grid_data
                    WHERE interval_start_utc < datetime('now', '-? days')
                """, (days,))
                conn.commit()
                rows_deleted = cursor.rowcount
                logger.info(f"Deleted {rows_deleted} old records from database")
                return rows_deleted
        except Exception as e:
            raise DatabaseError(f"Error cleaning up old data: {str(e)}")
    
    def __del__(self) -> None:
        """Clean up database connections."""
        try:
            self.pool.close_all()
        except Exception as e:
            logger.error(f"Error closing database connections: {str(e)}")
