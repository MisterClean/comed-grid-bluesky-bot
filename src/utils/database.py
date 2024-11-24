import sqlite3
import pandas as pd
from datetime import datetime
from src.utils.logger import setup_logger

logger = setup_logger()

class DatabaseManager:
    def __init__(self, db_path="data/grid_data.db"):
        self.db_path = db_path
        self._ensure_db_exists()

    def _ensure_db_exists(self):
        """Create the database and table if they don't exist"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS grid_data (
                    interval_start_utc TIMESTAMP PRIMARY KEY,
                    interval_end_utc TIMESTAMP,
                    load_mw REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
            logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing database: {str(e)}")
            raise
        finally:
            conn.close()

    def _get_connection(self):
        """Get a database connection"""
        return sqlite3.connect(self.db_path)

    def get_latest_timestamp(self):
        """Get the most recent interval_end_utc from the database"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(interval_end_utc) FROM grid_data")
            result = cursor.fetchone()[0]
            return result if result else None
        finally:
            conn.close()

    def upsert_data(self, df):
        """Upsert data from a pandas DataFrame into the database"""
        if df.empty:
            logger.info("No new data to upsert")
            return 0

        conn = self._get_connection()
        try:
            # Rename the load column if needed
            if 'load.comed' in df.columns:
                df = df.rename(columns={'load.comed': 'load_mw'})

            # Convert DataFrame to list of tuples
            records = []
            for _, row in df.iterrows():
                record = (
                    row['interval_start_utc'].isoformat(),
                    row['interval_end_utc'].isoformat(),
                    float(row['load_mw'] if 'load_mw' in df.columns else row['load.comed'])
                )
                records.append(record)
            
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
        finally:
            conn.close()

    def get_data_since(self, start_time):
        """Retrieve data from the database since a given timestamp"""
        conn = self._get_connection()
        try:
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
            
            # Rename load_mw back to load.comed for compatibility with rest of app
            df = df.rename(columns={'load_mw': 'load.comed'})
            
            return df
        finally:
            conn.close()
