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
        """Create the database and tables if they don't exist"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            
            # Grid data table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS grid_data (
                    interval_start_utc TIMESTAMP PRIMARY KEY,
                    interval_end_utc TIMESTAMP,
                    load_mw REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # NRC power reactor status table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS nrc_reactor_status (
                    report_date TIMESTAMP,
                    unit_name TEXT,
                    power_pct REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (report_date, unit_name)
                )
            """)
            
            # EIA capacity table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS eia_capacity (
                    period TEXT,
                    plant_id TEXT,
                    generator_id TEXT,
                    net_summer_capacity_mw REAL,
                    net_winter_capacity_mw REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (period, plant_id, generator_id)
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

    def get_latest_nrc_date(self):
        """Get the most recent report date from NRC data"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(report_date) FROM nrc_reactor_status")
            result = cursor.fetchone()[0]
            return result if result else None
        finally:
            conn.close()

    def get_latest_eia_period(self):
        """Get the most recent period from EIA data"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(period) FROM eia_capacity")
            result = cursor.fetchone()[0]
            return result if result else None
        finally:
            conn.close()

    def get_nrc_data_for_date(self, report_date):
        """Get NRC data for a specific report date"""
        conn = self._get_connection()
        try:
            query = """
                SELECT report_date, unit_name, power_pct
                FROM nrc_reactor_status
                WHERE report_date = ?
            """
            df = pd.read_sql_query(query, conn, params=(report_date.isoformat(),))
            df['report_date'] = pd.to_datetime(df['report_date'])
            return df
        finally:
            conn.close()

    def get_eia_data_for_period(self, period):
        """Get EIA data for a specific period"""
        conn = self._get_connection()
        try:
            query = """
                SELECT period, plant_id, generator_id, net_summer_capacity_mw, net_winter_capacity_mw
                FROM eia_capacity
                WHERE period = ?
            """
            return pd.read_sql_query(query, conn, params=(period,))
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

    def upsert_nrc_data(self, df):
        """Upsert NRC reactor status data"""
        if df.empty:
            logger.info("No new NRC data to upsert")
            return 0

        conn = self._get_connection()
        try:
            records = [
                (row['report_date'].isoformat(), row['unit_name'], float(row['power_pct']))
                for _, row in df.iterrows()
            ]
            
            cursor = conn.cursor()
            cursor.executemany("""
                INSERT OR REPLACE INTO nrc_reactor_status
                (report_date, unit_name, power_pct)
                VALUES (?, ?, ?)
            """, records)
            
            conn.commit()
            rows_affected = cursor.rowcount
            logger.info(f"Upserted {rows_affected} NRC records into database")
            return rows_affected
        finally:
            conn.close()

    def upsert_eia_data(self, df):
        """Upsert EIA capacity data"""
        if df.empty:
            logger.info("No new EIA data to upsert")
            return 0

        conn = self._get_connection()
        try:
            records = [
                (
                    row['period'],
                    row['plant_id'],
                    row['generator_id'],
                    float(row['net_summer_capacity_mw']),
                    float(row['net_winter_capacity_mw'])
                )
                for _, row in df.iterrows()
            ]
            
            cursor = conn.cursor()
            cursor.executemany("""
                INSERT OR REPLACE INTO eia_capacity
                (period, plant_id, generator_id, net_summer_capacity_mw, net_winter_capacity_mw)
                VALUES (?, ?, ?, ?, ?)
            """, records)
            
            conn.commit()
            rows_affected = cursor.rowcount
            logger.info(f"Upserted {rows_affected} EIA records into database")
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

    def get_latest_nrc_data(self, units=None):
        """Get the latest NRC data for specified units"""
        conn = self._get_connection()
        try:
            query = """
                WITH latest_date AS (
                    SELECT MAX(report_date) as max_date 
                    FROM nrc_reactor_status
                )
                SELECT * FROM nrc_reactor_status 
                WHERE report_date = (SELECT max_date FROM latest_date)
            """
            
            if units:
                placeholders = ','.join('?' * len(units))
                query += f" AND unit_name IN ({placeholders})"
                df = pd.read_sql_query(query, conn, params=units)
            else:
                df = pd.read_sql_query(query, conn)
            
            df['report_date'] = pd.to_datetime(df['report_date'])
            return df
        finally:
            conn.close()

    def get_latest_eia_data(self, plant_ids=None):
        """Get the latest EIA capacity data for specified plants"""
        conn = self._get_connection()
        try:
            query = """
                WITH latest_period AS (
                    SELECT MAX(period) as max_period 
                    FROM eia_capacity
                )
                SELECT * FROM eia_capacity 
                WHERE period = (SELECT max_period FROM latest_period)
            """
            
            if plant_ids:
                placeholders = ','.join('?' * len(plant_ids))
                query += f" AND plant_id IN ({placeholders})"
                df = pd.read_sql_query(query, conn, params=plant_ids)
            else:
                df = pd.read_sql_query(query, conn)
            
            return df
        finally:
            conn.close()
