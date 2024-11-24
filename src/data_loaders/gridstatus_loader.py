import os
import pytz
import pandas as pd
from datetime import datetime, timedelta
from gridstatusio import GridStatusClient
from src.utils.logger import setup_logger
from src.utils.config import load_config
from src.utils.database import DatabaseManager

logger = setup_logger()

class GridDataLoader:
    def __init__(self):
        self.config = load_config()['data_settings']
        self.client = self._initialize_client()
        self.db = DatabaseManager()

    def _initialize_client(self):
        """Initialize GridStatus client"""
        api_key = os.getenv('GRIDSTATUS_API_KEY')
        if not api_key:
            raise ValueError("GRIDSTATUS_API_KEY environment variable not set")
        return GridStatusClient(api_key=api_key)

    def get_load_data(self):
        """Get ComEd load data based on config settings"""
        end_time = datetime.now(pytz.UTC)
        
        # Check the latest data in our database
        latest_timestamp = self.db.get_latest_timestamp()
        
        if latest_timestamp:
            # Database exists, fetch only new data
            latest_dt = pd.to_datetime(latest_timestamp)
            if latest_dt.tz is None:
                latest_dt = latest_dt.tz_localize('UTC')
            elif str(latest_dt.tz) != 'UTC':
                latest_dt = latest_dt.tz_convert('UTC')
            
            start_time = latest_dt + timedelta(minutes=1)
            logger.info(f"Found existing data, fetching from {start_time} onwards")
        else:
            # No data in database, do initial historical load
            start_time = end_time - timedelta(days=self.config['initial_days_back'])
            logger.info(f"No existing data found. Performing initial load from {start_time} to {end_time}")
        
        try:
            if start_time >= end_time:
                logger.info("Database is up to date, no new data to fetch")
                return self.db.get_data_since(
                    (end_time - timedelta(days=self.config['days_back'])).isoformat()
                )
            
            # For initial load, fetch in chunks
            if not latest_timestamp:
                logger.info("Performing chunked historical data fetch...")
                chunk_size = timedelta(days=5)
                current_start = start_time
                all_data = []

                while current_start < end_time:
                    chunk_end = min(current_start + chunk_size, end_time)
                    logger.info(f"Fetching chunk from {current_start} to {chunk_end}")
                    
                    chunk_df = self.client.get_dataset(
                        dataset=self.config['dataset'],
                        start=current_start.isoformat(),
                        end=chunk_end.isoformat(),
                        columns=self.config['columns'],
                        limit=self.config['limit']
                    )
                    
                    if not chunk_df.empty:
                        chunk_df = self._process_dataframe(chunk_df)
                        if not chunk_df.empty:
                            all_data.append(chunk_df)
                            self.db.upsert_data(chunk_df)
                    
                    current_start = chunk_end
                
                if all_data:
                    df = pd.concat(all_data, ignore_index=True)
                else:
                    df = pd.DataFrame()
            else:
                # Regular incremental fetch
                df = self.client.get_dataset(
                    dataset=self.config['dataset'],
                    start=start_time.isoformat(),
                    end=end_time.isoformat(),
                    columns=self.config['columns'],
                    limit=self.config['limit']
                )
                
                if not df.empty:
                    df = self._process_dataframe(df)
                    if not df.empty:
                        self.db.upsert_data(df)
            
            # Get data from database for analysis
            df = self.db.get_data_since(
                (end_time - timedelta(days=self.config['days_back'])).isoformat()
            )
            
            if df.empty:
                raise ValueError("No load data available for analysis")
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching data: {str(e)}")
            raise

    def _process_dataframe(self, df):
        """Process the raw dataframe"""
        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame(df)
        
        # Handle missing or invalid values
        df = df.dropna(subset=['interval_start_utc', 'interval_end_utc', 'load.comed'])
        df = df[df['load.comed'] >= 0]  # Filter out negative load values
        
        if df.empty:
            logger.warning("No valid data after filtering NaN values")
            return df
        
        # Ensure proper timezone handling
        for col in ['interval_start_utc', 'interval_end_utc']:
            if df[col].dt.tz is None:
                df[col] = pd.to_datetime(df[col]).dt.tz_localize('UTC')
            elif str(df[col].dt.tz) != 'UTC':
                df[col] = pd.to_datetime(df[col]).dt.tz_convert('UTC')
        
        return df
