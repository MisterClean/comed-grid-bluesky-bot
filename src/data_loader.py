import os
import pytz
import pandas as pd
from datetime import datetime, timedelta
from gridstatusio import GridStatusClient
from src.utils.logger import setup_logger
from src.utils.config import load_config

logger = setup_logger()

class GridDataLoader:
    def __init__(self):
        self.config = load_config()['data_settings']
        self.client = self._initialize_client()
        self.source_tz = pytz.timezone(self.config['timezones']['source'])
        self.target_tz = pytz.timezone(self.config['timezones']['target'])

    def _initialize_client(self):
        api_key = os.getenv('GRIDSTATUS_API_KEY')
        if not api_key:
            raise ValueError("GRIDSTATUS_API_KEY environment variable not set")
        return GridStatusClient(api_key=api_key)

    def get_load_data(self):
        """Get ComEd load data based on config settings"""
        end_time = datetime.now(self.source_tz)
        start_time = end_time - timedelta(days=self.config['days_back'])
        
        logger.info(f"Fetching load data from {start_time} to {end_time}")
        
        try:
            df = self.client.get_dataset(
                dataset=self.config['dataset'],
                start=start_time.isoformat(),
                end=end_time.isoformat(),
                columns=self.config['columns'],
                limit=self.config['limit']
            )
            
            df = self._process_dataframe(df)
            logger.info(f"Successfully fetched {len(df)} records")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching data: {str(e)}")
            raise

    def _process_dataframe(self, df):
        """Process the raw dataframe"""
        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame(df)
        
        # Convert UTC timestamps to target timezone
        source_cols = ['interval_start_utc', 'interval_end_utc']
        target_cols = ['interval_start_central', 'interval_end_central']
        
        for source_col, target_col in zip(source_cols, target_cols):
            df[target_col] = pd.to_datetime(df[source_col]).dt.tz_convert(self.target_tz)
        
        return df
