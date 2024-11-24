from typing import Optional, List, Dict, Any
import os
import pytz
import pandas as pd
from datetime import datetime, timedelta
from gridstatusio import GridStatusClient

from src.interfaces import DataLoader, DataFetchError
from src.utils.logger import setup_logger
from src.utils.config import load_config
from src.utils.database import DatabaseManager

logger = setup_logger()

class GridDataLoader(DataLoader):
    """Handles fetching and processing of grid load data from the GridStatus API."""

    def __init__(self) -> None:
        """Initialize the GridDataLoader with configuration and dependencies."""
        self.config: Dict[str, Any] = load_config()['data_settings']
        self.client: GridStatusClient = self._initialize_client()
        self.db: DatabaseManager = DatabaseManager()

    def _initialize_client(self) -> GridStatusClient:
        """Initialize the GridStatus API client.
        
        Returns:
            GridStatusClient: Initialized API client
            
        Raises:
            DataFetchError: If API key is not configured
        """
        api_key = os.getenv('GRIDSTATUS_API_KEY')
        if not api_key:
            raise DataFetchError("GRIDSTATUS_API_KEY environment variable not set")
        return GridStatusClient(api_key=api_key)

    def get_load_data(self) -> pd.DataFrame:
        """Get ComEd load data based on config settings.
        
        Returns:
            pd.DataFrame: DataFrame containing load data with columns:
                - interval_start_utc: UTC timestamp of interval start
                - interval_end_utc: UTC timestamp of interval end
                - load.comed: Load value in MW
                
        Raises:
            DataFetchError: If there is an error fetching or processing the data
        """
        end_time = datetime.now(pytz.UTC)
        
        try:
            # Check the latest data in our database
            latest_timestamp = self.db.get_latest_timestamp()
            
            if latest_timestamp:
                # Database exists, fetch only new data
                latest_dt = self._ensure_utc_timestamp(pd.to_datetime(latest_timestamp))
                start_time = latest_dt + timedelta(minutes=1)
                logger.info(f"Found existing data, fetching from {start_time} onwards")
            else:
                # No data in database, do initial historical load
                start_time = end_time - timedelta(days=self.config['initial_days_back'])
                logger.info(f"No existing data found. Performing initial load from {start_time} to {end_time}")
            
            if start_time >= end_time:
                logger.info("Database is up to date, no new data to fetch")
                return self._get_recent_data(end_time)
            
            # For initial load, fetch data in chunks
            if not latest_timestamp:
                df = self._fetch_historical_data(start_time, end_time)
            else:
                # Regular incremental fetch
                df = self._fetch_incremental_data(start_time, end_time)
            
            return self._get_recent_data(end_time)
            
        except Exception as e:
            error_msg = f"Error fetching data: {str(e)}"
            logger.error(error_msg)
            raise DataFetchError(error_msg) from e

    def _ensure_utc_timestamp(self, timestamp: pd.Timestamp) -> pd.Timestamp:
        """Ensure a timestamp is in UTC timezone.
        
        Args:
            timestamp: Input timestamp
            
        Returns:
            pd.Timestamp: UTC timezone-aware timestamp
        """
        if timestamp.tz is None:
            return timestamp.tz_localize('UTC')
        return timestamp.tz_convert('UTC')

    def _fetch_historical_data(self, start_time: datetime, end_time: datetime) -> pd.DataFrame:
        """Fetch historical data in chunks to avoid API limits.
        
        Args:
            start_time: Start time for data fetch
            end_time: End time for data fetch
            
        Returns:
            pd.DataFrame: Combined historical data
        """
        logger.info("Performing chunked historical data fetch...")
        chunk_size = timedelta(days=5)
        current_start = start_time
        all_data: List[pd.DataFrame] = []

        while current_start < end_time:
            chunk_end = min(current_start + chunk_size, end_time)
            logger.info(f"Fetching chunk from {current_start} to {chunk_end}")
            
            chunk_df = self._fetch_data_chunk(current_start, chunk_end)
            if not chunk_df.empty:
                self.db.upsert_data(chunk_df)
                all_data.append(chunk_df)
            
            current_start = chunk_end
        
        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

    def _fetch_incremental_data(self, start_time: datetime, end_time: datetime) -> pd.DataFrame:
        """Fetch incremental data update.
        
        Args:
            start_time: Start time for data fetch
            end_time: End time for data fetch
            
        Returns:
            pd.DataFrame: New data since last update
        """
        df = self._fetch_data_chunk(start_time, end_time)
        if not df.empty:
            self.db.upsert_data(df)
        return df

    def _fetch_data_chunk(self, start_time: datetime, end_time: datetime) -> pd.DataFrame:
        """Fetch a single chunk of data from the API.
        
        Args:
            start_time: Start time for chunk
            end_time: End time for chunk
            
        Returns:
            pd.DataFrame: Data for the specified time period
        """
        df = self.client.get_dataset(
            dataset=self.config['dataset'],
            start=start_time.isoformat(),
            end=end_time.isoformat(),
            columns=self.config['columns'],
            limit=self.config['limit']
        )
        
        return self._process_dataframe(df)

    def _process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process the raw dataframe.
        
        Args:
            df: Raw DataFrame from API
            
        Returns:
            pd.DataFrame: Processed DataFrame with consistent formatting
        """
        # Convert to DataFrame if needed and create an explicit copy
        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame(df)
        df = df.copy()
        
        # Drop any rows with NaN values
        df = df.dropna(subset=['interval_start_utc', 'interval_end_utc', 'load.comed'])
        
        if df.empty:
            logger.warning("No valid data after filtering NaN values")
            return df
        
        # Convert timestamps using loc accessor
        for col in ['interval_start_utc', 'interval_end_utc']:
            df.loc[:, col] = df[col].apply(lambda x: self._ensure_utc_timestamp(pd.to_datetime(x)))
        
        return df

    def _get_recent_data(self, end_time: datetime) -> pd.DataFrame:
        """Get recent data from the database.
        
        Args:
            end_time: Current time
            
        Returns:
            pd.DataFrame: Recent data based on config settings
        """
        return self.db.get_data_since(
            (end_time - timedelta(days=self.config['days_back'])).isoformat()
        )
