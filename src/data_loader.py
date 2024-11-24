import os
import pytz
import pandas as pd
import requests
from datetime import datetime, timedelta
from gridstatusio import GridStatusClient
from src.utils.logger import setup_logger
from src.utils.config import load_config
from src.utils.database import DatabaseManager
from src.interfaces import DataLoader, NuclearDataLoader, DataFetchError

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

class NRCDataLoader(NuclearDataLoader):
    def __init__(self):
        self.config = load_config()['nuclear_data']['nrc']
        self.db = DatabaseManager()

    def get_reactor_status(self):
        """Fetch current reactor status from NRC"""
        try:
            response = requests.get(self.config['url'])
            response.raise_for_status()
            
            # Parse the pipe-delimited data
            lines = response.text.strip().split('\n')
            data = []
            # Skip the header row
            for line in lines[1:]:  # Start from index 1 to skip header
                if '|' in line:  # Skip malformed lines
                    date_str, unit, power = line.strip().split('|')
                    try:
                        data.append({
                            'report_date': pd.to_datetime(date_str.strip()).tz_localize('UTC'),
                            'unit_name': unit.strip(),
                            'power_pct': float(power.strip())
                        })
                    except ValueError as e:
                        logger.warning(f"Skipping malformed line: {line}. Error: {str(e)}")
                        continue
            
            df = pd.DataFrame(data)
            
            # Filter for configured plants only
            if self.config['plants']:
                df = df[df['unit_name'].isin(self.config['plants'])]
            
            if not df.empty:
                self.db.upsert_nrc_data(df)
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching NRC data: {str(e)}")
            raise DataFetchError(f"Failed to fetch NRC data: {str(e)}")

    def get_capacity_data(self):
        """This is handled by EIADataLoader"""
        return pd.DataFrame()

    def estimate_generation(self):
        """This is handled after combining NRC and EIA data"""
        return pd.DataFrame()

class EIADataLoader(NuclearDataLoader):
    def __init__(self):
        self.config = load_config()['nuclear_data']['eia']
        self.db = DatabaseManager()
        self.api_key = os.getenv('EIA_API_KEY')
        if not self.api_key:
            raise ValueError("EIA_API_KEY environment variable not set")

    def get_reactor_status(self):
        """This is handled by NRCDataLoader"""
        return pd.DataFrame()

    def get_capacity_data(self):
        """Fetch capacity data from EIA API"""
        try:
            # Construct API URL with parameters
            plant_ids = self.config['plant_ids']
            plant_ids_param = '&'.join([f'facets[plantid][]={pid}' for pid in plant_ids])
            
            url = (
                "https://api.eia.gov/v2/electricity/operating-generator-capacity/data/?"
                f"frequency=monthly&data[0]=net-summer-capacity-mw&data[1]=net-winter-capacity-mw&{plant_ids_param}"
                "&start=2023-01&end=2024-09&sort[0][column]=period&sort[0][direction]=desc"
                f"&api_key={self.api_key}"
            )
            
            response = requests.get(url)
            response.raise_for_status()
            
            data = response.json()
            if 'response' not in data or 'data' not in data['response']:
                raise DataFetchError("Invalid response format from EIA API")
            
            # Extract and rename columns to match our schema
            raw_df = pd.DataFrame(data['response']['data'])
            df = pd.DataFrame({
                'period': raw_df['period'],
                'plant_id': raw_df['plantid'].astype(str),  # Convert to string to match config
                'generator_id': raw_df['generatorid'],
                'net_summer_capacity_mw': raw_df['net-summer-capacity-mw'],
                'net_winter_capacity_mw': raw_df['net-winter-capacity-mw']
            })
            
            if not df.empty:
                self.db.upsert_eia_data(df)
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching EIA data: {str(e)}")
            raise DataFetchError(f"Failed to fetch EIA data: {str(e)}")

    def estimate_generation(self):
        """This is handled after combining NRC and EIA data"""
        return pd.DataFrame()

class NuclearDataManager:
    """Manages the combination of NRC and EIA data to estimate generation"""
    
    def __init__(self):
        self.config = load_config()['nuclear_data']
        self.nrc_loader = NRCDataLoader()
        self.eia_loader = EIADataLoader()
        self.db = DatabaseManager()

    def update_data(self):
        """Update both NRC and EIA data"""
        self.nrc_loader.get_reactor_status()
        self.eia_loader.get_capacity_data()

    def estimate_generation(self) -> pd.DataFrame:
        """Calculate estimated nuclear generation by combining NRC and EIA data"""
        try:
            # Get latest NRC status for configured plants
            nrc_df = self.db.get_latest_nrc_data(self.config['nrc']['plants'])
            
            # Get latest EIA capacity data
            eia_df = self.db.get_latest_eia_data(self.config['eia']['plant_ids'])
            
            if nrc_df.empty or eia_df.empty:
                raise DataFetchError("Missing required NRC or EIA data")
            
            # Map plant names and combine data
            results = []
            for plant, mapping in self.config['eia']['plant_mappings'].items():
                plant_id = mapping['eia_plant_id']
                nrc_names = mapping['nrc_names']
                
                # Get capacity for this plant
                plant_capacity = eia_df[eia_df['plant_id'] == plant_id]
                
                # Get NRC status for this plant's units
                plant_status = nrc_df[nrc_df['unit_name'].isin(nrc_names)]
                
                # Calculate generation for each unit
                for _, status in plant_status.iterrows():
                    unit_num = status['unit_name'].split()[-1]  # Extract unit number
                    capacity = plant_capacity[plant_capacity['generator_id'] == unit_num]
                    
                    if not capacity.empty:
                        # Use average of summer and winter capacity
                        avg_capacity = (
                            capacity['net_summer_capacity_mw'].iloc[0] +
                            capacity['net_winter_capacity_mw'].iloc[0]
                        ) / 2
                        
                        estimated_mw = avg_capacity * (status['power_pct'] / 100)
                        
                        results.append({
                            'timestamp': status['report_date'],
                            'unit': status['unit_name'],
                            'estimated_mw': estimated_mw
                        })
            
            return pd.DataFrame(results)
            
        except Exception as e:
            logger.error(f"Error estimating generation: {str(e)}")
            raise DataFetchError(f"Failed to estimate generation: {str(e)}")
