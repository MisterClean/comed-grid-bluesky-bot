import os
import pandas as pd
import requests
from datetime import datetime, timedelta
from src.utils.logger import setup_logger
from src.utils.config import load_config
from src.utils.database import DatabaseManager
from src.interfaces import NuclearDataLoader, DataFetchError
from src.data_loaders.nrc_loader import NRCDataLoader

logger = setup_logger()

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

    def get_latest_available_data(self):
        """This is handled by NRCDataLoader"""
        return pd.DataFrame()

    def get_capacity_data(self):
        """Fetch capacity data from EIA API"""
        try:
            # Calculate date range for last 3 months to ensure we get the most recent data
            end_date = datetime.now()
            start_date = end_date - timedelta(days=90)
            
            # Format dates for API
            start_str = start_date.strftime('%Y-%m')
            end_str = end_date.strftime('%Y-%m')
            
            # Construct API URL with parameters
            plant_ids = self.config['plant_ids']
            plant_ids_param = '&'.join([f'facets[plantid][]={str(pid)}' for pid in plant_ids])
            
            url = (
                "https://api.eia.gov/v2/electricity/operating-generator-capacity/data/?"
                f"frequency=monthly&data[0]=net-summer-capacity-mw&data[1]=net-winter-capacity-mw&{plant_ids_param}"
                f"&start={start_str}&end={end_str}&sort[0][column]=period&sort[0][direction]=desc"
                f"&api_key={self.api_key}"
            )
            
            response = requests.get(url)
            response.raise_for_status()
            
            data = response.json()
            if 'response' not in data or 'data' not in data['response']:
                raise DataFetchError("Invalid response format from EIA API")
            
            # Extract and rename columns to match our schema
            raw_df = pd.DataFrame(data['response']['data'])
            
            # If no data returned, raise error
            if raw_df.empty:
                raise DataFetchError("No capacity data returned from EIA API")
            
            df = pd.DataFrame({
                'period': pd.to_datetime(raw_df['period']).dt.strftime('%Y-%m'),  # Format as YYYY-MM string
                'plant_id': raw_df['plantid'].astype(str),  # Convert to string
                'generator_id': raw_df['generatorid'].astype(str),  # Convert to string
                'net_summer_capacity_mw': pd.to_numeric(raw_df['net-summer-capacity-mw'], errors='coerce'),
                'net_winter_capacity_mw': pd.to_numeric(raw_df['net-winter-capacity-mw'], errors='coerce')
            })
            
            # Drop any rows with NaN values
            df = df.dropna()
            
            # Keep only the most recent month's data for each plant/generator
            df = df.sort_values('period', ascending=False)
            df = df.drop_duplicates(subset=['plant_id', 'generator_id'], keep='first')
            
            if not df.empty:
                latest_period = df['period'].max()
                # Get existing data for the same period
                existing_data = self.db.get_eia_data_for_period(latest_period)
                
                if existing_data.empty:
                    # No existing data for this period, safe to upsert
                    self.db.upsert_eia_data(df)
                    logger.info(f"Stored new EIA data for period {latest_period}")
                else:
                    # Compare with existing data
                    df_compare = df.merge(
                        existing_data,
                        on=['period', 'plant_id', 'generator_id'],
                        how='left',
                        suffixes=('_new', '_existing')
                    )
                    
                    # Check if any values are different (using small threshold for float comparison)
                    changes = df_compare[
                        (abs(df_compare['net_summer_capacity_mw_new'] - df_compare['net_summer_capacity_mw_existing']) > 0.01) |
                        (abs(df_compare['net_winter_capacity_mw_new'] - df_compare['net_winter_capacity_mw_existing']) > 0.01)
                    ]
                    
                    if not changes.empty:
                        self.db.upsert_eia_data(df)
                        logger.info(f"Updated EIA data with {len(changes)} changes for period {latest_period}")
                    else:
                        logger.info(f"No changes in EIA data for period {latest_period}, skipping upsert")
            
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
        # Use get_latest_available_data instead of get_reactor_status
        self.nrc_loader.get_latest_available_data()
        self.eia_loader.get_capacity_data()

    def get_seasonal_capacity(self, month: int, summer_capacity: float, winter_capacity: float) -> float:
        """
        Determine the appropriate capacity based on the month.
        
        Args:
            month: Month as integer (1-12)
            summer_capacity: Summer capacity in MW
            winter_capacity: Winter capacity in MW
            
        Returns:
            float: Appropriate capacity for the given month
        """
        # Summer months (June-September)
        if month in [6, 7, 8, 9]:
            return summer_capacity
        # Winter months (December-March)
        elif month in [12, 1, 2, 3]:
            return winter_capacity
        # Shoulder months (April-May, October-November)
        else:
            return (summer_capacity + winter_capacity) / 2

    def estimate_generation(self) -> pd.DataFrame:
        """Calculate estimated nuclear generation by combining NRC and EIA data"""
        try:
            # Get latest NRC status for configured plants
            # Use get_latest_available_data to ensure we have the most recent data
            nrc_df = self.nrc_loader.get_latest_available_data()
            
            # Get latest EIA capacity data
            eia_df = self.db.get_latest_eia_data([str(pid) for pid in self.config['eia']['plant_ids']])
            
            if nrc_df.empty or eia_df.empty:
                raise DataFetchError("Missing required NRC or EIA data")
            
            # Log the timestamp of the NRC data being used
            logger.info(f"Using NRC data from: {nrc_df['report_date'].max()}")
            
            # Map plant names and combine data
            results = []
            for plant, mapping in self.config['eia']['plant_mappings'].items():
                plant_id = str(mapping['eia_plant_id'])  # Convert to string to match database
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
                        # Get the month from the NRC report date
                        report_month = status['report_date'].month
                        
                        # Get appropriate capacity based on season
                        seasonal_capacity = self.get_seasonal_capacity(
                            report_month,
                            float(capacity['net_summer_capacity_mw'].iloc[0]),
                            float(capacity['net_winter_capacity_mw'].iloc[0])
                        )
                        
                        # Calculate estimated generation
                        estimated_mw = seasonal_capacity * (float(status['power_pct']) / 100)
                        
                        results.append({
                            'timestamp': status['report_date'],
                            'unit': status['unit_name'],
                            'estimated_mw': estimated_mw,
                            'capacity_used': seasonal_capacity  # Added for debugging/verification
                        })
            
            return pd.DataFrame(results)
            
        except Exception as e:
            logger.error(f"Error estimating generation: {str(e)}")
            raise DataFetchError(f"Failed to estimate generation: {str(e)}")
