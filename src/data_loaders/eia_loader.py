import os
import pandas as pd
import requests
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
