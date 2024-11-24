import pandas as pd
import requests
from src.utils.logger import setup_logger
from src.utils.config import load_config
from src.utils.database import DatabaseManager
from src.interfaces import NuclearDataLoader, DataFetchError

logger = setup_logger()

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
