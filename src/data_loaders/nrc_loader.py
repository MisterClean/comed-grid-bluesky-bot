import pandas as pd
import requests
from datetime import datetime, timedelta
import pytz
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
                        # Convert the misleading midnight timestamp to actual ~9am Eastern time
                        # First parse as naive datetime
                        parsed_date = pd.to_datetime(date_str.strip())
                        
                        # Add 9 hours to reflect actual data collection time
                        actual_time = parsed_date + timedelta(hours=9)
                        
                        # Localize to Eastern time
                        eastern = pytz.timezone('America/New_York')
                        localized_time = eastern.localize(actual_time)
                        
                        # Convert to UTC for storage
                        utc_time = localized_time.astimezone(pytz.UTC)
                        
                        data.append({
                            'report_date': utc_time,
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
                # Get the latest data from the database for comparison
                latest_date = df['report_date'].max()
                existing_data = self.db.get_nrc_data_for_date(latest_date)
                
                if existing_data.empty:
                    # No existing data for this date, safe to upsert
                    self.db.upsert_nrc_data(df)
                    logger.info(f"Stored new NRC data with timestamp {latest_date}")
                else:
                    # Compare with existing data
                    df_compare = df.merge(
                        existing_data,
                        on=['report_date', 'unit_name'],
                        how='left',
                        suffixes=('_new', '_existing')
                    )
                    
                    # Check if any values are different
                    changes = df_compare[
                        abs(df_compare['power_pct_new'] - df_compare['power_pct_existing']) > 0.01
                    ]
                    
                    if not changes.empty:
                        self.db.upsert_nrc_data(df)
                        logger.info(f"Updated NRC data with {len(changes)} changes for timestamp {latest_date}")
                    else:
                        logger.info(f"No changes in NRC data for timestamp {latest_date}, skipping upsert")
            else:
                logger.warning("No valid NRC data found to store")
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching NRC data: {str(e)}")
            raise DataFetchError(f"Failed to fetch NRC data: {str(e)}")

    def get_latest_available_data(self):
        """Get the most recent NRC data available, even if not from today"""
        try:
            # First try to get fresh data
            current_data = self.get_reactor_status()
            
            if not current_data.empty:
                return current_data
            
            # If no fresh data, get the most recent data from the database
            logger.info("No fresh NRC data available, retrieving most recent stored data")
            return self.db.get_latest_nrc_data(self.config['plants'])
            
        except Exception as e:
            logger.error(f"Error getting latest available NRC data: {str(e)}")
            raise DataFetchError(f"Failed to get latest available NRC data: {str(e)}")

    def get_capacity_data(self):
        """This is handled by EIADataLoader"""
        return pd.DataFrame()

    def estimate_generation(self):
        """This is handled after combining NRC and EIA data"""
        return pd.DataFrame()
