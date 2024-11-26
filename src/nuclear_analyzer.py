from datetime import datetime, timedelta
import pytz
import pandas as pd
import numpy as np
from src.utils.logger import setup_logger
from src.utils.config import load_config
from src.data_loader import NuclearDataManager

logger = setup_logger()

class NuclearAnalyzer:
    def __init__(self):
        self.logger = setup_logger()
        self.config = load_config()
        self.target_tz = pytz.timezone(self.config['data_settings']['timezones']['target'])
        self.nuclear_manager = NuclearDataManager()

    def check_nrc_data_age(self):
        """Check if NRC data is recent enough based on config settings"""
        if not self.config['posting']['processes']['nuclear'].get('require_recent_nrc_data', True):
            return True

        try:
            # Update nuclear data
            self.nuclear_manager.update_data()
            nrc_data = self.nuclear_manager.nrc_loader.get_latest_available_data()
            
            if nrc_data.empty:
                logger.warning("No NRC data available")
                return False
            
            # Get the latest data timestamp
            latest_nrc_time = nrc_data['report_date'].max()
            current_time = pd.Timestamp.now(tz=pytz.UTC)
            
            # Check if data is older than 24 hours
            age = current_time - latest_nrc_time
            if age > timedelta(hours=24):
                logger.warning(f"NRC data is too old ({age.total_seconds()/3600:.1f} hours)")
                return False
                
            logger.info(f"NRC data age check passed ({age.total_seconds()/3600:.1f} hours)")
            return True
            
        except Exception as e:
            logger.error(f"Error checking NRC data age: {str(e)}")
            return False

    def calculate_stats(self, load_df, hours=24):
        """Calculate nuclear generation statistics for the specified number of hours"""
        try:
            # Check NRC data age first
            if not self.check_nrc_data_age():
                raise ValueError("NRC data is not recent enough for analysis")

            # Get the latest UTC time and convert to target timezone for display
            now_utc = load_df['interval_start_utc'].max()
            now_local = now_utc.tz_convert(self.target_tz)
            period_start_local = now_local - timedelta(hours=hours)
            period_start_utc = period_start_local.tz_convert(pytz.UTC)
            
            # Get recent load data
            recent_load = load_df[load_df['interval_start_utc'] >= period_start_utc].copy()
            
            # Update nuclear data and get generation estimates
            self.nuclear_manager.update_data()
            nuclear_gen = self.nuclear_manager.estimate_generation()
            
            if nuclear_gen.empty:
                raise ValueError("No nuclear generation data available")
            
            # Calculate the most common time difference between load data points
            recent_load = recent_load.sort_values('interval_start_utc')
            time_diffs = recent_load['interval_start_utc'].diff().dropna()
            if time_diffs.empty:
                raise ValueError("Cannot determine time frequency from load data")
            
            # Get the most common time difference (mode)
            freq_td = time_diffs.mode().iloc[0]
            # Convert timedelta to minutes for creating date_range
            freq_minutes = int(freq_td.total_seconds() / 60)
            
            # Create time range with calculated frequency
            time_index = pd.date_range(
                start=period_start_utc,
                end=now_utc,
                freq=f'{freq_minutes}min'
            )
            
            # First ensure timestamps are UTC
            nuclear_gen['timestamp'] = pd.to_datetime(nuclear_gen['timestamp'])
            if nuclear_gen['timestamp'].dt.tz is None:
                nuclear_gen['timestamp'] = nuclear_gen['timestamp'].dt.tz_localize('UTC')
            elif str(nuclear_gen['timestamp'].dt.tz) != 'UTC':
                nuclear_gen['timestamp'] = nuclear_gen['timestamp'].dt.tz_convert('UTC')
            
            # Group by timestamp and sum estimated_mw
            nuclear_grouped = nuclear_gen.groupby('timestamp')['estimated_mw'].sum().reset_index()
            
            # Set timestamp as index for resampling
            nuclear_grouped.set_index('timestamp', inplace=True)
            
            # Reindex with forward fill to create continuous time series
            nuclear_df = nuclear_grouped.reindex(time_index, method='ffill').reset_index()
            nuclear_df.rename(columns={'index': 'timestamp'}, inplace=True)
            
            # Calculate total nuclear generation and load
            total_nuclear = nuclear_df['estimated_mw'].sum()
            total_load = recent_load['load.comed'].sum()
            
            if total_load == 0:
                raise ValueError("Load data is zero or missing")
            
            # Calculate percentage of load that could be supplied by nuclear
            nuclear_percentage = (total_nuclear / total_load) * 100
            
            # Merge the resampled nuclear data with load data
            merged = pd.merge(
                recent_load,
                nuclear_df,
                left_on='interval_start_utc',
                right_on='timestamp',
                how='inner'
            )
            
            if merged.empty:
                raise ValueError("No overlapping time periods between load and nuclear data")
            
            full_coverage_hours = (merged['estimated_mw'] >= merged['load.comed']).mean() * 100
            
            stats = {
                'nuclear_percentage': nuclear_percentage,
                'full_coverage_hours': full_coverage_hours,
                'start_time': period_start_local,
                'end_time': now_local,
                'total_nuclear': total_nuclear,
                'total_load': total_load,
                'nuclear_data': nuclear_df,  # Use resampled data instead of raw data
                'load_data': recent_load      # Include for visualization
            }
            
            logger.info(f"Calculated nuclear stats for period {period_start_local} to {now_local}")
            return stats
            
        except Exception as e:
            logger.error(f"Error calculating nuclear stats: {str(e)}")
            raise

    def format_stats_message(self, stats):
        """Format nuclear statistics into a message"""
        return (
            f"⚛️⚡️ Over the last 24 hours, enough nuclear energy was available to supply "
            f"{round(stats['nuclear_percentage'])}% of overall electricity demand in northern Illinois.\n\n"
            f"⏰ Nuclear plants could meet all electricity demand {round(stats['full_coverage_hours'])}% "
            f"of the last 24 hours."
        )
