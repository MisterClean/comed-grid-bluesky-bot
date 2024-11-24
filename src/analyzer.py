from datetime import datetime, timedelta
import pytz
import pandas as pd
from src.utils.logger import setup_logger
from src.utils.config import load_config
from src.data_loader import NuclearDataManager

logger = setup_logger()

class LoadAnalyzer:
    def __init__(self):
        self.logger = setup_logger()
        self.config = load_config()
        self.target_tz = pytz.timezone(self.config['data_settings']['timezones']['target'])

    def calculate_stats(self, df, hours=4):
        """Calculate load statistics for the specified number of hours"""
        try:
            # Get the latest UTC time and convert to target timezone for display
            now_utc = df['interval_start_utc'].max()
            now_local = now_utc.tz_convert(self.target_tz)
            period_start_local = now_local - timedelta(hours=hours)
            period_start_utc = period_start_local.tz_convert(pytz.UTC)
            
            recent_data = df[df['interval_start_utc'] >= period_start_utc]
            
            stats = {
                'average': recent_data['load.comed'].mean(),
                'maximum': recent_data['load.comed'].max(),
                'minimum': recent_data['load.comed'].min(),
                'start_time': period_start_local,
                'end_time': now_local
            }
            
            logger.info(f"Calculated stats for period {period_start_local} to {now_local}")
            return stats
            
        except Exception as e:
            logger.error(f"Error calculating stats: {str(e)}")
            raise

    def format_stats_message(self, stats):
        """Format statistics into a message"""
        return (
            f"ComEd Load Report "
            f"({stats['start_time'].strftime('%I:%M %p')} - "
            f"{stats['end_time'].strftime('%I:%M %p')} CT)\n\n"
            f"Average Load: {stats['average']:,.0f} MW\n"
            f"Maximum Load: {stats['maximum']:,.0f} MW\n"
            f"Minimum Load: {stats['minimum']:,.0f} MW"
        )

class NuclearAnalyzer:
    def __init__(self):
        self.logger = setup_logger()
        self.config = load_config()
        self.target_tz = pytz.timezone(self.config['data_settings']['timezones']['target'])
        self.nuclear_manager = NuclearDataManager()

    def calculate_stats(self, load_df, hours=24):
        """Calculate nuclear generation statistics for the specified number of hours"""
        try:
            # Get the latest UTC time and convert to target timezone for display
            now_utc = load_df['interval_start_utc'].max()
            now_local = now_utc.tz_convert(self.target_tz)
            period_start_local = now_local - timedelta(hours=hours)
            period_start_utc = period_start_local.tz_convert(pytz.UTC)
            
            # Get recent load data
            recent_load = load_df[load_df['interval_start_utc'] >= period_start_utc]
            
            # Update nuclear data and get generation estimates
            self.nuclear_manager.update_data()
            nuclear_gen = self.nuclear_manager.estimate_generation()
            
            if nuclear_gen.empty:
                raise ValueError("No nuclear generation data available")
            
            # Calculate total nuclear generation and load
            total_nuclear = nuclear_gen['estimated_mw'].sum()
            total_load = recent_load['load.comed'].sum()
            
            if total_load == 0:
                raise ValueError("Load data is zero or missing")
            
            # Calculate percentage of load that could be supplied by nuclear
            nuclear_percentage = (total_nuclear / total_load) * 100
            
            # Calculate hours where nuclear could cover 100% of load
            nuclear_df = pd.DataFrame({
                'timestamp': nuclear_gen['timestamp'].unique(),
                'total_nuclear': nuclear_gen.groupby('timestamp')['estimated_mw'].sum()
            })
            
            merged = pd.merge(
                recent_load, 
                nuclear_df,
                left_on='interval_start_utc',
                right_on='timestamp',
                how='inner'
            )
            
            if merged.empty:
                raise ValueError("No overlapping time periods between load and nuclear data")
            
            full_coverage_hours = (merged['total_nuclear'] >= merged['load.comed']).mean() * 100
            
            stats = {
                'nuclear_percentage': nuclear_percentage,
                'full_coverage_hours': full_coverage_hours,
                'start_time': period_start_local,
                'end_time': now_local,
                'total_nuclear': total_nuclear,
                'total_load': total_load,
                'nuclear_data': nuclear_gen,  # Include for visualization
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
            f"{stats['nuclear_percentage']:.1f}% of electricity demand in northern Illinois.\n\n"
            f"⏰ There was enough nuclear energy to cover demand {stats['full_coverage_hours']:.1f}% "
            f"of the last 24 hours.\n\n"
            f"Load Data From Grid Status"
        )
