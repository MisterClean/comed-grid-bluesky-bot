from datetime import datetime, timedelta
import pytz
from src.utils.logger import setup_logger
from src.utils.config import load_config

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
