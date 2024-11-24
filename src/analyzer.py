from datetime import datetime, timedelta
from src.utils.logger import setup_logger

logger = setup_logger()

class LoadAnalyzer:
    def __init__(self):
        self.logger = setup_logger()

    def calculate_stats(self, df, hours=4):
        """Calculate load statistics for the specified number of hours"""
        try:
            now = df['interval_start_central'].max()
            period_start = now - timedelta(hours=hours)
            
            recent_data = df[df['interval_start_central'] >= period_start]
            
            stats = {
                'average': recent_data['load.comed'].mean(),
                'maximum': recent_data['load.comed'].max(),
                'minimum': recent_data['load.comed'].min(),
                'start_time': period_start,
                'end_time': now
            }
            
            logger.info(f"Calculated stats for period {period_start} to {now}")
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
