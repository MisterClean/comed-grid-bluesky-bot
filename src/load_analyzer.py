from datetime import datetime, timedelta
import pytz
import pandas as pd
from src.utils.logger import setup_logger
from src.utils.config import load_config

logger = setup_logger()

class LoadAnalyzer:
    def __init__(self):
        self.logger = setup_logger()
        self.config = load_config()
        self.target_tz = pytz.timezone(self.config['data_settings']['timezones']['target'])

    def calculate_stats(self, df):
        """Calculate comprehensive load statistics for the last 24 hours"""
        try:
            # Get the latest UTC time and convert to target timezone for display
            now_utc = df['interval_start_utc'].max()
            now_local = now_utc.tz_convert(self.target_tz)
            period_start_local = now_local - timedelta(hours=24)
            period_start_utc = period_start_local.tz_convert(pytz.UTC)
            
            # Get recent data (last 24 hours)
            recent_data = df[df['interval_start_utc'] >= period_start_utc].copy()
            
            # Calculate current load
            current_load = recent_data['load.comed'].iloc[-1]
            
            # Calculate base load (10th percentile)
            base_load = recent_data['load.comed'].quantile(0.1)
            
            # Calculate load factor (average load / peak load)
            avg_load = recent_data['load.comed'].mean()
            peak_load = recent_data['load.comed'].max()
            load_factor = avg_load / peak_load
            
            # Calculate ramp rates (MW/hr)
            recent_data['ramp_rate'] = recent_data['load.comed'].diff() * 12  # Convert 5-min rate to hourly
            max_ramp = recent_data['ramp_rate'].max()
            max_ramp_idx = recent_data['ramp_rate'].idxmax()
            max_ramp_time = recent_data.loc[max_ramp_idx, 'interval_start_utc'].tz_convert(self.target_tz)
            
            # Calculate load volatility (standard deviation / mean)
            volatility = recent_data['load.comed'].std() / recent_data['load.comed'].mean()
            
            # Calculate load trend
            window_size = 12  # 1-hour window (assuming 5-minute intervals)
            rolling_avg = recent_data['load.comed'].rolling(window=window_size).mean()
            start_avg = rolling_avg.iloc[window_size:window_size*2].mean()
            end_avg = rolling_avg.iloc[-window_size:].mean()
            pct_change = ((end_avg - start_avg) / start_avg) * 100
            trend_direction = 'increasing' if pct_change > 1 else 'decreasing' if pct_change < -1 else 'stable'
            
            stats = {
                'current_load': current_load,
                'base_load': base_load,
                'load_factor': load_factor,
                'max_ramp': max_ramp,
                'max_ramp_time': max_ramp_time,
                'volatility': volatility,
                'trend': {
                    'direction': trend_direction,
                    'percentage': abs(pct_change)
                },
                'report_time': now_local
            }
            
            logger.info(f"Calculated load stats for period {period_start_local} to {now_local}")
            return stats
            
        except Exception as e:
            logger.error(f"Error calculating stats: {str(e)}")
            raise

    def format_stats_message(self, stats):
        """Format statistics into a message"""
        time_str = stats['report_time'].strftime('%-I:%M%p').lower()
        ramp_time = stats['max_ramp_time'].strftime('%-I:%M%p').lower()
        
        # Format trend with sign
        trend = stats['trend']
        trend_sign = '+' if trend['direction'] == 'increasing' else '-' if trend['direction'] == 'decreasing' else '±'
        
        return (
            f"⚡️ ComEd Grid Report\n"
            f"(Last 24H as of {time_str})\n\n"
            f"🔌 Current Load: {stats['current_load']:,.0f} MW\n\n"
            f"📊 System Dynamics:\n"
            f"Peak Ramp Rate: {stats['max_ramp']:,.0f} MW/hr ({ramp_time})\n"
            f"Load Volatility: {stats['volatility']:.1%}\n"
            f"Load is {trend['direction']} ({trend_sign}{trend['percentage']:.2f}%)\n\n"
            f"🏭 System Efficiency:\n"
            f"Load Factor: {stats['load_factor']:.0%}\n"
            f"Base Load: {stats['base_load']:,.0f} MW\n\n"
            f"Data From Grid Status"
        )
