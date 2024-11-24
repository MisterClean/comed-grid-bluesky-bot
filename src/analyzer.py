from typing import Dict, Any, List, Tuple
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz

from src.interfaces import Analyzer, AnalysisError
from src.utils.logger import setup_logger
from src.utils.config import load_config

logger = setup_logger()

class LoadAnalyzer(Analyzer):
    """Analyzes grid load data to generate insights and statistics."""

    def __init__(self):
        """Initialize the LoadAnalyzer with configuration."""
        self.config = load_config()
        self.target_tz = pytz.timezone(self.config['data_settings']['timezones']['target'])

    def calculate_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate comprehensive statistics from the load data."""
        try:
            if df.empty:
                raise AnalysisError("No data available for analysis")

            # Convert timestamps to target timezone
            df = self._localize_timestamps(df)
            
            # Basic statistics
            current_load = self._get_current_load(df)
            peak_data = self._get_peak_load(df)
            min_data = self._get_min_load(df)
            avg_load = self._calculate_average_load(df)
            
            # Advanced power system metrics
            load_factor = self._calculate_load_factor(df)
            ramp_stats = self._calculate_ramp_statistics(df)
            volatility_index = self._calculate_volatility_index(df)
            base_load = self._estimate_base_load(df)
            
            # Trend analysis
            trend_data = self._analyze_trend(df)
            
            # Daily pattern analysis
            daily_pattern = self._analyze_daily_pattern(df)
            
            # Generate period summary with advanced metrics
            period_summary = self._generate_period_summary(
                df, current_load, peak_data, min_data, avg_load, 
                load_factor, ramp_stats, volatility_index, base_load,
                trend_data
            )

            return {
                'current_load': round(current_load, 2),
                'peak_load': round(peak_data['load'], 2),
                'peak_time': peak_data['time'],
                'min_load': round(min_data['load'], 2),
                'min_time': min_data['time'],
                'avg_load': round(avg_load, 2),
                'load_factor': round(load_factor, 3),
                'ramp_stats': ramp_stats,
                'volatility_index': round(volatility_index, 3),
                'base_load': round(base_load, 2),
                'load_trend': trend_data,
                'daily_pattern': daily_pattern,
                'period_summary': period_summary
            }

        except Exception as e:
            error_msg = f"Error calculating statistics: {str(e)}"
            logger.error(error_msg)
            raise AnalysisError(error_msg)

    def _calculate_load_factor(self, df: pd.DataFrame) -> float:
        """Calculate load factor (average load / peak load)."""
        avg_load = df['load.comed'].mean()
        peak_load = df['load.comed'].max()
        return avg_load / peak_load

    def _calculate_ramp_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate ramping statistics."""
        df['ramp_rate'] = df['load.comed'].diff()
        max_up_idx = df['ramp_rate'].idxmax()
        max_down_idx = df['ramp_rate'].idxmin()
        
        max_up_ramp = df.loc[max_up_idx, 'ramp_rate']
        max_down_ramp = df.loc[max_down_idx, 'ramp_rate']
        
        # Determine which direction had the larger magnitude
        if abs(max_up_ramp) > abs(max_down_ramp):
            max_ramp = max_up_ramp
            max_ramp_time = df.loc[max_up_idx, 'display_time']
        else:
            max_ramp = max_down_ramp
            max_ramp_time = df.loc[max_down_idx, 'display_time']
        
        return {
            'max_ramp': max_ramp,  # Now keeping the sign
            'max_ramp_time': max_ramp_time,
            'max_up_ramp': max_up_ramp,
            'max_down_ramp': max_down_ramp,
            'avg_ramp': df['ramp_rate'].abs().mean()
        }

    def _calculate_volatility_index(self, df: pd.DataFrame) -> float:
        """Calculate load volatility index."""
        return df['load.comed'].std() / df['load.comed'].mean()

    def _estimate_base_load(self, df: pd.DataFrame) -> float:
        """Estimate base load using statistical methods."""
        return df['load.comed'].quantile(0.1)

    def _generate_period_summary(
        self,
        df: pd.DataFrame,
        current_load: float,
        peak_data: Dict[str, Any],
        min_data: Dict[str, Any],
        avg_load: float,
        load_factor: float,
        ramp_stats: Dict[str, float],
        volatility_index: float,
        base_load: float,
        trend_data: Dict[str, Any]
    ) -> str:
        """Generate a human-readable summary of the period."""
        # Get the last timestamp from the data
        last_time = df['display_time'].iloc[-1]
        time_str = last_time.strftime('%-I:%M%p').lower()
        
        # Format the peak ramp time
        ramp_time = ramp_stats['max_ramp_time'].strftime('%-I:%M%p').lower()
        
        # Calculate hourly ramp rate (keeping the sign)
        max_ramp_hr = ramp_stats['max_ramp'] * 12  # Convert 5-min rate to hourly rate
        
        # Format trend percentage with sign
        trend_pct = trend_data['percentage']
        trend_sign = '+' if trend_data['direction'] == 'increasing' else '-' if trend_data['direction'] == 'decreasing' else '±'
        
        # Add sign to ramp rate display (it's already in the number, just format with sign)
        ramp_sign = '+' if max_ramp_hr > 0 else ''  # No need to add - sign as it's in the number
        
        return (
            f"⚡️ ComEd Grid Report\n"
            f"(Last 24H as of {time_str})\n\n"
            f"🔌 Current Load: {self._format_load(current_load)} MW\n\n"
            f"📊 System Dynamics:\n"
            f"Peak Ramp Rate: {ramp_sign}{max_ramp_hr:,.0f} MW/hr ({ramp_time})\n"
            f"Load Volatility: {volatility_index:.1%}\n"
            f"Load is {trend_data['direction']} ({trend_sign}{trend_pct:.2f}%)\n\n"
            f"🏭 System Efficiency:\n"
            f"Load Factor: {load_factor:.0%}\n"
            f"Base Load: {self._format_load(base_load)} MW\n\n"
            f"Data From Grid Status"
        )

    def _localize_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert UTC timestamps to target timezone."""
        df = df.copy()
        for col in ['interval_start_utc', 'interval_end_utc']:
            df[col] = df[col].dt.tz_convert(self.target_tz)
        # Add display_time column for consistent timezone handling
        df['display_time'] = df['interval_start_utc']
        return df

    def _get_current_load(self, df: pd.DataFrame) -> float:
        """Get the most recent load value."""
        return df['load.comed'].iloc[-1]

    def _get_peak_load(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get peak load and its timestamp."""
        peak_idx = df['load.comed'].idxmax()
        return {
            'load': df.loc[peak_idx, 'load.comed'],
            'time': df.loc[peak_idx, 'interval_start_utc']
        }

    def _get_min_load(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get minimum load and its timestamp."""
        min_idx = df['load.comed'].idxmin()
        return {
            'load': df.loc[min_idx, 'load.comed'],
            'time': df.loc[min_idx, 'interval_start_utc']
        }

    def _calculate_average_load(self, df: pd.DataFrame) -> float:
        """Calculate average load."""
        return df['load.comed'].mean()

    def _analyze_trend(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze load trend over the period."""
        window_size = 12  # 1-hour window (assuming 5-minute intervals)
        rolling_avg = df['load.comed'].rolling(window=window_size).mean()
        
        start_avg = rolling_avg.iloc[window_size:window_size*2].mean()
        end_avg = rolling_avg.iloc[-window_size:].mean()
        
        pct_change = ((end_avg - start_avg) / start_avg) * 100
        direction = 'increasing' if pct_change > 1 else 'decreasing' if pct_change < -1 else 'stable'
        
        return {
            'direction': direction,
            'percentage': abs(pct_change)
        }

    def _analyze_daily_pattern(self, df: pd.DataFrame) -> Dict[int, float]:
        """Analyze average load by hour of day."""
        df['hour'] = df['interval_start_utc'].dt.hour
        hourly_avg = df.groupby('hour')['load.comed'].mean()
        return {hour: round(avg, 2) for hour, avg in hourly_avg.items()}

    def _format_load(self, load: float) -> str:
        """Format load value with thousands separator."""
        return f"{load:,.0f}"
