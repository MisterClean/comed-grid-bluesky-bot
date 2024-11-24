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
        """Calculate comprehensive statistics from the load data.
        
        Args:
            df: DataFrame containing the load data with columns:
                - interval_start_utc
                - interval_end_utc
                - load.comed
                
        Returns:
            Dict containing calculated statistics:
                - current_load: Current load in MW
                - peak_load: Peak load in MW
                - min_load: Minimum load in MW
                - avg_load: Average load in MW
                - peak_time: Time of peak load
                - min_time: Time of minimum load
                - load_trend: Trend direction and percentage
                - daily_pattern: Dict of hourly averages
                - period_summary: Text summary of the period
                
        Raises:
            AnalysisError: If there is an error calculating statistics
        """
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
            
            # Trend analysis
            trend_data = self._analyze_trend(df)
            
            # Daily pattern analysis
            daily_pattern = self._analyze_daily_pattern(df)
            
            # Generate period summary
            period_summary = self._generate_period_summary(
                df, current_load, peak_data, min_data, avg_load, trend_data
            )

            return {
                'current_load': round(current_load, 2),
                'peak_load': round(peak_data['load'], 2),
                'peak_time': peak_data['time'],
                'min_load': round(min_data['load'], 2),
                'min_time': min_data['time'],
                'avg_load': round(avg_load, 2),
                'load_trend': trend_data,
                'daily_pattern': daily_pattern,
                'period_summary': period_summary
            }

        except Exception as e:
            error_msg = f"Error calculating statistics: {str(e)}"
            logger.error(error_msg)
            raise AnalysisError(error_msg)

    def _localize_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert UTC timestamps to target timezone.
        
        Args:
            df: DataFrame with UTC timestamps
            
        Returns:
            DataFrame with localized timestamps
        """
        df = df.copy()
        for col in ['interval_start_utc', 'interval_end_utc']:
            df[col] = df[col].dt.tz_convert(self.target_tz)
        return df

    def _get_current_load(self, df: pd.DataFrame) -> float:
        """Get the most recent load value.
        
        Args:
            df: DataFrame with load data
            
        Returns:
            float: Current load in MW
        """
        return df['load.comed'].iloc[-1]

    def _get_peak_load(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get peak load and its timestamp.
        
        Args:
            df: DataFrame with load data
            
        Returns:
            Dict containing peak load value and time
        """
        peak_idx = df['load.comed'].idxmax()
        return {
            'load': df.loc[peak_idx, 'load.comed'],
            'time': df.loc[peak_idx, 'interval_start_utc']
        }

    def _get_min_load(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get minimum load and its timestamp.
        
        Args:
            df: DataFrame with load data
            
        Returns:
            Dict containing minimum load value and time
        """
        min_idx = df['load.comed'].idxmin()
        return {
            'load': df.loc[min_idx, 'load.comed'],
            'time': df.loc[min_idx, 'interval_start_utc']
        }

    def _calculate_average_load(self, df: pd.DataFrame) -> float:
        """Calculate average load.
        
        Args:
            df: DataFrame with load data
            
        Returns:
            float: Average load in MW
        """
        return df['load.comed'].mean()

    def _analyze_trend(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze load trend over the period.
        
        Args:
            df: DataFrame with load data
            
        Returns:
            Dict containing trend direction and percentage change
        """
        # Calculate rolling average to smooth out noise
        window_size = 12  # 1-hour window (assuming 5-minute intervals)
        rolling_avg = df['load.comed'].rolling(window=window_size).mean()
        
        # Compare beginning and end of period
        start_avg = rolling_avg.iloc[window_size:window_size*2].mean()
        end_avg = rolling_avg.iloc[-window_size:].mean()
        
        pct_change = ((end_avg - start_avg) / start_avg) * 100
        direction = 'increasing' if pct_change > 1 else 'decreasing' if pct_change < -1 else 'stable'
        
        return {
            'direction': direction,
            'percentage': round(abs(pct_change), 2)
        }

    def _analyze_daily_pattern(self, df: pd.DataFrame) -> Dict[int, float]:
        """Analyze average load by hour of day.
        
        Args:
            df: DataFrame with load data
            
        Returns:
            Dict mapping hour to average load
        """
        df['hour'] = df['interval_start_utc'].dt.hour
        hourly_avg = df.groupby('hour')['load.comed'].mean()
        return {hour: round(avg, 2) for hour, avg in hourly_avg.items()}

    def _generate_period_summary(
        self,
        df: pd.DataFrame,
        current_load: float,
        peak_data: Dict[str, Any],
        min_data: Dict[str, Any],
        avg_load: float,
        trend_data: Dict[str, Any]
    ) -> str:
        """Generate a human-readable summary of the period.
        
        Args:
            df: DataFrame with load data
            current_load: Current load value
            peak_data: Peak load information
            min_data: Minimum load information
            avg_load: Average load value
            trend_data: Trend analysis results
            
        Returns:
            str: Formatted summary text
        """
        period_start = df['interval_start_utc'].iloc[0].strftime('%I:%M %p')
        period_end = df['interval_start_utc'].iloc[-1].strftime('%I:%M %p')
        
        return (
            f"ComEd Load Report ({period_start} - {period_end} {self.target_tz.zone})\n\n"
            f"Current Load: {self._format_load(current_load)} MW\n"
            f"Peak Load: {self._format_load(peak_data['load'])} MW at {peak_data['time'].strftime('%I:%M %p')}\n"
            f"Minimum Load: {self._format_load(min_data['load'])} MW at {min_data['time'].strftime('%I:%M %p')}\n"
            f"Average Load: {self._format_load(avg_load)} MW\n"
            f"Load is {trend_data['direction']} "
            f"({trend_data['percentage']}% change over period)"
        )

    def _format_load(self, load: float) -> str:
        """Format load value with thousands separator.
        
        Args:
            load: Load value in MW
            
        Returns:
            str: Formatted load string
        """
        return f"{load:,.0f}"
