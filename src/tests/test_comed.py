"""Test script for ComEd load and grid stats functionality"""
import os
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pytz
import pandas as pd
from ..load_visualizer import LoadVisualizer
from ..load_analyzer import LoadAnalyzer
from ..utils.logger import setup_logger
from ..utils.config import load_config

logger = setup_logger()

class ComedTestApp:
    def __init__(self):
        self.config = load_config()
        self.load_visualizer = LoadVisualizer()
        self.load_analyzer = LoadAnalyzer()
        
        # Ensure output directory exists
        self.output_dir = Path('output')
        self.output_dir.mkdir(exist_ok=True)

    def generate_chart_filename(self):
        """Generate a unique filename for the load chart"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return self.output_dir / f'comed_test_{timestamp}.png'

    def create_mock_load_data(self):
        """Create mock load data for testing"""
        # Create dates in UTC
        now = datetime.now(pytz.UTC)
        
        # Create 24 hours of mock data
        dates = pd.date_range(
            end=now, 
            periods=24, 
            freq='H',
            tz='UTC'
        )
        
        # Create mock load values with a realistic daily pattern
        # Morning ramp up, afternoon peak, evening decline
        base_load = 10000  # Base load in MW
        peak_add = 8000    # Additional peak load
        
        mock_load = []
        for hour in range(24):
            if hour < 6:  # Night (midnight to 6am)
                load = base_load + (hour * 200)
            elif hour < 12:  # Morning ramp
                load = base_load + ((hour - 6) * 1000)
            elif hour < 18:  # Afternoon peak
                load = base_load + peak_add - ((hour - 12) * 200)
            else:  # Evening decline
                load = base_load + peak_add - ((hour - 12) * 800)
            mock_load.append(max(load, base_load))
        
        return pd.DataFrame({
            'interval_start_utc': dates,
            'load.comed': mock_load
        })

    def run(self):
        """Run ComEd load and grid stats test"""
        try:
            logger.info("Starting ComEd load test")
            
            # Use mock load data instead of fetching from API
            logger.info("Creating mock load data")
            load_df = self.create_mock_load_data()
            if load_df.empty:
                raise ValueError("Failed to create mock load data")
            
            # Calculate load statistics using LoadAnalyzer
            logger.info("Calculating load statistics")
            load_stats = self.load_analyzer.calculate_stats(load_df)
            
            # Extract relevant stats for display
            grid_stats = {
                'total_load': load_df['load.comed'].sum(),
                'peak_load': load_df['load.comed'].max(),
                'min_load': load_df['load.comed'].min(),
                'avg_load': load_df['load.comed'].mean(),
                'load_factor': load_stats['load_factor'],
                'peak_hour': load_stats['report_time'].strftime('%H:%M'),
                'valley_hour': (load_stats['report_time'] - timedelta(hours=12)).strftime('%H:%M')  # Example valley hour
            }
            
            # Generate test chart using the processed data
            chart_path = self.generate_chart_filename()
            self.load_visualizer.create_load_chart(
                load_df,
                output_path=str(chart_path)
            )
            
            # Print results
            print("\nComEd Load Test Results:")
            print("-" * 50)
            print(f"Total Load: {grid_stats['total_load']:.2f} MW")
            print(f"Peak Load: {grid_stats['peak_load']:.2f} MW")
            print(f"Minimum Load: {grid_stats['min_load']:.2f} MW")
            print(f"Average Load: {grid_stats['avg_load']:.2f} MW")
            print(f"Load Factor: {grid_stats['load_factor']:.2%}")
            print(f"Peak Hour: {grid_stats['peak_hour']}")
            print(f"Valley Hour: {grid_stats['valley_hour']}")
            print(f"Chart saved to: {chart_path}")
            print("-" * 50)
            
            return True

        except Exception as e:
            logger.error(f"Error in ComEd load test: {str(e)}")
            return False

def main():
    # Load environment variables
    load_dotenv()
    
    # Run test
    app = ComedTestApp()
    success = app.run()
    return 0 if success else 1

if __name__ == "__main__":
    main()
