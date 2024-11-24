"""Test script for nuclear data processing functionality"""
import os
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pytz
import pandas as pd
from .data_loader import NuclearDataManager
from .visualizer import NuclearVisualizer
from .analyzer import NuclearAnalyzer
from .utils.logger import setup_logger
from .utils.config import load_config

logger = setup_logger()

class NuclearTestApp:
    def __init__(self):
        self.config = load_config()
        self.nuclear_manager = NuclearDataManager()
        self.nuclear_visualizer = NuclearVisualizer()
        self.nuclear_analyzer = NuclearAnalyzer()
        
        # Ensure output directory exists
        self.output_dir = Path('output')
        self.output_dir.mkdir(exist_ok=True)

    def generate_chart_filename(self):
        """Generate a unique filename for the nuclear chart"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return self.output_dir / f'nuclear_test_{timestamp}.png'

    def create_mock_load_data(self):
        """Create mock load data for testing"""
        # Create dates in UTC
        now = datetime.now(pytz.UTC)
        
        # Create 24 hours of mock data
        dates = pd.date_range(
            end=now, 
            periods=24, 
            freq='H',
            tz='UTC'  # Explicitly set timezone to UTC
        )
        
        # Create mock load values (simulating typical load pattern)
        mock_load = [
            15000 + (i * 500 if i < 12 else (24 - i) * 500)  # Simple pattern
            for i in range(24)
        ]
        
        return pd.DataFrame({
            'interval_start_utc': dates,
            'load.comed': mock_load
        })

    def run(self):
        """Run nuclear data processing test"""
        try:
            logger.info("Starting Nuclear data test")
            
            # Use mock load data instead of fetching from API
            logger.info("Creating mock load data")
            load_df = self.create_mock_load_data()
            if load_df.empty:
                raise ValueError("Failed to create mock load data")
            
            # Calculate nuclear statistics (this will also update nuclear data)
            logger.info("Calculating nuclear statistics")
            nuclear_stats = self.nuclear_analyzer.calculate_stats(load_df)
            
            # Debug log nuclear data structure
            nuclear_data = nuclear_stats['nuclear_data']
            logger.info(f"Nuclear data columns: {nuclear_data.columns.tolist()}")
            logger.info(f"Nuclear data sample:\n{nuclear_data.head()}")
            logger.info(f"Nuclear data info:\n{nuclear_data.info()}")
            
            # Generate test chart using the processed data from stats
            chart_path = self.generate_chart_filename()
            self.nuclear_visualizer.create_nuclear_vs_load_chart(
                nuclear_stats['load_data'],
                nuclear_stats['nuclear_data'],
                output_path=str(chart_path)
            )
            
            # Print results
            print("\nNuclear Test Results:")
            print("-" * 50)
            print(f"Nuclear Coverage: {nuclear_stats['nuclear_percentage']:.1f}%")
            print(f"Total Nuclear Generation: {nuclear_stats['total_nuclear']:.2f} MW")
            print(f"Total Load: {nuclear_stats['total_load']:.2f} MW")
            print(f"Hours at Full Coverage: {nuclear_stats['full_coverage_hours']:.1f}%")
            print(f"Chart saved to: {chart_path}")
            print("-" * 50)
            
            return True

        except Exception as e:
            logger.error(f"Error in nuclear test: {str(e)}")
            return False

def main():
    # Load environment variables
    load_dotenv()
    
    # Check for required environment variables
    required_env_vars = ['EIA_API_KEY']  # Removed GRIDSTATUS_API_KEY since we're not using it
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        return 1

    # Run test
    app = NuclearTestApp()
    success = app.run()
    return 0 if success else 1

if __name__ == "__main__":
    main()
