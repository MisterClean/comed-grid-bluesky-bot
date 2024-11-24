"""Test script for nuclear data processing functionality"""
import os
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pytz
from .data_loader import NuclearDataManager, GridDataLoader
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
        self.grid_loader = GridDataLoader()
        
        # Ensure output directory exists
        self.output_dir = Path('output')
        self.output_dir.mkdir(exist_ok=True)

    def generate_chart_filename(self):
        """Generate a unique filename for the nuclear chart"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return self.output_dir / f'nuclear_test_{timestamp}.png'

    def run(self):
        """Run nuclear data processing test"""
        try:
            logger.info("Starting Nuclear data test")
            
            # Get load data for the last 24 hours
            logger.info("Fetching load data")
            load_df = self.grid_loader.get_load_data()
            if load_df.empty:
                raise ValueError("No load data available")
            
            # Update nuclear data
            logger.info("Fetching and processing nuclear data")
            self.nuclear_manager.update_data()
            
            # Calculate nuclear statistics
            nuclear_stats = self.nuclear_analyzer.calculate_stats(load_df)
            
            # Generate test chart
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
    required_env_vars = ['EIA_API_KEY', 'GRIDSTATUS_API_KEY']
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
