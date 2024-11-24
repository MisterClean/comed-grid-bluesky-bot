"""Test script for nuclear data processing functionality"""
import os
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
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

    def run(self):
        """Run nuclear data processing test"""
        try:
            logger.info("Starting Nuclear data test")
            
            # Update nuclear data
            logger.info("Fetching and processing nuclear data")
            self.nuclear_manager.update_data()
            
            # Get estimated generation
            generation_df = self.nuclear_manager.estimate_generation()
            if generation_df.empty:
                raise ValueError("No nuclear generation data available")
            
            # Calculate nuclear statistics
            nuclear_stats = self.nuclear_analyzer.calculate_stats(generation_df)
            
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
            print(f"Total Nuclear Capacity: {nuclear_stats['total_nuclear_capacity']:.2f} MW")
            print(f"Current Nuclear Generation: {nuclear_stats['current_nuclear_generation']:.2f} MW")
            print(f"Nuclear Capacity Factor: {nuclear_stats['nuclear_capacity_factor']:.1%}")
            print(f"Hours at Full Capacity: {nuclear_stats['hours_at_full_capacity']:.1f}")
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
    required_env_vars = ['EIA_API_KEY']
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
