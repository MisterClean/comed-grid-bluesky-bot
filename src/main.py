# main.py
import os
import sys
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from .data_loader import GridDataLoader, NuclearDataManager
from .load_visualizer import LoadVisualizer
from .nuclear_visualizer import NuclearVisualizer
from .load_analyzer import LoadAnalyzer
from .nuclear_analyzer import NuclearAnalyzer
from .bluesky_poster import BlueSkyPoster
from .utils.logger import setup_logger
from .utils.config import load_config

logger = setup_logger()

class ComedLoadApp:
    def __init__(self):
        self.config = load_config()
        self.processes = self.config['posting']['processes']
        
        # Initialize components based on enabled processes
        if self.processes['load']['enabled']:
            self.data_loader = GridDataLoader()
            self.load_visualizer = LoadVisualizer()
            self.load_analyzer = LoadAnalyzer()
            
        if self.processes['nuclear']['enabled']:
            self.nuclear_manager = NuclearDataManager()
            self.nuclear_visualizer = NuclearVisualizer()
            self.nuclear_analyzer = NuclearAnalyzer()
            
        self.poster = BlueSkyPoster()
        
        # Ensure output directory exists
        self.output_dir = Path('output')
        self.output_dir.mkdir(exist_ok=True)

    def generate_chart_filename(self, prefix):
        """Generate a unique filename for a chart"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return self.output_dir / f'{prefix}_{timestamp}.png'

    def run(self):
        """Run the main application logic"""
        try:
            logger.info("Starting ComEd update cycle")
            success = True
            
            # Process load data if enabled
            if self.processes['load']['enabled']:
                try:
                    logger.info("Processing load data")
                    load_df = self.data_loader.get_load_data()
                    if load_df.empty:
                        raise ValueError("No data received from GridStatus API")

                    # Calculate load stats first since we need them for the chart
                    load_stats = self.load_analyzer.calculate_stats(load_df)
                    
                    # Create and save the load chart
                    load_chart_path = self.generate_chart_filename('comed_load')
                    self.load_visualizer.create_load_chart(load_df, output_path=str(load_chart_path))
                    
                    # Post the update with stats and chart
                    self.poster.post_load_update(load_stats, str(load_chart_path))
                    logger.info("Load data processing completed")
                except Exception as e:
                    logger.error(f"Error processing load data: {str(e)}")
                    success = False

            # Process nuclear data if enabled
            if self.processes['nuclear']['enabled']:
                try:
                    logger.info("Processing nuclear data")
                    self.nuclear_manager.update_data()
                    nuclear_stats = self.nuclear_analyzer.calculate_stats(load_df if 'load_df' in locals() else None)
                    
                    nuclear_chart_path = self.generate_chart_filename('nuclear')
                    self.nuclear_visualizer.create_nuclear_chart(
                        nuclear_stats['nuclear_data'],
                        nuclear_stats,
                        output_path=str(nuclear_chart_path)
                    )
                    
                    self.poster.post_nuclear_update(nuclear_stats, str(nuclear_chart_path))
                    logger.info("Nuclear data processing completed")
                except Exception as e:
                    logger.error(f"Error processing nuclear data: {str(e)}")
                    success = False

            logger.info("Update cycle completed")
            return success

        except Exception as e:
            logger.error(f"Error in main execution: {str(e)}")
            self.handle_error(e)
            return False

    def handle_error(self, error):
        """Handle application errors"""
        error_msg = f"ComEd Load Bot Error: {str(error)}"
        logger.exception(error_msg)

def cleanup_old_files():
    """Clean up old chart files"""
    try:
        output_dir = Path('output')
        if output_dir.exists():
            # Keep only the 10 most recent files (5 each for load and nuclear)
            files = sorted(output_dir.glob('*.png'), 
                         key=lambda x: x.stat().st_mtime, 
                         reverse=True)
            for file in files[10:]:  # Keep 10 most recent files
                file.unlink()
    except Exception as e:
        logger.error(f"Error cleaning up old files: {e}")

def main():
    # Load environment variables
    load_dotenv()
    
    # Check required environment variables based on enabled processes
    config = load_config()
    processes = config['posting']['processes']
    
    required_env_vars = ['BLUESKY_USERNAME', 'BLUESKY_PASSWORD']
    if processes['load']['enabled']:
        required_env_vars.append('GRIDSTATUS_API_KEY')
    if processes['nuclear']['enabled']:
        required_env_vars.append('EIA_API_KEY')
    
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)

    # Initialize and run application
    try:
        app = ComedLoadApp()
        success = app.run()
        
        # Clean up old files
        cleanup_old_files()
        
        # Exit with appropriate status code
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.exception("Unhandled error in main application")
        sys.exit(1)

if __name__ == "__main__":
    main()
