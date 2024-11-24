# main.py
import os
import sys
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from src.data_loader import GridDataLoader
from src.visualizer import LoadVisualizer
from src.analyzer import LoadAnalyzer
from src.bluesky_poster import BlueSkyPoster
from src.utils.logger import setup_logger
from src.utils.config import load_config

logger = setup_logger()

class ComedLoadApp:
    def __init__(self):
        self.config = load_config()
        self.data_loader = GridDataLoader()
        self.visualizer = LoadVisualizer()
        self.analyzer = LoadAnalyzer()
        self.poster = BlueSkyPoster()
        
        # Ensure output directory exists
        self.output_dir = Path('output')
        self.output_dir.mkdir(exist_ok=True)

    def generate_chart_filename(self):
        """Generate a unique filename for the chart"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return self.output_dir / f'comed_load_{timestamp}.png'

    def run(self):
        """Run the main application logic"""
        try:
            logger.info("Starting ComEd Load update cycle")
            
            # Fetch data
            logger.info("Fetching load data")
            df = self.data_loader.get_load_data()
            if df.empty:
                raise ValueError("No data received from GridStatus API")

            # Generate chart
            logger.info("Generating visualization")
            chart_path = self.generate_chart_filename()
            self.visualizer.create_24h_chart(df, output_path=str(chart_path))

            # Calculate statistics
            logger.info("Calculating load statistics")
            stats = self.analyzer.calculate_stats(df)

            # Post update
            logger.info("Posting update to BlueSky")
            self.poster.post_update(stats, str(chart_path))

            logger.info("Update cycle completed successfully")
            return True

        except Exception as e:
            logger.error(f"Error in main execution: {str(e)}")
            # Optionally send notification about failure
            self.handle_error(e)
            return False

    def handle_error(self, error):
        """Handle application errors"""
        error_msg = f"ComEd Load Bot Error: {str(error)}"
        
        # Log the full error with traceback
        logger.exception(error_msg)
        
        # If configured, could send error notification
        # self.send_error_notification(error_msg)
        
        # Optionally post error status to monitoring service
        # self.update_monitoring_status('error', error_msg)

def cleanup_old_files():
    """Clean up old chart files"""
    try:
        output_dir = Path('output')
        if output_dir.exists():
            # Keep only the 5 most recent files
            files = sorted(output_dir.glob('comed_load_*.png'), 
                         key=lambda x: x.stat().st_mtime, 
                         reverse=True)
            for file in files[5:]:  # Keep 5 most recent files
                file.unlink()
    except Exception as e:
        logger.error(f"Error cleaning up old files: {e}")

def main():
    # Load environment variables
    load_dotenv()
    
    # Basic environment check
    required_env_vars = ['GRIDSTATUS_API_KEY', 'BLUESKY_USERNAME', 'BLUESKY_PASSWORD']
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
