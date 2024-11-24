import os
import sys
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime
import pytz
from dotenv import load_dotenv

from src.data_loader import GridDataLoader
from src.visualizer import LoadVisualizer
from src.analyzer import LoadAnalyzer
from src.bluesky_poster import BlueSkyPoster
from src.interfaces import (
    DataLoader,
    Visualizer,
    Analyzer,
    SocialPoster,
    DataFetchError,
    DatabaseError,
    VisualizationError,
    AnalysisError,
    PostingError
)
from src.utils.logger import setup_logger
from src.utils.config import load_config, ConfigError

logger = setup_logger()

class ComedLoadApp:
    """Main application class for the ComEd Load monitoring system."""

    def __init__(
        self,
        data_loader: Optional[DataLoader] = None,
        visualizer: Optional[Visualizer] = None,
        analyzer: Optional[Analyzer] = None,
        poster: Optional[SocialPoster] = None
    ):
        """Initialize the application with dependencies.
        
        Args:
            data_loader: Data loading implementation
            visualizer: Visualization implementation
            analyzer: Analysis implementation
            poster: Social media posting implementation
            
        Raises:
            ConfigError: If configuration loading fails
        """
        try:
            self.config = load_config()
        except Exception as e:
            raise ConfigError(f"Failed to load configuration: {str(e)}")

        # Initialize dependencies with dependency injection
        self.data_loader = data_loader or GridDataLoader()
        self.visualizer = visualizer or LoadVisualizer()
        self.analyzer = analyzer or LoadAnalyzer()
        self.poster = poster or BlueSkyPoster()
        
        # Ensure output directory exists
        self.output_dir = Path('output')
        self.output_dir.mkdir(exist_ok=True)

    def generate_chart_filename(self) -> Path:
        """Generate a unique filename for the chart.
        
        Returns:
            Path: Path object for the chart file
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return self.output_dir / f'comed_load_{timestamp}.png'

    def run(self) -> bool:
        """Run the main application logic.
        
        Returns:
            bool: True if execution was successful, False otherwise
        """
        try:
            logger.info("Starting ComEd Load update cycle")
            
            # Fetch data
            logger.info("Fetching load data")
            df = self.data_loader.get_load_data()
            if df.empty:
                raise DataFetchError("No data received from GridStatus API")

            # Generate chart
            logger.info("Generating visualization")
            chart_path = self.generate_chart_filename()
            self.visualizer.create_24h_chart(df, str(chart_path))

            # Calculate statistics
            logger.info("Calculating load statistics")
            stats = self.analyzer.calculate_stats(df)

            # Post update
            logger.info("Posting update to BlueSky")
            self.poster.post_update(stats, str(chart_path))

            logger.info("Update cycle completed successfully")
            return True

        except DataFetchError as e:
            logger.error(f"Data fetch error: {str(e)}")
            self.handle_error("Data Fetch", e)
            return False
        except DatabaseError as e:
            logger.error(f"Database error: {str(e)}")
            self.handle_error("Database", e)
            return False
        except VisualizationError as e:
            logger.error(f"Visualization error: {str(e)}")
            self.handle_error("Visualization", e)
            return False
        except AnalysisError as e:
            logger.error(f"Analysis error: {str(e)}")
            self.handle_error("Analysis", e)
            return False
        except PostingError as e:
            logger.error(f"Posting error: {str(e)}")
            self.handle_error("Social Media Posting", e)
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            self.handle_error("Application", e)
            return False

    def handle_error(self, component: str, error: Exception) -> None:
        """Handle application errors with component context.
        
        Args:
            component: Name of the component where the error occurred
            error: The exception that was raised
        """
        error_msg = f"ComEd Load Bot {component} Error: {str(error)}"
        
        # Log the full error with traceback
        logger.exception(error_msg)
        
        # Here you could add additional error handling:
        # - Send error notifications (email, Slack, etc.)
        # - Update monitoring systems
        # - Trigger fallback procedures
        # self.notify_error(error_msg)
        # self.update_monitoring_status('error', error_msg)

def cleanup_old_files(max_files: int = 5) -> None:
    """Clean up old chart files.
    
    Args:
        max_files: Maximum number of files to keep
    """
    try:
        output_dir = Path('output')
        if output_dir.exists():
            files = sorted(
                output_dir.glob('comed_load_*.png'),
                key=lambda x: x.stat().st_mtime,
                reverse=True
            )
            for file in files[max_files:]:
                try:
                    file.unlink()
                    logger.debug(f"Deleted old chart file: {file}")
                except Exception as e:
                    logger.warning(f"Failed to delete file {file}: {str(e)}")
    except Exception as e:
        logger.error(f"Error cleaning up old files: {str(e)}")

def check_environment() -> None:
    """Check required environment variables.
    
    Raises:
        EnvironmentError: If required variables are missing
    """
    required_env_vars = ['GRIDSTATUS_API_KEY', 'BLUESKY_USERNAME', 'BLUESKY_PASSWORD']
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )

def main() -> None:
    """Main entry point for the application."""
    try:
        # Load environment variables
        load_dotenv()
        
        # Check environment
        check_environment()
        
        # Initialize and run application
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
        logger.exception("Fatal error in main application")
        sys.exit(1)

if __name__ == "__main__":
    main()
