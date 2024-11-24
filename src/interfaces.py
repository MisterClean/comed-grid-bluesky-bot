from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
import pandas as pd
from datetime import datetime

class DataLoader(ABC):
    """Abstract base class for data loading operations."""
    
    @abstractmethod
    def get_load_data(self) -> pd.DataFrame:
        """Fetch load data for the configured time period.
        
        Returns:
            pd.DataFrame: DataFrame containing load data with columns:
                - interval_start_utc: UTC timestamp of interval start
                - interval_end_utc: UTC timestamp of interval end
                - load.comed: Load value in MW
        
        Raises:
            DataFetchError: If there is an error fetching the data
        """
        pass

class DatabaseInterface(ABC):
    """Abstract base class for database operations."""
    
    @abstractmethod
    def get_latest_timestamp(self) -> Optional[datetime]:
        """Get the most recent timestamp from the database.
        
        Returns:
            Optional[datetime]: The latest timestamp or None if no data exists
        """
        pass
    
    @abstractmethod
    def upsert_data(self, df: pd.DataFrame) -> int:
        """Upsert data into the database.
        
        Args:
            df: DataFrame containing the data to upsert
            
        Returns:
            int: Number of records affected
            
        Raises:
            DatabaseError: If there is an error upserting the data
        """
        pass
    
    @abstractmethod
    def get_data_since(self, start_time: str) -> pd.DataFrame:
        """Retrieve data from the database since a given timestamp.
        
        Args:
            start_time: ISO format timestamp string
            
        Returns:
            pd.DataFrame: DataFrame containing the requested data
            
        Raises:
            DatabaseError: If there is an error retrieving the data
        """
        pass

class Visualizer(ABC):
    """Abstract base class for visualization operations."""
    
    @abstractmethod
    def create_24h_chart(self, df: pd.DataFrame, output_path: str) -> None:
        """Create a 24-hour load chart visualization.
        
        Args:
            df: DataFrame containing the load data
            output_path: Path where the chart should be saved
            
        Raises:
            VisualizationError: If there is an error creating the chart
        """
        pass

class Analyzer(ABC):
    """Abstract base class for data analysis operations."""
    
    @abstractmethod
    def calculate_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate statistics from the load data.
        
        Args:
            df: DataFrame containing the load data
            
        Returns:
            Dict[str, Any]: Dictionary containing calculated statistics
            
        Raises:
            AnalysisError: If there is an error calculating statistics
        """
        pass

class SocialPoster(ABC):
    """Abstract base class for social media posting operations."""
    
    @abstractmethod
    def post_update(self, stats: Dict[str, Any], chart_path: str) -> bool:
        """Post an update to social media.
        
        Args:
            stats: Dictionary containing statistics to post
            chart_path: Path to the chart image to include
            
        Returns:
            bool: True if post was successful, False otherwise
            
        Raises:
            PostingError: If there is an error posting the update
        """
        pass

# Custom Exceptions
class DataFetchError(Exception):
    """Raised when there is an error fetching data."""
    pass

class DatabaseError(Exception):
    """Raised when there is a database operation error."""
    pass

class VisualizationError(Exception):
    """Raised when there is an error creating visualizations."""
    pass

class AnalysisError(Exception):
    """Raised when there is an error performing analysis."""
    pass

class PostingError(Exception):
    """Raised when there is an error posting to social media."""
    pass
