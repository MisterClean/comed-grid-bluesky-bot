from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
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
    def create_load_chart(self, df: pd.DataFrame, output_path: str) -> None:
        """Create a load chart visualization.
        
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

class NuclearDataLoader(ABC):
    """Abstract base class for nuclear data loading operations."""
    
    @abstractmethod
    def get_reactor_status(self) -> pd.DataFrame:
        """Fetch current reactor status data.
        
        Note: This method returns raw reactor status data with timestamps at midnight.
        For accurate timing that reflects the ~9am Eastern data collection time,
        use get_latest_available_data() instead.
        
        Returns:
            pd.DataFrame: DataFrame containing reactor status with columns:
                - timestamp: UTC timestamp of status
                - unit: Reactor unit name
                - power: Power level as decimal (0-1)
        
        Raises:
            DataFetchError: If there is an error fetching the data
        """
        pass
    
    @abstractmethod
    def get_latest_available_data(self) -> pd.DataFrame:
        """Get the most recent available reactor status data.
        
        This method handles the actual ~9am Eastern data collection time and
        falls back to the most recent stored data if fresh data isn't available.
        This is the preferred method for getting reactor status data.
        
        Returns:
            pd.DataFrame: DataFrame containing reactor status with columns:
                - report_date: UTC timestamp reflecting actual collection time
                - unit_name: Reactor unit name
                - power_pct: Power level percentage (0-100)
        
        Raises:
            DataFetchError: If there is an error fetching the data
        """
        pass
    
    @abstractmethod
    def get_capacity_data(self) -> pd.DataFrame:
        """Fetch reactor capacity data.
        
        Returns:
            pd.DataFrame: DataFrame containing capacity data with columns:
                - plant_id: EIA plant ID
                - unit: Generator unit number
                - capacity_mw: Net summer capacity in MW
        
        Raises:
            DataFetchError: If there is an error fetching the data
        """
        pass
    
    @abstractmethod
    def estimate_generation(self) -> pd.DataFrame:
        """Calculate estimated nuclear generation.
        
        Returns:
            pd.DataFrame: DataFrame containing generation estimates with columns:
                - timestamp: UTC timestamp
                - unit: Reactor unit name
                - estimated_mw: Estimated generation in MW
        
        Raises:
            DataFetchError: If there is an error calculating estimates
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
