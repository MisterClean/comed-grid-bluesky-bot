from typing import Dict, Any, Optional
import os
import time
from pathlib import Path
from atproto_client import Client
from datetime import datetime
import pytz

from src.interfaces import SocialPoster, PostingError
from src.utils.logger import setup_logger
from src.utils.config import load_config

logger = setup_logger()

class BlueSkyPoster(SocialPoster):
    """Handles posting updates to BlueSky social network."""

    def __init__(self):
        """Initialize the BlueSkyPoster with configuration and client."""
        self.config = load_config()['posting']
        self.client: Optional[Client] = None
        self._initialize_client()

    def _initialize_client(self) -> None:
        """Initialize the BlueSky API client.
        
        Raises:
            PostingError: If credentials are missing or authentication fails
        """
        try:
            username = os.getenv('BLUESKY_USERNAME')
            password = os.getenv('BLUESKY_PASSWORD')
            
            if not username or not password:
                raise PostingError(
                    "BLUESKY_USERNAME and BLUESKY_PASSWORD environment variables must be set"
                )
            
            self.client = Client()
            self.client.login(username, password)
            logger.info("Successfully authenticated with BlueSky")
            
        except Exception as e:
            error_msg = f"Failed to initialize BlueSky client: {str(e)}"
            logger.error(error_msg)
            raise PostingError(error_msg)

    def post_update(self, stats: Dict[str, Any], chart_path: str) -> bool:
        """Post an update to BlueSky with load statistics and visualization.
        
        Args:
            stats: Dictionary containing load statistics
            chart_path: Path to the chart image
            
        Returns:
            bool: True if post was successful, False otherwise
            
        Raises:
            PostingError: If there is an error posting the update
        """
        if not self.client:
            raise PostingError("BlueSky client not initialized")

        try:
            # Verify chart file exists
            if not Path(chart_path).exists():
                raise PostingError(f"Chart file not found: {chart_path}")

            # Format the post text
            post_text = self._format_post_text(stats)
            
            # Upload the image
            image_blob = self._upload_image(chart_path)
            
            # Create the post with retries
            success = self._create_post_with_retry(post_text, image_blob)
            
            return success

        except Exception as e:
            error_msg = f"Error posting update: {str(e)}"
            logger.error(error_msg)
            raise PostingError(error_msg)

    def _format_post_text(self, stats: Dict[str, Any]) -> str:
        """Format statistics into a post message that fits BlueSky's 300 character limit.
        
        Args:
            stats: Dictionary containing load statistics
            
        Returns:
            str: Formatted post text
        """
        # Use the period summary directly since it's already properly formatted
        text = stats['period_summary']
        
        # Final safety check - truncate if too long
        if len(text) > 300:
            text = text[:297] + "..."
            
        return text

    def _upload_image(self, image_path: str) -> Dict[str, Any]:
        """Upload an image to BlueSky.
        
        Args:
            image_path: Path to the image file
            
        Returns:
            Dict[str, Any]: Response containing the uploaded image blob data
            
        Raises:
            PostingError: If image upload fails
        """
        try:
            with open(image_path, 'rb') as f:
                image_data = f.read()
            
            response = self.client.com.atproto.repo.upload_blob(image_data)
            logger.info("Successfully uploaded image to BlueSky")
            return response.blob
            
        except Exception as e:
            raise PostingError(f"Failed to upload image: {str(e)}")

    def _create_post_with_retry(
        self,
        text: str,
        image_blob: Dict[str, Any]
    ) -> bool:
        """Create a post with retry logic.
        
        Args:
            text: Post text content
            image_blob: Uploaded image blob data
            
        Returns:
            bool: True if post was successful
            
        Raises:
            PostingError: If all retry attempts fail
        """
        retry_count = 0
        while retry_count < self.config['retry_attempts']:
            try:
                # Create embed with image
                embed = {
                    '$type': 'app.bsky.embed.images',
                    'images': [{
                        'alt': 'ComEd Grid Load Chart - Last 24 hours of power consumption in megawatts. Data From Grid Status',
                        'image': image_blob,
                        'aspectRatio': {'width': 16, 'height': 9}
                    }]
                }

                # Find byte indices for the link text
                link_text = "Data From Grid Status"
                byte_start = text.encode('utf-8').find(link_text.encode('utf-8'))
                byte_end = byte_start + len(link_text.encode('utf-8'))

                # Create facet for the link
                facets = [{
                    'index': {
                        'byteStart': byte_start,
                        'byteEnd': byte_end
                    },
                    'features': [{
                        '$type': 'app.bsky.richtext.facet#link',
                        'uri': 'https://www.gridstatus.io/'
                    }]
                }]
                
                # Create the post record data
                record = {
                    'text': text,
                    'embed': embed,
                    'facets': facets,
                    'createdAt': datetime.now(pytz.UTC).isoformat(),
                    '$type': 'app.bsky.feed.post'
                }
                
                # Create the post with proper data structure
                data = {
                    'collection': 'app.bsky.feed.post',
                    'repo': self.client.me.did,
                    'record': record
                }
                
                # Create the post
                self.client.com.atproto.repo.create_record(data=data)
                
                logger.info("Successfully posted update to BlueSky")
                return True
                
            except Exception as e:
                retry_count += 1
                if retry_count < self.config['retry_attempts']:
                    wait_time = 2 ** retry_count  # Exponential backoff
                    logger.warning(
                        f"Post attempt {retry_count} failed. "
                        f"Retrying in {wait_time} seconds..."
                    )
                    time.sleep(wait_time)
                else:
                    raise PostingError(
                        f"Failed to create post after {retry_count} attempts: {str(e)}"
                    )
        
        return False

    def _format_time(self, dt: datetime) -> str:
        """Format a datetime object for display.
        
        Args:
            dt: Datetime object to format
            
        Returns:
            str: Formatted time string
        """
        if dt.tzinfo is None:
            dt = pytz.UTC.localize(dt)
        target_tz = pytz.timezone(self.config['data_settings']['timezones']['target'])
        local_time = dt.astimezone(target_tz)
        return local_time.strftime('%I:%M %p %Z')
