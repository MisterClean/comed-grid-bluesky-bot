from atproto import Client, client_utils
import os
from datetime import datetime
from src.utils.logger import setup_logger
from src.utils.config import load_config

logger = setup_logger()

class BlueSkyPoster:
    def __init__(self):
        self.client = self._initialize_client()
        self.config = load_config()['posting']

    def _initialize_client(self):
        """Initialize BlueSky client"""
        username = os.getenv('BLUESKY_USERNAME')
        password = os.getenv('BLUESKY_PASSWORD')
        
        if not username or not password:
            raise ValueError("BLUESKY_USERNAME and BLUESKY_PASSWORD must be set")
        
        try:
            client = Client()
            profile = client.login(username, password)
            logger.info(f"Successfully logged into BlueSky as {profile.display_name}")
            return client
        except Exception as e:
            logger.error(f"Failed to initialize BlueSky client: {str(e)}")
            raise

    def create_post_text(self, stats):
        """Create formatted post text using TextBuilder"""
        return (
            client_utils.TextBuilder()
            .text(
                f"ComEd Load Report "
                f"({stats['start_time'].strftime('%I:%M %p')} - "
                f"{stats['end_time'].strftime('%I:%M %p')} CT)\n\n"
                f"Average Load: {stats['average']:,.0f} MW\n"
                f"Maximum Load: {stats['maximum']:,.0f} MW\n"
                f"Minimum Load: {stats['minimum']:,.0f} MW\n\n"
            )
            .text("Data source: ")
            .link("PJM Interconnection", "https://www.pjm.com/markets-and-operations")
        )

    def post_update(self, stats, image_path=None):
        """Post an update to BlueSky with optional image"""
        try:
            # Create post text
            post_text = self.create_post_text(stats)
            
            # Handle image if provided
            if image_path and self.config.get('include_images', True):
                try:
                    with open(image_path, 'rb') as f:
                        image_data = f.read()
                        
                    # Upload image and post
                    logger.info("Attempting to send post with image")
                    self.client.send_image(
                        text=post_text,
                        image=image_data,
                        image_alt=f"ComEd load chart for {stats['start_time'].strftime('%Y-%m-%d')}"
                    )
                    logger.info("Post with image sent successfully")
                    return
                    
                except Exception as e:
                    logger.error(f"Failed to send post with image: {e}")
                    logger.info("Falling back to text-only post")
            
            # If we reach here, either no image was provided or image posting failed
            self.client.send_post(text=post_text)
            logger.info("Text-only post sent successfully")
            
        except Exception as e:
            logger.error(f"Error posting to BlueSky: {str(e)}")
            raise

    def test_post(self):
        """Send a test post to verify credentials and connectivity"""
        try:
            test_text = client_utils.TextBuilder().text("🔍 ComEd Load Bot - Test Post")
            self.client.send_post(text=test_text)
            logger.info("Test post sent successfully")
            return True
        except Exception as e:
            logger.error(f"Test post failed: {str(e)}")
            return False