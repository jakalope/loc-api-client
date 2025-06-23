"""
Configuration Management

Handles environment variables and configuration settings for the newsagger application.
"""

import os
import logging
from typing import Optional
from pathlib import Path
from dotenv import load_dotenv


class Config:
    """Configuration management class."""
    
    def __init__(self, env_file: Optional[str] = None):
        """Initialize configuration from environment variables."""
        # Load .env file if it exists
        if env_file:
            load_dotenv(env_file)
        else:
            # Look for .env in current directory or parent directories
            env_path = Path('.env')
            if env_path.exists():
                load_dotenv(env_path)
        
        # Library of Congress API Configuration
        self.loc_base_url = os.getenv('LOC_BASE_URL', 'https://chroniclingamerica.loc.gov/')
        
        # Parse request delay with fallback to default
        try:
            self.request_delay = float(os.getenv('REQUEST_DELAY', '3.0'))
        except ValueError:
            self.request_delay = 3.0
            
        # Parse max retries with fallback to default  
        try:
            self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
        except ValueError:
            self.max_retries = 3
        
        # Data Storage
        self.database_path = os.getenv('DATABASE_PATH', './data/newsagger.db')
        self.download_dir = os.getenv('DOWNLOAD_DIR', './data/downloads')
        
        # Logging
        self.log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
        
        # Current year for date validation
        from datetime import datetime
        self.current_year = datetime.now().year
        
        # Rate limiting safety checks
        if self.request_delay < 3.0:
            logging.warning("REQUEST_DELAY is below LOC recommended minimum of 3 seconds")
            self.request_delay = 3.0
        
        # Ensure directories exist
        Path(self.database_path).parent.mkdir(parents=True, exist_ok=True)
        Path(self.download_dir).mkdir(parents=True, exist_ok=True)
    
    def setup_logging(self):
        """Configure logging based on settings."""
        logging.basicConfig(
            level=getattr(logging, self.log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('newsagger.log')
            ]
        )
    
    def validate(self) -> bool:
        """Validate configuration settings."""
        try:
            # Check if base URL is accessible (basic validation)
            import requests
            response = requests.head(self.loc_base_url, timeout=10)
            if response.status_code >= 400:
                logging.error(f"LOC base URL not accessible: {self.loc_base_url}")
                return False
        except Exception as e:
            logging.warning(f"Could not validate LOC base URL: {e}")
        
        # Check write permissions for data directories
        try:
            test_file = Path(self.download_dir) / '.test'
            test_file.touch()
            test_file.unlink()
        except Exception as e:
            logging.error(f"Cannot write to download directory {self.download_dir}: {e}")
            return False
        
        return True
    
    def get_api_config(self) -> dict:
        """Get API client configuration."""
        return {
            'base_url': self.loc_base_url,
            'request_delay': self.request_delay,
            'max_retries': self.max_retries
        }
    
    def get_storage_config(self) -> dict:
        """Get storage configuration."""
        return {
            'db_path': self.database_path,
            'download_dir': self.download_dir
        }


# Global configuration instance
config = Config()