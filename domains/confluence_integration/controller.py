"""
Controller for Confluence Integration domain
"""

import sys
import yaml
import os
from pathlib import Path
from typing import Dict, Any, Optional

# Add project path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    # Load .env from project root
    base_dir = Path(__file__).parent.parent.parent
    env_path = base_dir / '.env'
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        # Try to load from current directory as fallback
        load_dotenv()
except ImportError:
    # python-dotenv not installed, load .env manually
    base_dir = Path(__file__).parent.parent.parent
    env_path = base_dir / '.env'
    if env_path.exists():
        try:
            with open(env_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip().strip('"').strip("'")
                        if key and value:
                            os.environ.setdefault(key, value)
        except Exception:
            pass

from .services.confluence_client import ConfluenceClient


class ConfluenceIntegrationController:
    """Controller that orchestrates Confluence integration operations"""
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize the controller
        
        Args:
            config_path: Optional path to confluence config file.
                        If not provided, uses default config/confluence.yaml
        """
        self.base_dir = Path(__file__).parent.parent.parent
        
        # Load Confluence configuration
        if config_path is None:
            config_path = self.base_dir / "config" / "confluence.yaml"
        
        self.config_path = Path(config_path)
        self.config = self._load_config()
        
        # Initialize Confluence client if config is valid
        self.client = None
        
        if self._validate_config():
            self.client = ConfluenceClient(
                url=self.config['url'],
                username=self.config['username'],
                api_token=self.config['api_token'],
                space_key=self.config['space_key']
            )
    
    def _load_config(self) -> Dict[str, Any]:
        """Load Confluence configuration from YAML file and .env"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f) or {}
            
            # Priority: .env > environment variable > config file
            # Get API token from .env (loaded via dotenv) or environment variable
            api_token = (
                os.getenv('CONFLUENCE_API_TOKEN', '') or 
                config.get('api_token', '')
            )
            if api_token:
                config['api_token'] = api_token
            
            # Allow overriding other config from .env
            config['url'] = os.getenv('CONFLUENCE_URL', config.get('url', ''))
            config['username'] = os.getenv('CONFLUENCE_USERNAME', config.get('username', ''))
            config['space_key'] = os.getenv('CONFLUENCE_SPACE_KEY', config.get('space_key', ''))
            
            return config
        except Exception as e:
            print(f"⚠️  Warning: Could not load Confluence config: {e}")
            return {}
    
    def _validate_config(self) -> bool:
        """Validate that required configuration is present"""
        required_keys = ['url', 'username', 'api_token', 'space_key']
        
        for key in required_keys:
            if not self.config.get(key):
                print(f"❌ Missing required configuration: {key}")
                if key == 'api_token':
                    print("   💡 Configure CONFLUENCE_API_TOKEN in .env file or environment variable")
                    print("   💡 Create .env file in project root with: CONFLUENCE_API_TOKEN=your-token")
                return False
        
        return True
    
    def get_config(self) -> Dict[str, Any]:
        """Get current Confluence configuration (without sensitive data)"""
        config_copy = self.config.copy()
        # Don't expose API token in config
        if 'api_token' in config_copy:
            config_copy['api_token'] = '***' if config_copy['api_token'] else ''
        return config_copy

