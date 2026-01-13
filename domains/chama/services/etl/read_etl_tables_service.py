"""
Service to load and extract information from Chama ETL architecture
"""

from pathlib import Path
from typing import List, Dict, Any
import json
import sys

# Add project path to import shared
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))


class ChamaConfigLoaderService:
    """Service to load and extract information from ETL architecture file"""
    
    def __init__(self, config_path: Path):
        """
        Initialize the service with the configuration path
        Note: config_path is kept for compatibility but not used.
        The architecture file is hardcoded to domains/chama/etl_architecture/etl_metadata_architecture.json
        
        Args:
            config_path: Path parameter (kept for compatibility, not used)
        """
        self.config_path = Path(config_path)
        # Hardcoded path to ETL architecture file
        base_dir = Path(__file__).parent.parent.parent.parent.parent
        self.architecture_file = base_dir / "domains" / "chama" / "etl_architecture" / "etl_metadata_architecture.json"
    
    def load_all(self) -> List[Dict[str, Any]]:
        """
        Load ETL architecture from the single JSON file
        
        Returns:
            List of file configurations (each file can have multiple mappings)
        """
        if not self.architecture_file.exists():
            print(f"⚠️  Architecture file not found: {self.architecture_file}")
            return []
        
        try:
            with open(self.architecture_file, 'r', encoding='utf-8') as f:
                architecture_data = json.load(f)
            
            # Return the files array directly (maintaining the same structure)
            files = architecture_data.get('files', [])
            print(f"📁 Loaded {len(files)} files from architecture")
            return files
            
        except Exception as e:
            print(f"❌ Error loading architecture file: {e}")
            return []
    
    def extract_config_info(self, file_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract information from a file configuration.
        Maintains the same structure as the input (no transformation).
        
        Args:
            file_config: Dictionary with file configuration from architecture file
                        Structure: {"name": "...", "mappings": [...]}
            
        Returns:
            Dictionary with the same structure (no transformation)
        """
        # Return the file configuration as-is, maintaining the same structure
        return file_config

