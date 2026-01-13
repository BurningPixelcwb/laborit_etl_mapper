"""
Service to load and extract information from Portal Turbo configurations
"""

from pathlib import Path
from typing import List, Dict, Any
import sys

# Add project path to import shared
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))
from shared.utils.file_utils import read_json_files_from_dir


class PortalTurboConfigLoaderService:
    """Service to load and extract information from ETL configurations"""
    
    def __init__(self, config_path: Path):
        """
        Initialize the service with the configuration path
        
        Args:
            config_path: Path to the directory with JSON files
        """
        self.config_path = Path(config_path)
    
    def load_all(self) -> List[Dict[str, Any]]:
        """
        Load all JSON configuration files
        
        Returns:
            List of dictionaries with configurations
        """
        return read_json_files_from_dir(self.config_path)
    
    def extract_config_info(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract relevant information from a configuration file.
        Portal Turbo specific logic.
        
        Args:
            config: Dictionary with JSON configuration
            
        Returns:
            Dictionary with extracted information
        """
        filename = config.get('_filename', '')
        db_temp = config.get('DbTemp', {})
        fields_mapper = config.get('FieldsMapper', {})
        db_file = config.get('DbFile', {})
        
        # Ensure db_temp and db_file are dicts
        if not isinstance(db_temp, dict):
            db_temp = {}
        if not isinstance(db_file, dict):
            db_file = {}
        
        table_name = db_file.get('Table', '')
        config_name = filename.replace('.json', '')
        
        # Handle FieldsMapper as list (alternative format)
        if isinstance(fields_mapper, list):
            columns = fields_mapper
            keys = [col.get('NameFile', '') for col in columns if col.get('Key', False)]
            update_fields = [col.get('NameFile', '') for col in columns if not col.get('Key', False) and col.get('NameFile')]
            period_column = ''  # Would need to check if there's a Period field in this format
        elif isinstance(fields_mapper, dict):
            columns = fields_mapper.get('Columns', [])
            keys = fields_mapper.get('Keys', [])
            update_fields = fields_mapper.get('Update', [])
            period_column = fields_mapper.get('Period', '')
        else:
            columns = []
            keys = []
            update_fields = []
            period_column = ''
        
        return {
            'config_file': filename,
            'config_name': config_name,
            'temp_table': db_temp.get('Table', ''),
            'temp_db': db_temp.get('Name', ''),
            'final_table': table_name,
            'keys': keys,
            'update_fields': update_fields,
            'period_column': period_column,
            'columns_count': len(columns),
            'columns': columns,
            'soft_delete': db_file.get('IsToPerformExclusion', False),
            'soft_delete_fields': db_file.get('FieldsToSoftDelete', []),
            'destination': db_file.get('Destination', []),
            'version': config.get('Version', 'N/A'),
            'estimated_map_name': table_name
        }

