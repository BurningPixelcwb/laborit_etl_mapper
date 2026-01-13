"""
Service to generate ETL metadata JSON from extracted configs
"""

import json
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime


class ConsultorTurboETLMetadataService:
    """Service to generate ETL metadata JSON"""
    
    def __init__(self, output_path: Path):
        """
        Initialize the service
        
        Args:
            output_path: Output directory (output/consultor_turbo)
        """
        self.output_path = Path(output_path)
    
    def generate_etl_metadata_json(self, configs: List[Dict[str, Any]]):
        """Generate single JSON file with all ETL metadata"""
        # Define the output path
        metadata_path = self.output_path / "etl_metadata"
        metadata_path.mkdir(parents=True, exist_ok=True)
        
        # Generate the metadata structure
        metadata = {
            'generated_at': datetime.now().isoformat(),
            'total_configs': len(configs),
            'configs': []
        }
        
        for info in configs:
            config_metadata = {
                'config_file': info['config_file'],
                'config_name': info['config_name'],
                'estimated_map_name': info['estimated_map_name'],
                'temp_table': info['temp_table'],
                'temp_db': info['temp_db'],
                'final_table': info['final_table'],
                'keys': info['keys'],
                'update_fields': info['update_fields'],
                'period_column': info['period_column'],
                'columns_count': info['columns_count'],
                'columns': info['columns'],
                'soft_delete': {
                    'enabled': info['soft_delete'],
                    'fields': info['soft_delete_fields']
                },
                'destination': info['destination'],
                'version': info['version']
            }
            metadata['configs'].append(config_metadata)
        
        # Save to JSON file
        json_file = metadata_path / "consultor_etl_metadata.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        print(f"   ✅ ETL metadata JSON generated: {json_file}")

