"""
Service to generate ETL metadata JSON from extracted configs
"""

import json
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime


class ChamaETLMetadataService:
    """Service to generate ETL metadata JSON"""
    
    def __init__(self, output_path: Path):
        """
        Initialize the service
        
        Args:
            output_path: Output directory (output/chama)
        """
        self.output_path = Path(output_path)
    
    def generate_etl_metadata_json(self, configs: List[Dict[str, Any]]):
        """
        Generate single JSON file with all ETL metadata.
        Maintains the same structure as the input architecture file.
        """
        # Define the output path
        metadata_path = self.output_path / "etl_metadata"
        metadata_path.mkdir(parents=True, exist_ok=True)
        
        # Generate the metadata structure - same as input (files[] with mappings[])
        metadata = {
            'generated_at': datetime.now().isoformat(),
            'files': configs  # Keep the same structure: files[] with mappings[]
        }
        
        # Save to JSON file
        json_file = metadata_path / "chama_etl_metadata.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        print(f"   ✅ ETL metadata JSON generated: {json_file}")

