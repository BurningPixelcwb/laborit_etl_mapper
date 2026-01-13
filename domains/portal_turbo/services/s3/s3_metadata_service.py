"""
Service to read S3 mapping JSON file and generate from_santander_metadata.json
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime


class PortalTurboS3MetadataService:
    """Service to read S3 mapping JSON file and generate metadata JSON"""
    
    def __init__(self, s3_path: Path, output_path: Path):
        """
        Initialize the service
        
        Args:
            s3_path: Path to S3 mapping JSON file directory
            output_path: Path to save results (output/portal_turbo)
        """
        self.s3_path = Path(s3_path)
        self.output_path = Path(output_path)
        self.from_santander_metadata_path = self.output_path / "from_santander_metadata"
        self.from_santander_metadata_path.mkdir(parents=True, exist_ok=True)
    
    def extract_table_name(self, map_name: str, base_name: str) -> str:
        """
        Extract table name from map or base_name.
        Removes common prefixes and date suffixes.
        """
        # Try to derive from map first
        if map_name:
            # Remove common prefixes
            table = map_name
            prefixes = ['etl_in_t_sis_dpr_', 't_d_por_', 't_x_', 't_x_por_']
            for prefix in prefixes:
                if table.startswith(prefix):
                    table = table[len(prefix):]
                    break
            return table
        
        # Fallback to base_name, removing date suffixes
        table = base_name
        # Remove date patterns like _20250819_1421
        table = re.sub(r'_\d{8}(?:_\d{4})?$', '', table)
        return table
    
    def normalize_column_name(self, col_name: str) -> str:
        """Normalize column name for comparison"""
        return col_name.lower().strip() if col_name else ''
    
    def generate_from_santander_metadata(self) -> Dict[str, Any]:
        """Generate metadata JSON from S3 mapping JSON file"""
        print("📝 Generating from_santander metadata from S3 mapping JSON file...")
        
        metadata = {
            'generated_at': datetime.now().isoformat(),
            'source_path': str(self.s3_path),
            'total_files': 0,
            'files': []
        }
        
        if not self.s3_path.exists():
            print(f"⚠️  S3 path does not exist: {self.s3_path}")
            return metadata
        
        # Look for s3_maps_input.json file
        json_file = self.s3_path / "s3_maps_input.json"
        
        if not json_file.exists():
            print(f"⚠️  S3 mapping JSON file not found: {json_file}")
            return metadata
        
        try:
            # Read the JSON file
            with open(json_file, 'r', encoding='utf-8') as f:
                s3_data = json.load(f)
            
            files_data = s3_data.get('files', [])
            print(f"📁 Found {len(files_data)} files in S3 mapping JSON")
            
            for file_data in files_data:
                try:
                    file_name = file_data.get('file_name', '')
                    base_name = file_data.get('base_name', '')
                    map_name = file_data.get('map', '')
                    columns_data = file_data.get('columns', [])
                    
                    # Extract table name from map or base_name
                    table_name = self.extract_table_name(map_name, base_name)
                    
                    # Process columns
                    processed_columns = []
                    for col in columns_data:
                        col_name = col.get('name', '').strip()
                        if col_name:  # Only add if name is not empty
                            processed_columns.append({
                                'name': col_name,
                                'laborit_name': None,  # Not available in JSON
                                'type': col.get('type'),
                                'normalized_name': self.normalize_column_name(col_name)
                            })
                    
                    file_metadata = {
                        'file_name': file_name,
                        'base_name': base_name,
                        'mapa': map_name,
                        'table': table_name,
                        'columns_count': len(processed_columns),
                        'columns': processed_columns
                    }
                    metadata['files'].append(file_metadata)
                    print(f"   ✅ Processed: {file_name} ({len(processed_columns)} columns, mapa: {map_name}, table: {table_name})")
                except Exception as e:
                    print(f"   ❌ Error processing file: {e}")
                    import traceback
                    traceback.print_exc()
                    continue
            
            metadata['total_files'] = len(metadata['files'])
            
            # Save metadata JSON
            metadata_file = self.from_santander_metadata_path / "from_santander_metadata.json"
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            print(f"✅ From Santander metadata saved to: {metadata_file}")
            return metadata
            
        except Exception as e:
            print(f"❌ Error loading S3 mapping JSON file: {e}")
            import traceback
            traceback.print_exc()
            return metadata

