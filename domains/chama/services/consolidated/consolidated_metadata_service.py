"""
Service to consolidate all metadata sources (ETL, S3, System) into a single JSON
"""

import json
from pathlib import Path
from typing import Dict, Any
from datetime import datetime


class ChamaConsolidatedMetadataService:
    """Service to consolidate ETL, S3, and System metadata into final JSON"""
    
    def __init__(self, base_dir: Path, output_path: Path):
        """
        Initialize the consolidator service
        
        Args:
            base_dir: Base directory of the project
            output_path: Output directory for consolidated metadata
        """
        self.base_dir = Path(base_dir)
        self.output_path = Path(output_path)
    
    def _normalize_name(self, name: str) -> str:
        """Normalize column name for comparison"""
        return name.lower().strip()
    
    def _check_field_in_s3(self, from_santander: str, s3_comparison: Dict[str, Any]) -> bool:
        """
        Check if a field exists in S3 metadata by looking at the S3 vs ETL comparison.
        A field exists in S3 if it's in the 'fields_in_both' list.
        """
        if not s3_comparison:
            return False
        
        normalized_from_santander = self._normalize_name(from_santander)
        
        # Check in fields_in_both
        for field in s3_comparison.get('fields_in_both', []):
            s3_from_santander = field.get('from_santander', '')
            if self._normalize_name(s3_from_santander) == normalized_from_santander:
                return True
        
        # Also check in fields_only_in_s3 (though this shouldn't happen for ETL fields)
        for field in s3_comparison.get('fields_only_in_s3', []):
            s3_from_santander = field.get('from_santander', '')
            if self._normalize_name(s3_from_santander) == normalized_from_santander:
                return True
        
        return False
    
    def consolidate(self) -> None:
        """
        Generate consolidated JSON with all metadata sources.
        Maintains the same structure as ETL (files[] → mappings[] → fields[])
        but adds exists_in_s3 and is_used_in_system flags to each field.
        """
        print("📝 Consolidating all metadata sources...")
        
        # Load all metadata files
        etl_metadata_path = self.base_dir / "output" / "chama" / "etl_metadata" / "chama_etl_metadata.json"
        s3_metadata_path = self.base_dir / "output" / "chama" / "from_santander_metadata" / "from_santander_metadata.json"
        s3_comparison_path = self.base_dir / "output" / "chama" / "s3_vs_etl" / "s3_vs_etl_metadata.json"
        system_comparison_path = self.base_dir / "output" / "chama" / "etl_vs_system" / "etl_vs_system_metadata.json"
        
        # Load ETL metadata
        etl_metadata = {}
        if etl_metadata_path.exists():
            with open(etl_metadata_path, 'r', encoding='utf-8') as f:
                etl_metadata = json.load(f)
        else:
            print(f"⚠️  ETL metadata not found: {etl_metadata_path}")
            return
        
        # Load S3 metadata
        s3_metadata = {}
        if s3_metadata_path.exists():
            with open(s3_metadata_path, 'r', encoding='utf-8') as f:
                s3_metadata = json.load(f)
        
        # Load S3 comparison
        s3_comparison_data = {}
        if s3_comparison_path.exists():
            with open(s3_comparison_path, 'r', encoding='utf-8') as f:
                s3_comparison_data = json.load(f)
        
        # Load System comparison
        system_comparison_data = {}
        if system_comparison_path.exists():
            with open(system_comparison_path, 'r', encoding='utf-8') as f:
                system_comparison_data = json.load(f)
        
        # Create mappings for quick lookup by file_name
        s3_comparison_map = {}
        for comp in s3_comparison_data.get('comparisons', []):
            file_name = comp.get('file_name', '')
            s3_comparison_map[file_name] = comp
        
        # Create mapping for system comparison: file_name -> mappings[]
        system_comparison_map = {}
        for file_comp in system_comparison_data.get('files', []):
            file_name = file_comp.get('file_name', '')
            # Map by file_name -> {map_name -> mapping_data}
            mapping_map = {}
            for mapping_comp in file_comp.get('mappings', []):
                map_name = mapping_comp.get('map', '')
                mapping_map[map_name] = mapping_comp
            system_comparison_map[file_name] = mapping_map
        
        # Create mapping for S3 files
        s3_files_map = {}
        for file_data in s3_metadata.get('files', []):
            file_name = file_data.get('name', '')
            s3_files_map[file_name] = file_data
        
        # Consolidate all information - maintain files[] structure
        consolidated = {
            'generated_at': datetime.now().isoformat(),
            'files': []
        }
        
        etl_files = etl_metadata.get('files', [])
        
        for etl_file in etl_files:
            file_name = etl_file.get('name', '')
            etl_mappings = etl_file.get('mappings', [])
            
            # Get S3 file data
            s3_file_data = s3_files_map.get(file_name, {})
            
            # Get S3 comparison for this file
            s3_comparison = s3_comparison_map.get(file_name, {})
            
            # Get System comparison for this file
            system_file_comparison = system_comparison_map.get(file_name, {})
            
            consolidated_file = {
                'name': file_name,
                'mappings': []
            }
            
            # Process each mapping
            for etl_mapping in etl_mappings:
                map_name = etl_mapping.get('map', '')
                table_name = etl_mapping.get('table', '')
                etl_fields = etl_mapping.get('fields', [])
                
                # Get System comparison for this specific mapping
                system_mapping_comp = system_file_comparison.get(map_name, {})
                
                # Get fields from system comparison (they already have is_used_in_system)
                system_fields_map = {}
                for sys_field in system_mapping_comp.get('fields', []):
                    from_santander = sys_field.get('from_santander', '')
                    normalized = self._normalize_name(from_santander)
                    system_fields_map[normalized] = sys_field
                
                # Build consolidated fields with all information
                consolidated_fields = []
                
                for etl_field in etl_fields:
                    from_santander = etl_field.get('from_santander', '')
                    laborit = etl_field.get('laborit', '')
                    field_type = etl_field.get('type', '')
                    
                    # Check if field exists in S3 (using S3 comparison data)
                    exists_in_s3 = self._check_field_in_s3(from_santander, s3_comparison)
                    
                    # Get is_used_in_system from system comparison
                    normalized_from_santander = self._normalize_name(from_santander)
                    system_field = system_fields_map.get(normalized_from_santander, {})
                    is_used_in_system = system_field.get('is_used_in_system', False)
                    
                    # Create consolidated field
                    consolidated_field = {
                        'from_santander': from_santander,
                        'laborit': laborit,
                        'type': field_type,
                        'exists_in_s3': exists_in_s3,
                        'is_used_in_system': is_used_in_system
                    }
                    
                    consolidated_fields.append(consolidated_field)
                
                # Calculate statistics for this mapping
                total_fields = len(consolidated_fields)
                used_in_system_count = sum(1 for f in consolidated_fields if f.get('is_used_in_system', False))
                unused_in_system_count = total_fields - used_in_system_count
                exists_in_s3_count = sum(1 for f in consolidated_fields if f.get('exists_in_s3', False))
                
                consolidated_mapping = {
                    'map': map_name,
                    'table': table_name,
                    'fields': consolidated_fields,
                    'statistics': {
                        'total_fields': total_fields,
                        'exists_in_s3_count': exists_in_s3_count,
                        'used_in_system_count': used_in_system_count,
                        'unused_in_system_count': unused_in_system_count
                    },
                    's3_comparison': {
                        'has_s3_file': bool(s3_file_data),
                        'fields_in_both_count': s3_comparison.get('fields_in_both_count', 0),
                        'fields_only_in_s3_count': s3_comparison.get('fields_only_in_s3_count', 0),
                        'fields_only_in_etl_count': s3_comparison.get('fields_only_in_etl_count', 0)
                    },
                    'system_comparison': {
                        'has_system_data': system_mapping_comp.get('has_system_data', False),
                        'system_unused_columns_count': system_mapping_comp.get('system_unused_columns_count', 0)
                    }
                }
                
                consolidated_file['mappings'].append(consolidated_mapping)
            
            consolidated['files'].append(consolidated_file)
        
        # Save consolidated metadata in consolidated_metadata directory
        consolidated_dir = self.output_path / "consolidated_metadata"
        consolidated_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = consolidated_dir / "consolidated_metadata.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(consolidated, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Consolidated metadata saved to: {output_file}")
        
        # Print summary
        total_files = len(consolidated['files'])
        total_mappings = sum(len(f.get('mappings', [])) for f in consolidated['files'])
        total_fields = sum(
            len(m.get('fields', []))
            for f in consolidated['files']
            for m in f.get('mappings', [])
        )
        print(f"\n📊 Summary:")
        print(f"   - Total files: {total_files}")
        print(f"   - Total mappings: {total_mappings}")
        print(f"   - Total fields: {total_fields}")

