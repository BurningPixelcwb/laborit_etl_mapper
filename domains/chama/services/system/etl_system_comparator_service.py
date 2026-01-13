"""
Service to compare ETL vs System metadata for Chama
"""

import json
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

from .from_system_service import ChamaFromSystemService


class ChamaETLSystemComparatorService:
    """Service to compare ETL metadata with System metadata"""
    
    def __init__(self, output_path: Path, etl_metadata_path: Path, system_metadata_path: Path):
        """
        Initialize the service
        
        Args:
            output_path: Path to save results (output/chama)
            etl_metadata_path: Path to ETL metadata JSON
            system_metadata_path: Path to System metadata JSON (chama_system.json)
        """
        self.output_path = Path(output_path)
        self.etl_metadata_path = Path(etl_metadata_path)
        self.system_metadata_path = Path(system_metadata_path)
        # Calculate base_dir from output_path (output/chama -> project root)
        base_dir = self.output_path.parent.parent
        self.from_system_service = ChamaFromSystemService(
            base_dir=base_dir,
            output_path=self.output_path
        )
    
    def normalize_column_name(self, col_name: str) -> str:
        """Normalize column name for comparison"""
        return col_name.lower().strip() if col_name else ''
    
    def load_etl_metadata(self) -> Dict[str, Any]:
        """Load ETL metadata JSON"""
        if not self.etl_metadata_path.exists():
            print(f"⚠️  ETL metadata file not found: {self.etl_metadata_path}")
            return {}
        
        with open(self.etl_metadata_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def load_system_metadata(self) -> Dict[str, Any]:
        """Load System metadata JSON"""
        if not self.system_metadata_path.exists():
            print(f"⚠️  System metadata file not found: {self.system_metadata_path}")
            return {}
        
        with open(self.system_metadata_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def compare_metadatas(self, etl_metadata: Dict[str, Any], system_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compare ETL metadata with System metadata.
        Adds is_used_in_system flag to each field.
        """
        print("\n🔍 Comparing ETL vs System metadata...")
        
        etl_files_list = etl_metadata.get('files', [])
        
        comparison = {
            'generated_at': datetime.now().isoformat(),
            'etl_metadata': {
                'total_files': len(etl_files_list),
                'generated_at': etl_metadata.get('generated_at', '')
            },
            'system_metadata': {
                'total_tables': len(system_metadata) if isinstance(system_metadata, list) else 0
            },
            'files': []
        }
        
        # Load system metadata using from_system_service
        system_map = self.from_system_service._load_system_metadata(self.system_metadata_path)
        
        # Process each file from ETL
        for etl_file in etl_files_list:
            file_name = etl_file.get('name', '')
            mappings = etl_file.get('mappings', [])
            
            file_comparison = {
                'file_name': file_name,
                'mappings': []
            }
            
            # Process each mapping in the file
            for mapping in mappings:
                mapping_name = mapping.get('map', '')
                table_name = mapping.get('table', '')
                fields = mapping.get('fields', [])
                
                # Get system data for this table
                system_data = system_map.get(table_name, {})
                
                # Get unused columns from system
                unused_columns_set, unused_columns_mapping = self.from_system_service._get_unused_columns(system_data)
                
                # Process each field - add is_used_in_system flag
                fields_with_usage = []
                used_count = 0
                unused_count = 0
                
                for field in fields:
                    laborit_name = field.get('laborit', '')
                    normalized_laborit = self.normalize_column_name(laborit_name)
                    
                    # Check if field is unused (if it's in the unused_columns_set, it's NOT used)
                    is_unused = normalized_laborit in unused_columns_set
                    is_used = not is_unused  # true = usado, false = não usado
                    
                    # Create field with usage flag
                    field_with_usage = {
                        'from_santander': field.get('from_santander', ''),
                        'laborit': laborit_name,
                        'type': field.get('type', ''),
                        'is_used_in_system': is_used  # true = usado, false = não usado
                    }
                    
                    fields_with_usage.append(field_with_usage)
                    
                    if is_used:
                        used_count += 1
                    else:
                        unused_count += 1
                
                mapping_comparison = {
                    'map': mapping_name,
                    'table': table_name,
                    'has_system_data': bool(system_data),
                    'fields': fields_with_usage,
                    'statistics': {
                        'total_fields': len(fields_with_usage),
                        'used_count': used_count,
                        'unused_count': unused_count
                    },
                    'system_unused_columns_count': len(unused_columns_set)
                }
                
                file_comparison['mappings'].append(mapping_comparison)
                
                status = "✅" if system_data else "⚠️"
                print(f"   {status} {file_name} -> {mapping_name} ({table_name}): Total={len(fields_with_usage)} | Used={used_count} | Unused={unused_count}")
            
            comparison['files'].append(file_comparison)
        
        # Find ETL files/mappings without corresponding System data
        etl_without_system = []
        for etl_file in etl_files_list:
            for mapping in etl_file.get('mappings', []):
                table_name = mapping.get('table', '')
                if table_name not in system_map:
                    etl_without_system.append({
                        'file_name': etl_file.get('name', ''),
                        'map': mapping.get('map', ''),
                        'table': table_name,
                        'fields_count': len(mapping.get('fields', []))
                    })
        
        comparison['etl_without_system'] = etl_without_system
        comparison['etl_without_system_count'] = len(etl_without_system)
        
        if etl_without_system:
            print(f"\n⚠️  {len(etl_without_system)} ETL mapping(s) without corresponding System data")
        
        return comparison
    
    def compare(self):
        """Execute complete ETL vs System metadata comparison"""
        print("🚀 Starting ETL vs System metadata comparison - Chama...")
        
        # Load ETL metadata
        etl_metadata = self.load_etl_metadata()
        
        if not etl_metadata.get('files'):
            print("⚠️  No ETL metadata found to compare!")
            return
        
        # Load System metadata
        system_metadata = self.load_system_metadata()
        
        if not system_metadata:
            print("⚠️  No System metadata found to compare!")
            return
        
        # Compare metadatas
        comparison_result = self.compare_metadatas(etl_metadata, system_metadata)
        
        # Save comparison result in etl_vs_system directory
        etl_vs_system_dir = self.output_path / "etl_vs_system"
        etl_vs_system_dir.mkdir(parents=True, exist_ok=True)
        
        comparison_file = etl_vs_system_dir / "etl_vs_system_metadata.json"
        with open(comparison_file, 'w', encoding='utf-8') as f:
            json.dump(comparison_result, f, indent=2, ensure_ascii=False)
        
        print(f"\n✅ Comparison result saved to: {comparison_file}")
        print(f"\n✨ Comparison completed!")
        print(f"   - ETL files: {comparison_result['etl_metadata']['total_files']}")
        print(f"   - System tables: {comparison_result['system_metadata']['total_tables']}")
        
        # Calculate total statistics
        total_fields = 0
        total_used = 0
        total_unused = 0
        for file_comp in comparison_result.get('files', []):
            for mapping_comp in file_comp.get('mappings', []):
                stats = mapping_comp.get('statistics', {})
                total_fields += stats.get('total_fields', 0)
                total_used += stats.get('used_count', 0)
                total_unused += stats.get('unused_count', 0)
        
        print(f"   - Total fields: {total_fields}")
        print(f"   - Used in system: {total_used}")
        print(f"   - Unused in system: {total_unused}")
        print(f"   - ETL mappings without System: {comparison_result['etl_without_system_count']}")

