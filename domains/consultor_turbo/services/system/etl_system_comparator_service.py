"""
Service to compare ETL vs System metadata for Consultor Turbo
"""

import json
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

from .from_system_service import ConsultorTurboFromSystemService


class ConsultorTurboETLSystemComparatorService:
    """Service to compare ETL metadata with System metadata"""
    
    def __init__(self, output_path: Path, etl_metadata_path: Path, system_metadata_path: Path):
        """
        Initialize the service
        
        Args:
            output_path: Path to save results (output/consultor_turbo)
            etl_metadata_path: Path to ETL metadata JSON
            system_metadata_path: Path to System metadata JSON (consultor_system.json)
        """
        self.output_path = Path(output_path)
        self.etl_metadata_path = Path(etl_metadata_path)
        self.system_metadata_path = Path(system_metadata_path)
        # Calculate base_dir from output_path (output/consultor_turbo -> project root)
        base_dir = self.output_path.parent.parent
        self.from_system_service = ConsultorTurboFromSystemService(
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
        """Compare ETL metadata with System metadata"""
        print("\n🔍 Comparing ETL vs System metadata...")
        
        comparison = {
            'generated_at': datetime.now().isoformat(),
            'etl_metadata': {
                'total_configs': etl_metadata.get('total_configs', 0),
                'generated_at': etl_metadata.get('generated_at', '')
            },
            'system_metadata': {
                'total_tables': len(system_metadata) if isinstance(system_metadata, dict) else 0
            },
            'comparisons': []
        }
        
        # Load system metadata using from_system_service
        system_map = self.from_system_service._load_system_metadata(self.system_metadata_path)
        
        etl_configs = {config['config_name']: config for config in etl_metadata.get('configs', [])}
        
        # Compare each ETL config with System metadata
        for config_name, etl_config in etl_configs.items():
            final_table = etl_config.get('final_table', '')
            system_data = system_map.get(final_table, {})
            
            # Get unused columns from system
            unused_columns_set, unused_columns_mapping = self.from_system_service._get_unused_columns(system_data)
            
            # Extract columns from ETL config - compare using NameFile (English name)
            etl_columns = []
            columns_unused_in_system = []
            columns_used_in_system = []
            
            for col in etl_config.get('columns', []):
                name_file = col.get('NameFile', '')
                name_temp = col.get('NameTemp', '')
                
                normalized_name_file = self.normalize_column_name(name_file)
                is_unused = normalized_name_file in unused_columns_set
                is_used = not is_unused  # Invertido: true = usado, false = não usado
                
                column_info = {
                    'name_file': name_file,
                    'name_temp': name_temp,
                    'type_file': col.get('TypeFile', ''),
                    'type_temp': col.get('TypeTemp', ''),
                    'is_key': col.get('Key', False),
                    'is_used_in_system': is_used
                }
                
                etl_columns.append(column_info)
                
                if is_unused:
                    columns_unused_in_system.append(column_info)
                else:
                    columns_used_in_system.append(column_info)
            
            comparison_item = {
                'config_name': config_name,
                'final_table': final_table,
                'has_system_data': bool(system_data),
                'etl_columns_count': len(etl_columns),
                'columns_unused_in_system_count': len(columns_unused_in_system),
                'columns_used_in_system_count': len(columns_used_in_system),
                'system_unused_columns_count': len(unused_columns_set),
                'columns_unused_in_system': columns_unused_in_system,
                'columns_used_in_system': columns_used_in_system,
                'system_unused_columns': list(unused_columns_set)
            }
            
            comparison['comparisons'].append(comparison_item)
            
            status = "✅" if system_data else "⚠️"
            print(f"   {status} {config_name} ({final_table}): ETL={len(etl_columns)} | Unused={len(columns_unused_in_system)} | Used={len(columns_used_in_system)} | System Unused={len(unused_columns_set)}")
        
        # Find ETL configs without corresponding System data
        etl_without_system = []
        for config_name, etl_config in etl_configs.items():
            final_table = etl_config.get('final_table', '')
            if final_table not in system_map:
                etl_without_system.append({
                    'config_name': config_name,
                    'config_file': etl_config.get('config_file', ''),
                    'final_table': final_table,
                    'columns_count': len(etl_config.get('columns', []))
                })
        
        comparison['etl_configs_without_system'] = etl_without_system
        comparison['etl_configs_without_system_count'] = len(etl_without_system)
        
        if etl_without_system:
            print(f"\n⚠️  {len(etl_without_system)} ETL config(s) without corresponding System data")
        
        return comparison
    
    def compare(self):
        """Execute complete ETL vs System metadata comparison"""
        print("🚀 Starting ETL vs System metadata comparison - Consultor Turbo...")
        
        # Load ETL metadata
        etl_metadata = self.load_etl_metadata()
        
        if not etl_metadata.get('configs'):
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
        print(f"   - ETL configs: {comparison_result['etl_metadata']['total_configs']}")
        print(f"   - System tables: {comparison_result['system_metadata']['total_tables']}")
        print(f"   - Comparisons made: {len(comparison_result['comparisons'])}")
        print(f"   - ETL configs without System: {comparison_result['etl_configs_without_system_count']}")

