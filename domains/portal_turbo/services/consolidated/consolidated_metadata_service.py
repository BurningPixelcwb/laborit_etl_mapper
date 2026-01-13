"""
Service to consolidate all metadata sources (ETL, S3, System) into a single JSON
"""

import json
from pathlib import Path
from typing import Dict, Any
from datetime import datetime


class PortalTurboConsolidatedMetadataService:
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
        if not name:
            return ''
        return str(name).lower().strip()
    
    def _empty_to_none(self, value: Any) -> Any:
        """Convert empty strings to None, keep other values as is"""
        if value == '' or value is None:
            return None
        return value
    
    def _check_column_in_s3(self, name_temp: str, s3_file_data: Dict[str, Any]) -> Dict[str, Any]:
        """Check if a column exists in S3 metadata"""
        s3_column_info = {'exists_in_s3': False}
        if s3_file_data and name_temp:
            for s3_col in s3_file_data.get('columns', []):
                s3_col_name = s3_col.get('name', '') or ''
                if self._normalize_name(s3_col_name) == self._normalize_name(name_temp):
                    s3_column_info = {
                        'exists_in_s3': True,
                        's3_name': self._empty_to_none(s3_col_name)
                    }
                    break
        return s3_column_info
    
    def consolidate(self) -> None:
        """Generate consolidated JSON with all table characteristics"""
        print("📝 Consolidating all metadata sources...")
        
        # Load all metadata files
        etl_metadata_path = self.base_dir / "output" / "portal_turbo" / "etl_metadata" / "portal_etl_metadata.json"
        s3_metadata_path = self.base_dir / "output" / "portal_turbo" / "from_santander_metadata" / "from_santander_metadata.json"
        s3_comparison_path = self.base_dir / "output" / "portal_turbo" / "s3_vs_etl" / "s3_vs_etl_metadata.json"
        system_comparison_path = self.base_dir / "output" / "portal_turbo" / "etl_vs_system" / "etl_vs_system_metadata.json"
        
        # Load ETL metadata
        etl_metadata = {}
        if etl_metadata_path.exists():
            with open(etl_metadata_path, 'r', encoding='utf-8') as f:
                etl_metadata = json.load(f)
        
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
        
        # Create mappings for quick lookup
        s3_comparison_map = {}
        for comp in s3_comparison_data.get('comparisons', []):
            etl_config_name = comp.get('etl_config_name', '')
            if etl_config_name:
                s3_comparison_map[etl_config_name] = comp
        
        system_comparison_map = {}
        for comp in system_comparison_data.get('comparisons', []):
            config_name = comp.get('config_name', '')
            system_comparison_map[config_name] = comp
        
        s3_files_map = {}
        for file_data in s3_metadata.get('files', []):
            mapa = file_data.get('mapa', '')
            if mapa:
                s3_files_map[mapa] = file_data
        
        # Consolidate all information
        consolidated = {
            'generated_at': datetime.now().isoformat(),
            'total_tables': len(etl_metadata.get('configs', [])),
            'tables': []
        }
        
        for etl_config in etl_metadata.get('configs', []):
            config_name = etl_config.get('config_name', '')
            final_table = etl_config.get('final_table', '')
            
            # Get S3 comparison data
            s3_comparison = s3_comparison_map.get(config_name, {})
            
            # Get System comparison data
            system_comparison = system_comparison_map.get(config_name, {})
            
            # Get S3 file data
            s3_file_data = s3_files_map.get(config_name, {})
            
            # Get system comparison data
            columns_unused_in_system = system_comparison.get('columns_unused_in_system', [])
            columns_used_in_system = system_comparison.get('columns_used_in_system', [])
            system_unused_columns = system_comparison.get('system_unused_columns', [])
            
            # Build column details with all information
            columns_detail = []
            columns_not_in_system = []  # Columns in ETL but not used in system
            
            # Create a map of columns by name_file for quick lookup (both used and unused)
            system_column_map = {}
            for col in columns_used_in_system:
                name_file = col.get('name_file', '')
                if name_file:
                    system_column_map[name_file] = col
            for col in columns_unused_in_system:
                name_file = col.get('name_file', '')
                if name_file:
                    system_column_map[name_file] = col
            
            for col in etl_config.get('columns', []):
                name_temp = self._empty_to_none(col.get('NameTemp', '') or '')
                name_file = self._empty_to_none(col.get('NameFile', '') or '')
                
                # Check if column is in S3
                s3_column_info = self._check_column_in_s3(name_temp or '', s3_file_data)
                
                # Get is_used_in_system from comparison data (true = usado, false = não usado)
                system_col_data = system_column_map.get(name_file or '', {})
                is_used = system_col_data.get('is_used_in_system', False) if system_col_data else False
                is_unused = not is_used
                
                # Track columns that are in ETL but not in system (unused)
                if is_unused:
                    columns_not_in_system.append({
                        'from_santander': name_temp,
                        'type_santander': self._empty_to_none(col.get('TypeTemp', '')),
                        'from_etl': name_file,
                        'type_etl': self._empty_to_none(col.get('TypeFile', '')),
                        'is_key': col.get('Key', False)
                    })
                
                column_info = {
                    'from_santander': name_temp,
                    'type_santander': self._empty_to_none(col.get('TypeTemp', '')),
                    'from_etl': name_file,
                    'type_etl': self._empty_to_none(col.get('TypeFile', '')),
                    'is_key': col.get('Key', False),
                    'is_used_in_system': is_used,
                    's3_info': s3_column_info
                }
                columns_detail.append(column_info)
            
            table_info = {
                'config_name': config_name,
                'config_file': self._empty_to_none(etl_config.get('config_file', '')),
                'final_table': final_table,
                'temp_table': self._empty_to_none(etl_config.get('temp_table', '')),
                'temp_db': self._empty_to_none(etl_config.get('temp_db', '')),
                'version': etl_config.get('version', 'N/A'),
                'keys': etl_config.get('keys', []),
                'update_fields': etl_config.get('update_fields', []),
                'period_column': self._empty_to_none(etl_config.get('period_column', '')),
                'soft_delete': {
                    'enabled': etl_config.get('soft_delete', {}).get('enabled', False),
                    'fields': etl_config.get('soft_delete', {}).get('fields', [])
                },
                'destination': etl_config.get('destination', []),
                'columns_count': etl_config.get('columns_count', 0),
                'columns': columns_detail,
                's3_comparison': {
                    'has_s3_file': bool(s3_file_data),
                    's3_file_name': self._empty_to_none(s3_file_data.get('file_name', '')) if s3_file_data else None,
                    's3_columns_count': s3_file_data.get('columns_count', 0) if s3_file_data else 0,
                    'columns_in_both_count': s3_comparison.get('columns_in_both_count', 0),
                    'columns_only_in_s3_count': s3_comparison.get('columns_only_in_s3_count', 0),
                    'columns_only_in_etl_count': s3_comparison.get('columns_only_in_etl_count', 0),
                    'columns_only_in_s3': s3_comparison.get('columns_only_in_s3', []),
                    'columns_only_in_etl': s3_comparison.get('columns_only_in_etl', [])
                },
                'system_comparison': {
                    'has_system_data': system_comparison.get('has_system_data', False),
                    'system_mapas': system_comparison.get('system_mapas', []),
                    'columns_unused_in_system_count': system_comparison.get('columns_unused_in_system_count', 0),
                    'columns_used_in_system_count': system_comparison.get('columns_used_in_system_count', 0),
                    'system_unused_columns_count': system_comparison.get('system_unused_columns_count', 0),
                    'columns_unused_in_system': columns_unused_in_system,
                    'columns_used_in_system': columns_used_in_system,
                    'system_unused_columns': system_unused_columns
                },
                'columns_not_in_system_count': len(columns_not_in_system),
                'columns_not_in_system': columns_not_in_system
            }
            
            consolidated['tables'].append(table_info)
        
        # Save consolidated metadata in consolidated_metadata directory
        consolidated_dir = self.output_path / "consolidated_metadata"
        consolidated_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = consolidated_dir / "consolidated_metadata.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(consolidated, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Consolidated metadata saved to: {output_file}")

