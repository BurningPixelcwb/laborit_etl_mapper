"""
Service to compare S3 vs ETL metadata for Consultor Turbo
"""

import json
from pathlib import Path
from typing import Dict, Any
from datetime import datetime


class ConsultorTurboS3ComparatorService:
    """Service to compare S3 metadata with ETL metadata"""
    
    def __init__(self, output_path: Path, etl_metadata_path: Path, s3_metadata_path: Path):
        """
        Initialize the service
        
        Args:
            output_path: Path to save results (output/consultor_turbo)
            etl_metadata_path: Path to ETL metadata JSON
            s3_metadata_path: Path to S3 metadata JSON (from_santander_metadata.json)
        """
        self.output_path = Path(output_path)
        self.etl_metadata_path = Path(etl_metadata_path)
        self.s3_metadata_path = Path(s3_metadata_path)
    
    def normalize_column_name(self, col_name: str) -> str:
        """Normalize column name for comparison"""
        return col_name.lower().strip()
    
    def load_etl_metadata(self) -> Dict[str, Any]:
        """Load ETL metadata JSON"""
        if not self.etl_metadata_path.exists():
            print(f"⚠️  ETL metadata file not found: {self.etl_metadata_path}")
            return {}
        
        with open(self.etl_metadata_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def load_s3_metadata(self) -> Dict[str, Any]:
        """Load S3 metadata JSON"""
        if not self.s3_metadata_path.exists():
            print(f"⚠️  S3 metadata file not found: {self.s3_metadata_path}")
            return {}
        
        with open(self.s3_metadata_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def compare_metadatas(self, s3_metadata: Dict[str, Any], etl_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Compare S3 metadata with ETL metadata"""
        print("\n🔍 Comparing S3 vs ETL metadata...")
        
        comparison = {
            'generated_at': datetime.now().isoformat(),
            's3_metadata': {
                'total_files': s3_metadata.get('total_files', 0),
                'generated_at': s3_metadata.get('generated_at', '')
            },
            'etl_metadata': {
                'total_configs': etl_metadata.get('total_configs', 0),
                'generated_at': etl_metadata.get('generated_at', '')
            },
            'comparisons': []
        }
        
        s3_files = {file['base_name']: file for file in s3_metadata.get('files', [])}
        etl_configs = {config['config_name']: config for config in etl_metadata.get('configs', [])}
        
        # Compare each S3 file with ETL config
        for base_name, s3_file_data in s3_files.items():
            etl_config = etl_configs.get(base_name, {})
            
            s3_columns = {col['normalized_name']: col['name'] for col in s3_file_data.get('columns', [])}
            etl_columns = {}
            
            if etl_config:
                # Extract columns from ETL config - compare using NameTemp
                for col in etl_config.get('columns', []):
                    name_temp = col.get('NameTemp', '')
                    if name_temp:
                        etl_columns[self.normalize_column_name(name_temp)] = {
                            'name_file': col.get('NameFile', ''),
                            'name_temp': name_temp,
                            'type_file': col.get('TypeFile', ''),
                            'type_temp': col.get('TypeTemp', ''),
                            'is_key': col.get('Key', False)
                        }
            
            # Find columns in S3 but not in ETL
            columns_only_in_s3 = []
            columns_in_both = []
            columns_only_in_etl = []
            
            for s3_norm, s3_orig in s3_columns.items():
                if s3_norm in etl_columns:
                    columns_in_both.append({
                        's3_name': s3_orig,
                        'etl_name_temp': etl_columns[s3_norm]['name_temp'],
                        'etl_name_file': etl_columns[s3_norm]['name_file'],
                        'type_file': etl_columns[s3_norm]['type_file'],
                        'type_temp': etl_columns[s3_norm]['type_temp'],
                        'is_key': etl_columns[s3_norm]['is_key']
                    })
                else:
                    columns_only_in_s3.append(s3_orig)
            
            # Find columns in ETL but not in S3
            for etl_norm, etl_data in etl_columns.items():
                if etl_norm not in s3_columns:
                    columns_only_in_etl.append({
                        'name_temp': etl_data['name_temp'],
                        'name_file': etl_data['name_file'],
                        'type_file': etl_data['type_file'],
                        'type_temp': etl_data['type_temp'],
                        'is_key': etl_data['is_key']
                    })
            
            comparison_item = {
                'base_name': base_name,
                's3_file': s3_file_data.get('file_name', ''),
                'has_etl_config': bool(etl_config),
                'etl_config_name': etl_config.get('config_name', '') if etl_config else '',
                'etl_final_table': etl_config.get('final_table', '') if etl_config else '',
                's3_columns_count': s3_file_data.get('columns_count', 0),
                'etl_columns_count': len(etl_config.get('columns', [])) if etl_config else 0,
                'columns_in_both_count': len(columns_in_both),
                'columns_only_in_s3_count': len(columns_only_in_s3),
                'columns_only_in_etl_count': len(columns_only_in_etl),
                'columns_in_both': columns_in_both,
                'columns_only_in_s3': columns_only_in_s3,
                'columns_only_in_etl': columns_only_in_etl
            }
            
            comparison['comparisons'].append(comparison_item)
            
            status = "✅" if etl_config else "⚠️"
            print(f"   {status} {base_name}: S3={s3_file_data.get('columns_count', 0)} | ETL={len(etl_config.get('columns', [])) if etl_config else 0} | Both={len(columns_in_both)} | Only S3={len(columns_only_in_s3)} | Only ETL={len(columns_only_in_etl)}")
        
        # Find ETL configs without corresponding S3 file
        etl_without_s3 = []
        for config_name, etl_config in etl_configs.items():
            if config_name not in s3_files:
                etl_without_s3.append({
                    'config_name': config_name,
                    'config_file': etl_config.get('config_file', ''),
                    'final_table': etl_config.get('final_table', ''),
                    'columns_count': len(etl_config.get('columns', []))
                })
        
        comparison['etl_configs_without_s3'] = etl_without_s3
        comparison['etl_configs_without_s3_count'] = len(etl_without_s3)
        
        if etl_without_s3:
            print(f"\n⚠️  {len(etl_without_s3)} ETL config(s) without corresponding S3 file")
        
        return comparison
    
    def compare(self):
        """Execute complete S3 vs ETL metadata comparison"""
        print("🚀 Starting S3 vs ETL metadata comparison - Consultor Turbo...")
        
        # Load S3 metadata (should already be generated)
        s3_metadata = self.load_s3_metadata()
        
        if not s3_metadata.get('files'):
            print("⚠️  No S3 files found to compare!")
            return
        
        # Load ETL metadata
        etl_metadata = self.load_etl_metadata()
        
        if not etl_metadata.get('configs'):
            print("⚠️  No ETL metadata found to compare!")
            return
        
        # Compare metadatas
        comparison_result = self.compare_metadatas(s3_metadata, etl_metadata)
        
        # Save comparison result in s3_vs_etl directory
        s3_vs_etl_dir = self.output_path / "s3_vs_etl"
        s3_vs_etl_dir.mkdir(parents=True, exist_ok=True)
        
        comparison_file = s3_vs_etl_dir / "s3_vs_etl_metadata.json"
        with open(comparison_file, 'w', encoding='utf-8') as f:
            json.dump(comparison_result, f, indent=2, ensure_ascii=False)
        
        print(f"\n✅ Comparison result saved to: {comparison_file}")
        print(f"\n✨ Comparison completed!")
        print(f"   - S3 files: {comparison_result['s3_metadata']['total_files']}")
        print(f"   - ETL configs: {comparison_result['etl_metadata']['total_configs']}")
        print(f"   - Comparisons made: {len(comparison_result['comparisons'])}")
        print(f"   - ETL configs without S3: {comparison_result['etl_configs_without_s3_count']}")
