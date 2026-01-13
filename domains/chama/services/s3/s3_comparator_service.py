"""
Service to compare S3 vs ETL metadata for Chama
"""

import json
from pathlib import Path
from typing import Dict, Any
from datetime import datetime


class ChamaS3ComparatorService:
    """Service to compare S3 metadata with ETL metadata"""
    
    def __init__(self, output_path: Path, etl_metadata_path: Path, s3_metadata_path: Path):
        """
        Initialize the service
        
        Args:
            output_path: Path to save results (output/chama)
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
        """
        Compare S3 metadata with ETL metadata.
        Both have the same structure: files[] with mappings[] and fields[]
        """
        print("\n🔍 Comparing S3 vs ETL metadata...")
        
        s3_files_list = s3_metadata.get('files', [])
        etl_files_list = etl_metadata.get('files', [])
        
        comparison = {
            'generated_at': datetime.now().isoformat(),
            's3_metadata': {
                'total_files': len(s3_files_list),
                'generated_at': s3_metadata.get('generated_at', '')
            },
            'etl_metadata': {
                'total_files': len(etl_files_list),
                'generated_at': etl_metadata.get('generated_at', '')
            },
            'comparisons': []
        }
        
        # Create mapping by file name for quick lookup
        s3_files_map = {file_data['name']: file_data for file_data in s3_files_list}
        etl_files_map = {file_data['name']: file_data for file_data in etl_files_list}
        
        # Compare each file
        all_file_names = set(s3_files_map.keys()) | set(etl_files_map.keys())
        
        for file_name in sorted(all_file_names):
            s3_file_data = s3_files_map.get(file_name)
            etl_file_data = etl_files_map.get(file_name)
            
            # Collect all fields from all mappings in S3
            s3_fields_map = {}
            if s3_file_data:
                for mapping in s3_file_data.get('mappings', []):
                    for field in mapping.get('fields', []):
                        from_santander = field.get('from_santander', '')
                        if from_santander:
                            normalized = self.normalize_column_name(from_santander)
                            s3_fields_map[normalized] = {
                                'from_santander': from_santander,
                                'laborit': field.get('laborit', ''),
                                'type': field.get('type', '')
                            }
            
            # Collect all fields from all mappings in ETL
            etl_fields_map = {}
            if etl_file_data:
                for mapping in etl_file_data.get('mappings', []):
                    for field in mapping.get('fields', []):
                        from_santander = field.get('from_santander', '')
                        if from_santander:
                            normalized = self.normalize_column_name(from_santander)
                            etl_fields_map[normalized] = {
                                'from_santander': from_santander,
                                'laborit': field.get('laborit', ''),
                                'type': field.get('type', '')
                            }
            
            # Compare fields
            fields_in_both = []
            fields_only_in_s3 = []
            fields_only_in_etl = []
            
            for s3_norm, s3_field in s3_fields_map.items():
                if s3_norm in etl_fields_map:
                    etl_field = etl_fields_map[s3_norm]
                    fields_in_both.append({
                        'from_santander': s3_field['from_santander'],
                        's3_laborit': s3_field['laborit'],
                        's3_type': s3_field['type'],
                        'etl_laborit': etl_field['laborit'],
                        'etl_type': etl_field['type']
                    })
                else:
                    fields_only_in_s3.append({
                        'from_santander': s3_field['from_santander'],
                        'laborit': s3_field['laborit'],
                        'type': s3_field['type']
                    })
            
            for etl_norm, etl_field in etl_fields_map.items():
                if etl_norm not in s3_fields_map:
                    fields_only_in_etl.append({
                        'from_santander': etl_field['from_santander'],
                        'laborit': etl_field['laborit'],
                        'type': etl_field['type']
                    })
            
            # Count total fields
            s3_total_fields = len(s3_fields_map)
            etl_total_fields = len(etl_fields_map)
            
            comparison_item = {
                'file_name': file_name,
                'has_s3_file': bool(s3_file_data),
                'has_etl_file': bool(etl_file_data),
                's3_mappings_count': len(s3_file_data.get('mappings', [])) if s3_file_data else 0,
                'etl_mappings_count': len(etl_file_data.get('mappings', [])) if etl_file_data else 0,
                's3_fields_count': s3_total_fields,
                'etl_fields_count': etl_total_fields,
                'fields_in_both_count': len(fields_in_both),
                'fields_only_in_s3_count': len(fields_only_in_s3),
                'fields_only_in_etl_count': len(fields_only_in_etl),
                'fields_in_both': fields_in_both,
                'fields_only_in_s3': fields_only_in_s3,
                'fields_only_in_etl': fields_only_in_etl
            }
            
            comparison['comparisons'].append(comparison_item)
            
            status = "✅" if (s3_file_data and etl_file_data) else "⚠️"
            print(f"   {status} {file_name}: S3={s3_total_fields} fields | ETL={etl_total_fields} fields | Both={len(fields_in_both)} | Only S3={len(fields_only_in_s3)} | Only ETL={len(fields_only_in_etl)}")
        
        # Find ETL files without corresponding S3 file
        etl_without_s3 = []
        for file_name, etl_file_data in etl_files_map.items():
            if file_name not in s3_files_map:
                total_fields = sum(len(m.get('fields', [])) for m in etl_file_data.get('mappings', []))
                etl_without_s3.append({
                    'file_name': file_name,
                    'mappings_count': len(etl_file_data.get('mappings', [])),
                    'fields_count': total_fields
                })
        
        comparison['etl_files_without_s3'] = etl_without_s3
        comparison['etl_files_without_s3_count'] = len(etl_without_s3)
        
        if etl_without_s3:
            print(f"\n⚠️  {len(etl_without_s3)} ETL file(s) without corresponding S3 file")
        
        return comparison
    
    def compare(self):
        """Execute complete S3 vs ETL metadata comparison"""
        print("🚀 Starting S3 vs ETL metadata comparison - Chama...")
        
        # Load S3 metadata (should already be generated)
        s3_metadata = self.load_s3_metadata()
        
        if not s3_metadata.get('files'):
            print("⚠️  No S3 files found to compare!")
            return
        
        # Load ETL metadata
        etl_metadata = self.load_etl_metadata()
        
        if not etl_metadata.get('files'):
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
        print(f"   - ETL files: {comparison_result['etl_metadata']['total_files']}")
        print(f"   - Comparisons made: {len(comparison_result['comparisons'])}")
        print(f"   - ETL files without S3: {comparison_result['etl_files_without_s3_count']}")

