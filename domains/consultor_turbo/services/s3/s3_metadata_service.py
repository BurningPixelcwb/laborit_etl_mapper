"""
Service to read S3 CSV files and generate from_santander_metadata.json
"""

import csv
import json
import re
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime


class ConsultorTurboS3MetadataService:
    """Service to read S3 CSV files and generate metadata JSON"""
    
    def __init__(self, s3_path: Path, output_path: Path):
        """
        Initialize the service
        
        Args:
            s3_path: Path to S3 CSV files
            output_path: Path to save results (output/consultor_turbo)
        """
        self.s3_path = Path(s3_path)
        self.output_path = Path(output_path)
        self.from_santander_metadata_path = self.output_path / "from_santander_metadata"
        self.from_santander_metadata_path.mkdir(parents=True, exist_ok=True)
    
    def extract_base_name(self, filename: str) -> str:
        """Extract base name from file removing yyyymmdd and extension."""
        name = filename.replace('.csv', '')
        name = re.sub(r'_\d{8}(?:_\d{4})?$', '', name)
        return name
    
    def detect_delimiter(self, filepath: Path) -> str:
        """Detect CSV file delimiter"""
        with open(filepath, 'r', encoding='utf-8') as f:
            first_line = f.readline()
            if ';' in first_line:
                return ';'
            return ','
    
    def get_columns_from_s3_file(self, filepath: Path) -> List[str]:
        """Read S3 CSV file and return column list"""
        delimiter = self.detect_delimiter(filepath)
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter=delimiter)
            header = next(reader)
            return [col.strip() for col in header if col.strip()]
    
    def normalize_column_name(self, col_name: str) -> str:
        """Normalize column name for comparison"""
        return col_name.lower().strip()
    
    def generate_from_santander_metadata(self) -> Dict[str, Any]:
        """Generate metadata JSON from S3 CSV files"""
        print("📝 Generating from_santander metadata from S3 CSV files...")
        
        metadata = {
            'generated_at': datetime.now().isoformat(),
            'source_path': str(self.s3_path),
            'total_files': 0,
            'files': []
        }
        
        if not self.s3_path.exists():
            print(f"⚠️  S3 path does not exist: {self.s3_path}")
            return metadata
        
        s3_files = {}
        for s3_file in self.s3_path.glob("*.csv"):
            if "Zone.Identifier" in s3_file.name:
                continue
            base_name = self.extract_base_name(s3_file.name)
            s3_files[base_name] = s3_file
        
        print(f"📁 Found {len(s3_files)} S3 CSV files")
        
        for base_name, s3_file in s3_files.items():
            try:
                columns = self.get_columns_from_s3_file(s3_file)
                
                file_metadata = {
                    'file_name': s3_file.name,
                    'base_name': base_name,
                    'columns_count': len(columns),
                    'columns': [
                        {
                            'name': col,
                            'normalized_name': self.normalize_column_name(col)
                        }
                        for col in columns
                    ]
                }
                metadata['files'].append(file_metadata)
                print(f"   ✅ Processed: {s3_file.name} ({len(columns)} columns)")
            except Exception as e:
                print(f"   ❌ Error processing {s3_file.name}: {e}")
                continue
        
        metadata['total_files'] = len(metadata['files'])
        
        # Save metadata JSON
        metadata_file = self.from_santander_metadata_path / "from_santander_metadata.json"
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        print(f"✅ From Santander metadata saved to: {metadata_file}")
        return metadata

