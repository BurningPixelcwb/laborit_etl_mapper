"""
Service to read S3 metadata JSON and generate from_santander_metadata.json
"""

import json
from pathlib import Path
from typing import Dict, Any
from datetime import datetime


class ChamaS3MetadataService:
    """Service to read S3 metadata JSON and generate from_santander_metadata.json"""
    
    def __init__(self, s3_path: Path, output_path: Path):
        """
        Initialize the service
        
        Args:
            s3_path: Path to S3 input directory (contains s3_metadata_archives.json)
            output_path: Path to save results (output/chama)
        """
        self.s3_path = Path(s3_path)
        self.output_path = Path(output_path)
        self.from_santander_metadata_path = self.output_path / "from_santander_metadata"
        self.from_santander_metadata_path.mkdir(parents=True, exist_ok=True)
        
        # Hardcoded path to S3 metadata archives JSON
        base_dir = Path(__file__).parent.parent.parent.parent.parent
        self.s3_metadata_file = base_dir / "domains" / "chama" / "s3_input" / "s3_metadata_archives.json"
    
    def generate_from_santander_metadata(self) -> Dict[str, Any]:
        """
        Generate metadata JSON from S3 metadata archives JSON.
        Maintains the same structure as the input (files[] with mappings[]).
        """
        print("📝 Generating from_santander metadata from S3 metadata archives...")
        
        if not self.s3_metadata_file.exists():
            print(f"⚠️  S3 metadata file not found: {self.s3_metadata_file}")
            return {
                'generated_at': datetime.now().isoformat(),
                'source_path': str(self.s3_metadata_file),
                'files': []
            }
        
        try:
            # Read the S3 metadata archives JSON
            with open(self.s3_metadata_file, 'r', encoding='utf-8') as f:
                s3_data = json.load(f)
            
            # Maintain the same structure, just add generated_at
            metadata = {
                'generated_at': datetime.now().isoformat(),
                'source_path': str(self.s3_metadata_file),
                'files': s3_data.get('files', [])
            }
            
            print(f"📁 Loaded {len(metadata['files'])} files from S3 metadata archives")
            
            # Save metadata JSON
            metadata_file = self.from_santander_metadata_path / "from_santander_metadata.json"
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            print(f"✅ From Santander metadata saved to: {metadata_file}")
            return metadata
            
        except Exception as e:
            print(f"❌ Error loading S3 metadata file: {e}")
            import traceback
            traceback.print_exc()
            return {
                'generated_at': datetime.now().isoformat(),
                'source_path': str(self.s3_metadata_file),
                'files': []
            }

