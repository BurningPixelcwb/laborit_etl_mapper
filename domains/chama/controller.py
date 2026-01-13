"""
Controller for Chama domain
"""

import sys
import json
import yaml
from pathlib import Path
from typing import Dict, Any, List

# Add project path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from .services.etl.read_etl_tables_service import ChamaConfigLoaderService
from .services.etl.etl_metadata_service import ChamaETLMetadataService
from .services.s3.s3_metadata_service import ChamaS3MetadataService
from .services.s3.s3_comparator_service import ChamaS3ComparatorService
from .services.consolidated.consolidated_metadata_service import ChamaConsolidatedMetadataService
from .services.documentation.documentation_service import ChamaDocumentationService


class ChamaController:
    """Controller that orchestrates Chama operations"""
    
    def __init__(self, config_path: Path, output_path: Path):
        """
        Initialize the controller
        
        Args:
            config_path: Path to JSON configurations
            output_path: Path to save outputs
        """
        self.config_path = Path(config_path)
        self.output_path = Path(output_path)
        self.base_dir = Path(__file__).parent.parent.parent
        
        # Initialize services
        self.config_loader = ChamaConfigLoaderService(self.config_path)
        self.etl_metadata_service = ChamaETLMetadataService(self.output_path)
        
        # Load S3 comparison config
        self._load_s3_config()
    
    def _load_s3_config(self):
        """Load S3 comparison configuration from projects.yaml"""
        config_file = self.base_dir / "config" / "projects.yaml"
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            projects = config.get('projects', {})
            chama_config = projects.get('chama', {})
            s3_config = chama_config.get('s3_comparison', {})
            
            if s3_config:
                s3_path = Path(s3_config.get('s3_path', ''))
                if not s3_path.is_absolute():
                    s3_path = self.base_dir / s3_path
                self.s3_path = s3_path
            else:
                self.s3_path = None
        except Exception as e:
            print(f"⚠️  Warning: Could not load S3 config: {e}")
            self.s3_path = None
    
    def generate_docs(self):
        """Generate all Chama documentation"""
        print("🚀 Starting documentation generation - Chama...")
        print(f"📁 Reading ETL architecture from: domains/chama/etl_architecture/etl_metadata_architecture.json")
        
        # Load configurations
        configs_raw = self.config_loader.load_all()
        
        if not configs_raw:
            print("❌ No files found in architecture!")
            return
        
        print(f"✅ {len(configs_raw)} files found in architecture")
        
        # Extract information (domain-specific logic)
        configs_info = [
            self.config_loader.extract_config_info(config)
            for config in configs_raw
        ]
        
        # Generate ETL metadata JSON
        print("\n📝 Generating ETL metadata JSON...")
        self.etl_metadata_service.generate_etl_metadata_json(configs_info)
        
        # Generate S3 metadata and run comparison
        if self.s3_path:
            self._run_s3_comparison()
        
        # Generate consolidated metadata
        print("\n📦 Generating consolidated metadata...")
        consolidator = ChamaConsolidatedMetadataService(
            base_dir=self.base_dir,
            output_path=self.output_path
        )
        consolidator.consolidate()
        
        # Generate documentation
        print("\n📄 Generating Markdown documentation...")
        documentation_service = ChamaDocumentationService(
            base_dir=self.base_dir,
            output_path=self.output_path
        )
        documentation_service.generate_documentation()
        
        # Statistics
        self._print_statistics(configs_info)
    
    def _run_s3_comparison(self):
        """Generate S3 metadata and run S3 vs ETL metadata comparison"""
        print("\n🔍 Running S3 vs ETL metadata comparison...")
        
        # First, generate S3 metadata
        s3_metadata_service = ChamaS3MetadataService(
            s3_path=self.s3_path,
            output_path=self.output_path
        )
        s3_metadata_service.generate_from_santander_metadata()
        
        # Then, compare S3 vs ETL
        etl_metadata_path = self.base_dir / "output" / "chama" / "etl_metadata" / "chama_etl_metadata.json"
        s3_metadata_path = self.base_dir / "output" / "chama" / "from_santander_metadata" / "from_santander_metadata.json"
        
        comparator = ChamaS3ComparatorService(
            output_path=self.output_path,
            etl_metadata_path=etl_metadata_path,
            s3_metadata_path=s3_metadata_path
        )
        comparator.compare()
    
    def _print_statistics(self, configs_info: List[Dict[str, Any]]):
        """Print generation statistics"""
        print("\n✨ ETL metadata generated successfully!")
        print(f"\n📊 Statistics:")
        print(f"   - Total files: {len(configs_info)}")
        
        # Count total mappings and fields
        total_mappings = 0
        total_fields = 0
        for file_config in configs_info:
            mappings = file_config.get('mappings', [])
            total_mappings += len(mappings)
            for mapping in mappings:
                fields = mapping.get('fields', [])
                total_fields += len(fields)
        
        print(f"   - Total mappings: {total_mappings}")
        print(f"   - Total mapped fields: {total_fields}")
    
    def compare_s3_etl(self, s3_path: Path, etl_path: Path, output_path: Path):
        """Compare S3 vs ETL metadata (used by external scripts)"""
        # First, generate S3 metadata
        s3_metadata_service = ChamaS3MetadataService(
            s3_path=s3_path,
            output_path=output_path
        )
        s3_metadata_service.generate_from_santander_metadata()
        
        # Then, compare S3 vs ETL
        etl_metadata_path = self.base_dir / "output" / "chama" / "etl_metadata" / "chama_etl_metadata.json"
        s3_metadata_path = output_path / "from_santander_metadata" / "from_santander_metadata.json"
        
        comparator = ChamaS3ComparatorService(
            output_path=output_path,
            etl_metadata_path=etl_metadata_path,
            s3_metadata_path=s3_metadata_path
        )
        comparator.compare()

