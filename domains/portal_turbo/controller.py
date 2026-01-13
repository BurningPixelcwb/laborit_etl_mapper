"""
Controller for Portal Turbo domain
"""

import sys
import yaml
from pathlib import Path
from typing import Dict, Any, List, Optional

# Add project path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from .services.etl.read_etl_tables_service import PortalTurboConfigLoaderService
from .services.etl.etl_metadata_service import PortalTurboETLMetadataService
from .services.s3.s3_metadata_service import PortalTurboS3MetadataService
from .services.s3.s3_comparator_service import PortalTurboS3ComparatorService
from .services.consolidated.consolidated_metadata_service import PortalTurboConsolidatedMetadataService
from .services.documentation.documentation_service import PortalTurboDocumentationService
from .services.system.portal_system_generator_service import PortalTurboSystemGeneratorService

# Import Confluence integration
try:
    from domains.confluence_integration import ConfluenceIntegrationController
    CONFLUENCE_AVAILABLE = True
except ImportError:
    CONFLUENCE_AVAILABLE = False


class PortalTurboController:
    """Controller that orchestrates Portal Turbo operations"""
    
    def __init__(self, config_path: Path, output_path: Path, csv_dir_name: str = "csv_portal_tables"):
        """
        Initialize the controller
        
        Args:
            config_path: Path to JSON configurations
            output_path: Path to save outputs
            csv_dir_name: Directory name for CSVs (not used currently)
        """
        self.config_path = Path(config_path)
        self.output_path = Path(output_path)
        self.csv_dir_name = csv_dir_name
        self.base_dir = Path(__file__).parent.parent.parent
        
        # Initialize services
        self.config_loader = PortalTurboConfigLoaderService(self.config_path)
        self.etl_metadata_service = PortalTurboETLMetadataService(self.output_path)
        
        # Load S3 comparison config
        self._load_s3_config()
    
    def _load_s3_config(self):
        """Load S3 comparison configuration from projects.yaml"""
        config_file = self.base_dir / "config" / "projects.yaml"
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            projects = config.get('projects', {})
            portal_config = projects.get('portal_turbo', {})
            s3_config = portal_config.get('s3_comparison', {})
            
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
        """Generate all Portal Turbo documentation"""
        print("🚀 Starting documentation generation - Portal Turbo...")
        print(f"📁 Reading configurations from: {self.config_path}")
        
        # Load configurations
        configs_raw = self.config_loader.load_all()
        
        if not configs_raw:
            print("❌ No configuration files found!")
            return
        
        print(f"✅ {len(configs_raw)} configuration files found")
        
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
        
        # Generate portal_system.json from .md files
        print("\n📋 Generating portal_system.json from .md files...")
        system_path = self.base_dir / "domains" / "portal_turbo" / "portal_system"
        system_generator = PortalTurboSystemGeneratorService(
            system_path=system_path,
            output_path=self.output_path
        )
        system_generator.generate_portal_system_json()
        
        # Generate consolidated metadata
        print("\n📦 Generating consolidated metadata...")
        consolidator = PortalTurboConsolidatedMetadataService(
            base_dir=self.base_dir,
            output_path=self.output_path
        )
        consolidator.consolidate()
        
        # Generate documentation
        print("\n📄 Generating Markdown documentation...")
        documentation_service = PortalTurboDocumentationService(
            base_dir=self.base_dir,
            output_path=self.output_path
        )
        documentation_service.generate_documentation()
        
        # Statistics
        self._print_statistics(configs_info)
    
    def publish_to_confluence(self, parent_page_title: Optional[str] = None) -> Dict[str, Any]:
        """
        Publish documentation to Confluence
        
        Args:
            parent_page_title: Optional custom parent page title.
                              If not provided, uses: "portal_turbo - {date}"
        
        Returns:
            Dictionary with publication results
        """
        if not CONFLUENCE_AVAILABLE:
            return {
                "success": False,
                "message": "Confluence integration not available. Check if domains.confluence_integration is installed."
            }
        
        documentation_path = self.output_path / "documentation"
        
        if not documentation_path.exists():
            return {
                "success": False,
                "message": f"Documentation path does not exist: {documentation_path}"
            }
        
        # Check if there are markdown files
        md_files = list(documentation_path.glob("*.md"))
        if not md_files:
            return {
                "success": False,
                "message": f"No markdown files found in {documentation_path}"
            }
        
        print(f"\n📤 Publishing documentation to Confluence...")
        print(f"📁 Documentation path: {documentation_path}")
        print(f"📄 Found {len(md_files)} markdown files")
        
        try:
            confluence_controller = ConfluenceIntegrationController()
            
            if not confluence_controller.client:
                return {
                    "success": False,
                    "message": "Confluence API token not configured. Set CONFLUENCE_API_TOKEN in .env file."
                }
            
            results = confluence_controller.publish_documentation(
                documentation_path=documentation_path,
                project_name="portal_turbo",
                parent_page_title=parent_page_title
            )
            
            if results.get('success'):
                print(f"\n✅ Successfully published to Confluence!")
                print(f"   Parent page: {results.get('parent_page_title')}")
                print(f"   Pages published: {len([p for p in results.get('pages', []) if p.get('success')])}")
            else:
                print(f"\n❌ Publication failed: {results.get('message', 'Unknown error')}")
            
            return results
            
        except Exception as e:
            error_msg = f"Error publishing to Confluence: {e}"
            print(f"\n❌ {error_msg}")
            import traceback
            traceback.print_exc()
            return {
                "success": False,
                "message": error_msg,
                "error": str(e)
            }
    
    def _run_s3_comparison(self):
        """Generate S3 metadata and run S3 vs ETL metadata comparison"""
        print("\n🔍 Running S3 vs ETL metadata comparison...")
        
        # First, generate S3 metadata
        s3_metadata_service = PortalTurboS3MetadataService(
            s3_path=self.s3_path,
            output_path=self.output_path
        )
        s3_metadata_service.generate_from_santander_metadata()
        
        # Then, compare S3 vs ETL
        etl_metadata_path = self.output_path / "etl_metadata" / "portal_etl_metadata.json"
        s3_metadata_path = self.output_path / "from_santander_metadata" / "from_santander_metadata.json"
        
        comparator = PortalTurboS3ComparatorService(
            output_path=self.output_path,
            etl_metadata_path=etl_metadata_path,
            s3_metadata_path=s3_metadata_path
        )
        comparator.compare()
    
    def _print_statistics(self, configs_info: List[Dict[str, Any]]):
        """Print generation statistics"""
        print("\n✨ Documentation generated successfully!")
        print(f"\n📊 Statistics:")
        print(f"   - Total configurations: {len(configs_info)}")
        print(f"   - Total mapped fields: {sum(c['columns_count'] for c in configs_info)}")
        print(f"   - Configurations with soft delete: {sum(1 for c in configs_info if c['soft_delete'])}")
    
    def compare_s3_etl(self, s3_path: Path, etl_path: Path, output_path: Path):
        """Compare S3 vs ETL files (used by external scripts)"""
        # First, generate S3 metadata
        s3_metadata_service = PortalTurboS3MetadataService(
            s3_path=s3_path,
            output_path=output_path
        )
        s3_metadata_service.generate_from_santander_metadata()
        
        # Then, compare S3 vs ETL
        etl_metadata_path = self.base_dir / "output" / "portal_turbo" / "etl_metadata" / "portal_etl_metadata.json"
        s3_metadata_path = output_path / "from_santander_metadata" / "from_santander_metadata.json"
        
        comparator = PortalTurboS3ComparatorService(
            output_path=output_path,
            etl_metadata_path=etl_metadata_path,
            s3_metadata_path=s3_metadata_path
        )
        comparator.compare()

