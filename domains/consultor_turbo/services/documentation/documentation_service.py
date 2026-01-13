"""
Service to generate Markdown documentation from consolidated metadata
"""

import json
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime


class ConsultorTurboDocumentationService:
    """Service to generate Markdown documentation for each table"""
    
    def __init__(self, base_dir: Path, output_path: Path):
        """
        Initialize the documentation service
        
        Args:
            base_dir: Base directory of the project
            output_path: Output directory for documentation
        """
        self.base_dir = Path(base_dir)
        self.output_path = Path(output_path)
        self.documentation_path = self.output_path / "documentation"
        self.documentation_path.mkdir(parents=True, exist_ok=True)
    
    def _escape_markdown(self, text: str) -> str:
        """Escape special characters for Markdown"""
        if text is None:
            return ""
        return str(text).replace("|", "\\|").replace("\n", " ")
    
    def _format_boolean(self, value: bool) -> str:
        """Format boolean value for display"""
        return "✅" if value else "❌"
    
    def _generate_table_row(self, column: Dict[str, Any], include_s3: bool = True) -> str:
        """Generate a table row for a column"""
        from_santander = self._escape_markdown(column.get('from_santander', ''))
        type_santander = self._escape_markdown(column.get('type_santander', ''))
        from_etl = self._escape_markdown(column.get('from_etl', ''))
        type_etl = self._escape_markdown(column.get('type_etl', ''))
        is_key = self._format_boolean(column.get('is_key', False))
        
        # Invert is_unused to used_in_sys: if is_unused is true, used_in_sys is false
        is_unused = column.get('is_unused', False)
        used_in_sys = self._format_boolean(not is_unused)
        
        row = f"| {from_santander} | {type_santander} | {from_etl} | {type_etl} | {is_key} | {used_in_sys} |"
        
        # Add S3 column only if S3 data is available
        if include_s3:
            s3_info = column.get('s3_info', {})
            exists_in_s3 = self._format_boolean(s3_info.get('exists_in_s3', False))
            row += f" {exists_in_s3} |"
        else:
            row += ""
        
        return row
    
    def _generate_table_markdown(self, table_info: Dict[str, Any], has_s3_data: bool = True) -> str:
        """Generate Markdown table for a table"""
        columns = table_info.get('columns', [])
        
        if not columns:
            return "No columns found."
        
        # Table header - conditionally include exists_in_s3
        if has_s3_data:
            markdown = "| from_santander | type_santander | from_etl | type_etl | is_key | used_in_sys | exists_in_s3 |\n"
            markdown += "|----------------|-----------------|----------|----------|--------|-------------|--------------|\n"
        else:
            markdown = "| from_santander | type_santander | from_etl | type_etl | is_key | used_in_sys |\n"
            markdown += "|----------------|-----------------|----------|----------|--------|-------------|\n"
        
        # Table rows
        for column in columns:
            markdown += self._generate_table_row(column, include_s3=has_s3_data) + "\n"
        
        return markdown
    
    def _generate_s3_only_table(self, columns_only_in_s3: List[str]) -> str:
        """Generate Markdown table for columns that exist only in S3"""
        if not columns_only_in_s3:
            return ""
        
        markdown = "| Column Name (S3) |\n"
        markdown += "|-----------------|\n"
        
        for col_name in columns_only_in_s3:
            escaped_name = self._escape_markdown(col_name)
            markdown += f"| {escaped_name} |\n"
        
        return markdown
    
    def _generate_etl_only_table(self, columns_only_in_etl: List[Dict[str, Any]]) -> str:
        """Generate Markdown table for columns that exist only in ETL"""
        if not columns_only_in_etl:
            return ""
        
        markdown = "| from_santander | type_santander | from_etl | type_etl | is_key |\n"
        markdown += "|----------------|-----------------|----------|----------|--------|\n"
        
        for col in columns_only_in_etl:
            from_santander = self._escape_markdown(col.get('name_temp', ''))
            type_santander = self._escape_markdown(col.get('type_temp', ''))
            from_etl = self._escape_markdown(col.get('name_file', ''))
            type_etl = self._escape_markdown(col.get('type_file', ''))
            is_key = self._format_boolean(col.get('is_key', False))
            
            markdown += f"| {from_santander} | {type_santander} | {from_etl} | {type_etl} | {is_key} |\n"
        
        return markdown
    
    def _generate_file_markdown(self, table_info: Dict[str, Any], s3_comparison: Dict[str, Any] = None) -> str:
        """Generate complete Markdown content for a table file"""
        final_table = table_info.get('final_table', 'Unknown')
        config_name = table_info.get('config_name', '')
        
        # Extract module prefix from output_path (e.g., "consultor_turbo" -> "consultor")
        # output_path is like: output/consultor_turbo/ or output/portal_turbo/
        module_name = self.output_path.name if self.output_path.name else 'unknown'
        project_prefix = module_name.split('_')[0] if '_' in module_name else module_name
        
        # Get current date/time
        date_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Prefix title with module prefix: "{prefix} - {title}"
        page_title = f"{project_prefix} - {final_table}"
        markdown = f"# {page_title}\n\n"
        markdown += f"*Documentation generated on: {date_now}*\n\n"
        markdown += f"**Config Name:** `{config_name}`\n\n"
        markdown += f"**Final Table:** `{final_table}`\n\n"
        
        # Add total columns count
        total_columns = table_info.get('columns_count', len(table_info.get('columns', [])))
        markdown += f"**Total Columns:** `{total_columns}`\n\n"
        
        # Check if S3 data is available - check s3_comparison in table_info
        s3_comparison_info = table_info.get('s3_comparison', {})
        has_s3_data = s3_comparison_info.get('has_s3_file', False)
        
        # Add main columns table
        markdown += "## Columns\n\n"
        markdown += self._generate_table_markdown(table_info, has_s3_data=has_s3_data)
        markdown += "\n"
        
        # Add columns only in S3 section
        if s3_comparison:
            columns_only_in_s3 = s3_comparison.get('columns_only_in_s3', [])
            if columns_only_in_s3:
                columns_only_in_s3_count = len(columns_only_in_s3)
                markdown += "## Columns Only in S3\n\n"
                markdown += "These columns exist in the S3 file but are not mapped in the ETL configuration:\n\n"
                markdown += f"**Total Columns Only in S3:** `{columns_only_in_s3_count}`\n\n"
                markdown += self._generate_s3_only_table(columns_only_in_s3)
                markdown += "\n"
            
            # Add columns only in ETL section
            columns_only_in_etl = s3_comparison.get('columns_only_in_etl', [])
            if columns_only_in_etl:
                columns_only_in_etl_count = s3_comparison.get('columns_only_in_etl_count', 0)
                markdown += "## Columns Only in ETL\n\n"
                markdown += "These columns are mapped in the ETL configuration but do not exist in the S3 file:\n\n"
                markdown += f"**Total Columns Only in ETL:** `{columns_only_in_etl_count}`\n\n"
                markdown += self._generate_etl_only_table(columns_only_in_etl)
                markdown += "\n"
        
        return markdown
    
    def generate_documentation(self):
        """Generate Markdown documentation for all tables"""
        print("📝 Generating Markdown documentation...")
        
        # Load consolidated metadata
        consolidated_path = self.output_path / "consolidated_metadata" / "consolidated_metadata.json"
        
        if not consolidated_path.exists():
            print(f"⚠️  Consolidated metadata not found: {consolidated_path}")
            return
        
        with open(consolidated_path, 'r', encoding='utf-8') as f:
            consolidated = json.load(f)
        
        # Load S3 comparison metadata to get columns_only_in_s3
        s3_comparison_path = self.output_path / "s3_vs_etl" / "s3_vs_etl_metadata.json"
        s3_comparison_map = {}
        
        if s3_comparison_path.exists():
            with open(s3_comparison_path, 'r', encoding='utf-8') as f:
                s3_comparison_data = json.load(f)
            
            # Create mapping by config_name (base_name)
            for comp in s3_comparison_data.get('comparisons', []):
                base_name = comp.get('base_name', '')
                if base_name:
                    s3_comparison_map[base_name] = comp
        
        tables = consolidated.get('tables', [])
        
        if not tables:
            print("⚠️  No tables found in consolidated metadata")
            return
        
        print(f"📁 Generating documentation for {len(tables)} tables...")
        
        for table_info in tables:
            try:
                final_table = table_info.get('final_table', 'unknown')
                config_name = table_info.get('config_name', '')
                
                # Get S3 comparison data for this table
                s3_comparison = s3_comparison_map.get(config_name) if s3_comparison_map else None
                
                # Generate safe filename
                safe_filename = final_table.replace('.', '_').replace('/', '_')
                file_path = self.documentation_path / f"{safe_filename}.md"
                
                # Generate markdown content
                markdown_content = self._generate_file_markdown(table_info, s3_comparison)
                
                # Write file
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(markdown_content)
                
                print(f"   ✅ Generated: {file_path.name}")
            except Exception as e:
                config_name = table_info.get('config_name', 'unknown')
                final_table = table_info.get('final_table', 'unknown')
                print(f"   ❌ Error generating {config_name} ({final_table}): {e}")
                import traceback
                traceback.print_exc()
                continue
        
        print(f"\n✅ Documentation generated in: {self.documentation_path}")

