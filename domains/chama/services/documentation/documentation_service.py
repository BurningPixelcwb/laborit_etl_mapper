"""
Service to convert consolidated metadata JSON to Confluence Wiki Markup
"""

import json
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime


class ChamaDocumentationService:
    """Service to convert Chama consolidated metadata JSON to Confluence Wiki Markup"""
    
    def __init__(self, base_dir: Path, output_path: Path):
        """
        Initialize the documentation service
        
        Args:
            base_dir: Base directory of the project
            output_path: Output directory (not used for saving, just for loading JSON)
        """
        self.base_dir = Path(base_dir)
        self.output_path = Path(output_path)
    
    def _escape_wiki(self, text: str) -> str:
        """Escape special characters for Confluence Wiki Markup"""
        if text is None:
            return ""
        text = str(text)
        # Escape pipe for tables
        text = text.replace("|", "\\|")
        # Escape curly braces for macros
        text = text.replace("{", "\\{").replace("}", "\\}")
        # Replace newlines with spaces
        text = text.replace("\n", " ")
        return text
    
    def _format_boolean(self, value: bool) -> str:
        """Format boolean value for display"""
        return "✅" if value else "❌"
    
    def _json_to_wiki_table_main(self, fields: List[Dict[str, Any]], has_s3_data: bool = True) -> str:
        """
        Convert JSON fields to Confluence Wiki Markup table (main table)
        
        Args:
            fields: List of field dictionaries
            has_s3_data: Whether to include exists_in_s3 column
            
        Returns:
            Wiki Markup table string
        """
        if not fields:
            return "No fields found."
        
        wiki_lines = []
        
        # Table header
        if has_s3_data:
            wiki_lines.append("||from_santander||type||laborit||exists_in_s3||is_used_in_system||")
        else:
            wiki_lines.append("||from_santander||type||laborit||is_used_in_system||")
        
        # Table rows
        for field in fields:
            from_santander = self._escape_wiki(field.get('from_santander', ''))
            field_type = self._escape_wiki(field.get('type', ''))
            laborit = self._escape_wiki(field.get('laborit', ''))
            exists_in_s3 = self._format_boolean(field.get('exists_in_s3', False))
            is_used_in_system = self._format_boolean(field.get('is_used_in_system', False))
            
            row = f"|{from_santander}|{field_type}|{laborit}|"
            
            if has_s3_data:
                row += f"{exists_in_s3}|{is_used_in_system}|"
            else:
                row += f"{is_used_in_system}|"
            
            wiki_lines.append(row)
        
        return "\n".join(wiki_lines)
    
    def _json_to_wiki_content(self, mapping: Dict[str, Any], file_name: str) -> str:
        """
        Convert JSON mapping to complete Confluence Wiki Markup content
        
        Args:
            mapping: Mapping information dictionary from consolidated metadata
            file_name: Name of the file this mapping belongs to
            
        Returns:
            Complete Wiki Markup content string
        """
        map_name = mapping.get('map', 'Unknown')
        table_name = mapping.get('table', 'Unknown')
        fields = mapping.get('fields', [])
        
        statistics = mapping.get('statistics', {})
        total_fields = statistics.get('total_fields', len(fields))
        exists_in_s3_count = statistics.get('exists_in_s3_count', 0)
        used_in_system_count = statistics.get('used_in_system_count', 0)
        unused_in_system_count = statistics.get('unused_in_system_count', 0)
        
        s3_comparison = mapping.get('s3_comparison', {})
        has_s3_data = s3_comparison.get('has_s3_file', False)
        fields_in_both_count = s3_comparison.get('fields_in_both_count', 0)
        fields_only_in_s3_count = s3_comparison.get('fields_only_in_s3_count', 0)
        fields_only_in_etl_count = s3_comparison.get('fields_only_in_etl_count', 0)
        
        system_comparison = mapping.get('system_comparison', {})
        has_system_data = system_comparison.get('has_system_data', False)
        
        # Get current date/time
        date_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Build Wiki content
        wiki_content = []
        
        # Title
        wiki_content.append(f"h1. {table_name}")
        wiki_content.append("")
        
        # Metadata
        wiki_content.append(f"*Documentation generated on: {date_now}*")
        wiki_content.append("")
        wiki_content.append(f"*File:* {{code}}{file_name}{{code}}")
        wiki_content.append("")
        wiki_content.append(f"*Mapping:* {{code}}{map_name}{{code}}")
        wiki_content.append("")
        wiki_content.append(f"*Table:* {{code}}{table_name}{{code}}")
        wiki_content.append("")
        wiki_content.append(f"*Total Fields:* {{code}}{total_fields}{{code}}")
        wiki_content.append("")
        
        # Statistics
        wiki_content.append("h2. Statistics")
        wiki_content.append("")
        wiki_content.append(f"*Fields in S3:* {{code}}{exists_in_s3_count}{{code}}")
        wiki_content.append("")
        wiki_content.append(f"*Fields Used in System:* {{code}}{used_in_system_count}{{code}}")
        wiki_content.append("")
        wiki_content.append(f"*Fields Unused in System:* {{code}}{unused_in_system_count}{{code}}")
        wiki_content.append("")
        
        if has_s3_data:
            wiki_content.append(f"*Fields in Both (S3 and ETL):* {{code}}{fields_in_both_count}{{code}}")
            wiki_content.append("")
            if fields_only_in_s3_count > 0:
                wiki_content.append(f"*Fields Only in S3:* {{code}}{fields_only_in_s3_count}{{code}}")
                wiki_content.append("")
            if fields_only_in_etl_count > 0:
                wiki_content.append(f"*Fields Only in ETL:* {{code}}{fields_only_in_etl_count}{{code}}")
                wiki_content.append("")
        
        # Main fields table
        wiki_content.append("h2. Fields")
        wiki_content.append("")
        wiki_content.append(self._json_to_wiki_table_main(fields, has_s3_data=has_s3_data))
        wiki_content.append("")
        
        return "\n".join(wiki_content)
    
    def generate_documentation(self):
        """
        Convert consolidated metadata JSON to Wiki Markup content.
        Does not save files locally, only prepares the conversion logic.
        """
        print("📝 Converting consolidated metadata to Wiki Markup format...")
        
        # Load consolidated metadata
        consolidated_path = self.output_path / "consolidated_metadata" / "consolidated_metadata.json"
        
        if not consolidated_path.exists():
            print(f"⚠️  Consolidated metadata not found: {consolidated_path}")
            return
        
        with open(consolidated_path, 'r', encoding='utf-8') as f:
            consolidated = json.load(f)
        
        files = consolidated.get('files', [])
        
        if not files:
            print("⚠️  No files found in consolidated metadata")
            return
        
        print(f"📁 Processing {len(files)} files...")
        
        total_mappings = 0
        for file_data in files:
            file_name = file_data.get('name', '')
            mappings = file_data.get('mappings', [])
            
            for mapping in mappings:
                total_mappings += 1
                map_name = mapping.get('map', 'unknown')
                table_name = mapping.get('table', 'unknown')
                
                # Convert to Wiki Markup (not saving, just preparing)
                wiki_content = self._json_to_wiki_content(mapping, file_name)
                
                print(f"   ✅ Converted: {file_name} -> {map_name} ({table_name})")
        
        print(f"\n✅ Converted {total_mappings} mappings to Wiki Markup format")

