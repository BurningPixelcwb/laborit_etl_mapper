"""
Service to publish Portal Turbo consolidated metadata JSON directly to Confluence as Wiki Markup
"""

import json
import time
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
from .confluence_client import ConfluenceClient


class PortalTurboJsonPublicationService:
    """Service to publish Portal Turbo consolidated metadata JSON directly to Confluence as Wiki Markup"""
    
    def __init__(self, client: ConfluenceClient):
        """
        Initialize Portal Turbo JSON publication service
        
        Args:
            client: ConfluenceClient instance
        """
        self.client = client
        self.project_name = 'portal_turbo'
        # Extract first name from project_name (before underscore)
        # Example: "portal_turbo" -> "portal"
        self.project_prefix = self.project_name.split('_')[0]
    
    def _escape_wiki(self, text: str) -> str:
        """Escape special characters for Confluence Wiki Markup"""
        if text is None or text == '':
            return "-"
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
    
    def _json_to_wiki_table_main(self, columns: List[Dict[str, Any]], has_s3_data: bool = True) -> str:
        """
        Convert JSON columns to Confluence Wiki Markup table (main table)
        
        Args:
            columns: List of column dictionaries
            has_s3_data: Whether to include exists_in_s3 column
            
        Returns:
            Wiki Markup table string
        """
        if not columns:
            return "No columns found."
        
        wiki_lines = []
        
        # Table header
        if has_s3_data:
            wiki_lines.append("||from_santander||type_santander||from_etl||type_etl||is_key||used_in_sys||exists_in_s3||")
        else:
            wiki_lines.append("||from_santander||type_santander||from_etl||type_etl||is_key||used_in_sys||")
        
        # Table rows
        for column in columns:
            from_santander = self._escape_wiki(column.get('from_santander', ''))
            type_santander = self._escape_wiki(column.get('type_santander', ''))
            from_etl = self._escape_wiki(column.get('from_etl', ''))
            type_etl = self._escape_wiki(column.get('type_etl', ''))
            is_key = self._format_boolean(column.get('is_key', False))
            is_used_in_system = column.get('is_used_in_system', False)
            used_in_sys = self._format_boolean(is_used_in_system)
            
            row = f"|{from_santander}|{type_santander}|{from_etl}|{type_etl}|{is_key}|{used_in_sys}|"
            
            if has_s3_data:
                s3_info = column.get('s3_info', {})
                exists_in_s3 = self._format_boolean(s3_info.get('exists_in_s3', False))
                row += f"{exists_in_s3}|"
            
            wiki_lines.append(row)
        
        return "\n".join(wiki_lines)
    
    def _json_to_wiki_table_s3_only(self, columns_only_in_s3: List[str]) -> str:
        """
        Convert JSON columns_only_in_s3 to Confluence Wiki Markup table
        
        Args:
            columns_only_in_s3: List of column names that exist only in S3
            
        Returns:
            Wiki Markup table string or empty string if no columns
        """
        if not columns_only_in_s3:
            return ""
        
        wiki_lines = []
        wiki_lines.append("||Column Name (S3)||")
        
        for col_name in columns_only_in_s3:
            escaped_name = self._escape_wiki(col_name)
            wiki_lines.append(f"|{escaped_name}|")
        
        return "\n".join(wiki_lines)
    
    def _json_to_wiki_table_etl_only(self, columns_only_in_etl: List[Dict[str, Any]]) -> str:
        """
        Convert JSON columns_only_in_etl to Confluence Wiki Markup table
        
        Args:
            columns_only_in_etl: List of column dictionaries that exist only in ETL
            
        Returns:
            Wiki Markup table string or empty string if no columns
        """
        if not columns_only_in_etl:
            return ""
        
        wiki_lines = []
        wiki_lines.append("||from_santander||type_santander||from_etl||type_etl||is_key||")
        
        for col in columns_only_in_etl:
            from_santander = self._escape_wiki(col.get('name_temp', ''))
            type_santander = self._escape_wiki(col.get('type_temp', ''))
            from_etl = self._escape_wiki(col.get('name_file', ''))
            type_etl = self._escape_wiki(col.get('type_file', ''))
            is_key = self._format_boolean(col.get('is_key', False))
            
            wiki_lines.append(f"|{from_santander}|{type_santander}|{from_etl}|{type_etl}|{is_key}|")
        
        return "\n".join(wiki_lines)
    
    def _json_to_wiki_content(self, table_info: Dict[str, Any]) -> str:
        """
        Convert JSON table info to complete Confluence Wiki Markup content
        
        Args:
            table_info: Table information dictionary from consolidated metadata
            
        Returns:
            Complete Wiki Markup content string
        """
        final_table = table_info.get('final_table', 'Unknown')
        config_name = table_info.get('config_name', '')
        columns = table_info.get('columns', [])
        columns_count = table_info.get('columns_count', len(columns))
        
        # Get current date/time
        date_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Build Wiki content
        wiki_content = []
        
        # Title
        wiki_content.append(f"h1. {final_table}")
        wiki_content.append("")
        
        # Metadata
        wiki_content.append(f"*Documentation generated on: {date_now}*")
        wiki_content.append("")
        wiki_content.append(f"*Config Name:* {{code}}{config_name}{{code}}")
        wiki_content.append("")
        wiki_content.append(f"*Final Table:* {{code}}{final_table}{{code}}")
        wiki_content.append("")
        wiki_content.append(f"*Total Columns:* {{code}}{columns_count}{{code}}")
        wiki_content.append("")
        
        # Check if S3 data is available
        # Verify if any column has s3_info (more reliable than has_s3_file)
        s3_comparison_info = table_info.get('s3_comparison', {})
        has_s3_file_flag = s3_comparison_info.get('has_s3_file', False)
        # Check if any column has s3_info data
        has_s3_info_in_columns = any(col.get('s3_info') for col in columns)
        # Show S3 column if either has_s3_file is true OR any column has s3_info
        has_s3_data = has_s3_file_flag or has_s3_info_in_columns
        
        # Main columns table
        wiki_content.append("h2. Columns")
        wiki_content.append("")
        wiki_content.append(self._json_to_wiki_table_main(columns, has_s3_data=has_s3_data))
        wiki_content.append("")
        
        # Get S3 comparison data
        s3_comp_data = table_info.get('s3_comparison', {})
        
        # Columns only in S3 section
        columns_only_in_s3 = s3_comp_data.get('columns_only_in_s3', [])
        if columns_only_in_s3:
            columns_only_in_s3_count = len(columns_only_in_s3)
            wiki_content.append("h2. Columns Only in S3")
            wiki_content.append("")
            wiki_content.append("These columns exist in the S3 file but are not mapped in the ETL configuration:")
            wiki_content.append("")
            wiki_content.append(f"*Total Columns Only in S3:* {{code}}{columns_only_in_s3_count}{{code}}")
            wiki_content.append("")
            wiki_content.append(self._json_to_wiki_table_s3_only(columns_only_in_s3))
            wiki_content.append("")
        
        # Columns only in ETL section
        columns_only_in_etl = s3_comp_data.get('columns_only_in_etl', [])
        if columns_only_in_etl:
            columns_only_in_etl_count = s3_comp_data.get('columns_only_in_etl_count', len(columns_only_in_etl))
            wiki_content.append("h2. Columns Only in ETL")
            wiki_content.append("")
            wiki_content.append("These columns are mapped in the ETL configuration but do not exist in the S3 file:")
            wiki_content.append("")
            wiki_content.append(f"*Total Columns Only in ETL:* {{code}}{columns_only_in_etl_count}{{code}}")
            wiki_content.append("")
            wiki_content.append(self._json_to_wiki_table_etl_only(columns_only_in_etl))
            wiki_content.append("")
        
        return "\n".join(wiki_content)
    
    def _load_json(self, json_path: Path) -> Dict[str, Any]:
        """Load JSON file"""
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def publish_from_json(
        self,
        json_path: Optional[Path] = None,
        parent_page_title: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Publish Portal Turbo consolidated metadata JSON to Confluence
        
        Args:
            json_path: Path to consolidated_metadata.json file.
                      If None, uses default path: output/portal_turbo/consolidated_metadata/consolidated_metadata.json
            parent_page_title: Optional custom parent page title
            
        Returns:
            Dictionary with publication results
        """
        # Default path for portal_turbo
        if json_path is None:
            base_path = Path(__file__).parent.parent.parent.parent.parent
            json_path = base_path / "output" / "portal_turbo" / "consolidated_metadata" / "consolidated_metadata.json"
        else:
            json_path = Path(json_path)
        
        if not json_path.exists():
            return {
                "success": False,
                "message": f"JSON file does not exist: {json_path}"
            }
        
        # Load JSON
        try:
            consolidated_data = self._load_json(json_path)
        except Exception as e:
            return {
                "success": False,
                "message": f"Error loading JSON file: {str(e)}"
            }
        
        tables = consolidated_data.get('tables', [])
        
        if not tables:
            return {
                "success": False,
                "message": f"No tables found in JSON file: {json_path}"
            }
        
        # Create parent page title
        if not parent_page_title:
            date_str = datetime.now().strftime("%Y-%m-%d")
            parent_page_title = f"{self.project_name} - Consolidated Metadata - {date_str}"
        
        results = {
            "success": True,
            "parent_page_title": parent_page_title,
            "parent_page_id": None,
            "pages": [],
            "errors": []
        }
        
        try:
            # Create or get parent page
            print(f"📄 Creating/updating parent page: {parent_page_title}")
            existing_parent = self.client.get_page_by_title(parent_page_title)
            
            if existing_parent:
                # Update parent page
                parent_id = existing_parent['id']
                version = existing_parent['version']['number'] + 1
                parent_content = f"h1. {self.project_name} - Consolidated Metadata\n\nDocumentation generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.\n\nTotal tables: {len(tables)}"
                self.client.update_page(
                    page_id=parent_id,
                    title=parent_page_title,
                    content=parent_content,
                    version=version,
                    content_format='wiki'
                )
                print(f"   ✅ Updated parent page (ID: {parent_id})")
            else:
                # Create parent page
                parent_content = f"h1. {self.project_name} - Consolidated Metadata\n\nDocumentation generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.\n\nTotal tables: {len(tables)}"
                parent_page = self.client.create_page(
                    title=parent_page_title,
                    content=parent_content,
                    content_format='wiki'
                )
                parent_id = parent_page['id']
                print(f"   ✅ Created parent page (ID: {parent_id})")
            
            results["parent_page_id"] = parent_id
            
            # Publish each table
            print(f"\n📤 Publishing {len(tables)} tables from JSON...")
            
            for i, table_info in enumerate(tables, 1):
                try:
                    final_table = table_info.get('final_table', 'unknown')
                    config_name = table_info.get('config_name', 'unknown')
                    
                    print(f"\n[{i}/{len(tables)}] Processing: {final_table} ({config_name})")
                    
                    # Convert JSON to Wiki Markup
                    wiki_content = self._json_to_wiki_content(table_info)
                    
                    # Prefix title with project prefix: "{prefix} - {title}"
                    page_title = f"{self.project_prefix} - {final_table}"
                    
                    # Try to create page directly - no validation, just create
                    # If it fails, then check only in the current module's parent
                    try:
                        page = self.client.create_page(
                            title=page_title,
                            content=wiki_content,
                            parent_id=parent_id,
                            content_format='wiki'
                        )
                        page_id = page['id']
                        print(f"   ✅ Created page: {page_title} (ID: {page_id})")
                        results["pages"].append({
                            "success": True,
                            "table": final_table,
                            "config_name": config_name,
                            "title": page_title,
                            "page_id": page_id,
                            "action": "created"
                        })
                    except Exception as create_error:
                        # Only if creation fails, check if page exists in THIS module's parent
                        # Don't check other modules at all
                        try:
                            existing_page = self.client.get_page_by_title_and_parent(page_title, parent_id)
                            if existing_page:
                                # Page exists in this module - update it
                                page_id = existing_page['id']
                                version = existing_page['version']['number'] + 1
                                self.client.update_page(
                                    page_id=page_id,
                                    title=page_title,
                                    content=wiki_content,
                                    version=version,
                                    parent_id=None,  # Preserve existing parent
                                    content_format='wiki'
                                )
                                print(f"   ✅ Updated page: {page_title} (ID: {page_id})")
                                results["pages"].append({
                                    "success": True,
                                    "table": final_table,
                                    "config_name": config_name,
                                    "title": page_title,
                                    "page_id": page_id,
                                    "action": "updated"
                                })
                            else:
                                # Page doesn't exist in this module but creation failed
                                # Re-raise the original error
                                raise create_error
                        except Exception:
                            # If check also fails, re-raise original creation error
                            raise create_error
                    
                    # Add delay to avoid rate limiting (5 seconds)
                    if i < len(tables):
                        time.sleep(5)
                        
                except Exception as e:
                    final_table = table_info.get('final_table', 'unknown')
                    config_name = table_info.get('config_name', 'unknown')
                    error_msg = f"Error publishing table {final_table} ({config_name}): {str(e)}"
                    print(f"   ❌ {error_msg}")
                    results["errors"].append({
                        "table": final_table,
                        "config_name": config_name,
                        "error": error_msg
                    })
                    results["pages"].append({
                        "success": False,
                        "table": final_table,
                        "config_name": config_name,
                        "error": error_msg
                    })
                    continue
            
            # Check if all pages were successful
            successful_pages = [p for p in results["pages"] if p.get("success")]
            if len(successful_pages) < len(tables):
                results["success"] = False
                results["message"] = f"Some pages failed to publish. {len(successful_pages)}/{len(tables)} succeeded."
            else:
                results["message"] = f"Successfully published {len(successful_pages)} pages."
            
            return results
            
        except Exception as e:
            error_msg = f"Error during publication: {str(e)}"
            print(f"❌ {error_msg}")
            import traceback
            traceback.print_exc()
            return {
                "success": False,
                "message": error_msg,
                "error": str(e),
                "pages": results.get("pages", []),
                "errors": results.get("errors", [])
            }


