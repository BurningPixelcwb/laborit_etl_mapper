"""
Service to publish Chama consolidated metadata JSON directly to Confluence as Wiki Markup
"""

import json
import time
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
from .confluence_client import ConfluenceClient


class ChamaJsonPublicationService:
    """Service to publish Chama consolidated metadata JSON directly to Confluence as Wiki Markup"""
    
    def __init__(self, client: ConfluenceClient):
        """
        Initialize Chama JSON publication service
        
        Args:
            client: ConfluenceClient instance
        """
        self.client = client
        self.project_name = 'chama'
        # Extract prefix from project_name (for chama, prefix is 'chama')
        # This matches the pattern used in other modules
        self.project_prefix = self.project_name
    
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
        Publish Chama consolidated metadata JSON to Confluence
        
        Args:
            json_path: Path to consolidated_metadata.json file. 
                      If None, uses default path: output/chama/consolidated_metadata/consolidated_metadata.json
            parent_page_title: Optional custom parent page title
            
        Returns:
            Dictionary with publication results
        """
        # Default path for chama
        if json_path is None:
            base_path = Path(__file__).parent.parent.parent.parent.parent
            json_path = base_path / "output" / "chama" / "consolidated_metadata" / "consolidated_metadata.json"
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
        
        files = consolidated_data.get('files', [])
        
        if not files:
            return {
                "success": False,
                "message": f"No files found in JSON file: {json_path}"
            }
        
        # Count total mappings
        total_mappings = sum(len(f.get('mappings', [])) for f in files)
        
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
                parent_content = f"h1. {self.project_name} - Consolidated Metadata\n\nDocumentation generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.\n\nTotal files: {len(files)}\nTotal mappings: {total_mappings}"
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
                parent_content = f"h1. {self.project_name} - Consolidated Metadata\n\nDocumentation generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.\n\nTotal files: {len(files)}\nTotal mappings: {total_mappings}"
                parent_page = self.client.create_page(
                    title=parent_page_title,
                    content=parent_content,
                    content_format='wiki'
                )
                parent_id = parent_page['id']
                print(f"   ✅ Created parent page (ID: {parent_id})")
            
            results["parent_page_id"] = parent_id
            
            # Publish each mapping
            print(f"\n📤 Publishing {total_mappings} mappings from JSON...")
            
            mapping_index = 0
            for file_data in files:
                file_name = file_data.get('name', '')
                mappings = file_data.get('mappings', [])
                
                for mapping in mappings:
                    mapping_index += 1
                    try:
                        map_name = mapping.get('map', 'unknown')
                        table_name = mapping.get('table', 'unknown')
                        
                        # Validate table_name
                        if not table_name or table_name == 'unknown' or not table_name.strip():
                            error_msg = f"Invalid table_name value: '{table_name}'"
                            print(f"\n[{mapping_index}/{total_mappings}] ❌ Skipping: {error_msg}")
                            results["errors"].append({
                                "file": file_name,
                                "mapping": map_name,
                                "table": table_name,
                                "error": error_msg
                            })
                            results["pages"].append({
                                "success": False,
                                "file": file_name,
                                "mapping": map_name,
                                "table": table_name,
                                "error": error_msg
                            })
                            continue
                        
                        print(f"\n[{mapping_index}/{total_mappings}] Processing: {table_name} ({file_name} -> {map_name})")
                        
                        # Convert JSON to Wiki Markup
                        try:
                            wiki_content = self._json_to_wiki_content(mapping, file_name)
                            if not wiki_content or not wiki_content.strip():
                                raise ValueError("Generated wiki content is empty")
                            print(f"   📝 Wiki content generated ({len(wiki_content)} chars)")
                        except Exception as e:
                            error_msg = f"Error converting to wiki content: {str(e)}"
                            print(f"   ❌ {error_msg}")
                            import traceback
                            traceback.print_exc()
                            results["errors"].append({
                                "file": file_name,
                                "mapping": map_name,
                                "table": table_name,
                                "error": error_msg
                            })
                            results["pages"].append({
                                "success": False,
                                "file": file_name,
                                "mapping": map_name,
                                "table": table_name,
                                "error": error_msg
                            })
                            continue
                        
                        # Prefix title with project prefix: "{prefix} - {title}"
                        page_title = f"{self.project_prefix} - {table_name.strip()}"
                        
                        # Validate parent_id
                        if not parent_id:
                            error_msg = "Parent page ID is not set. Cannot create child page."
                            print(f"   ❌ {error_msg}")
                            results["errors"].append({
                                "file": file_name,
                                "mapping": map_name,
                                "table": table_name,
                                "error": error_msg
                            })
                            results["pages"].append({
                                "success": False,
                                "file": file_name,
                                "mapping": map_name,
                                "table": table_name,
                                "error": error_msg
                            })
                            continue
                        
                        print(f"   📎 Parent page ID: {parent_id}")
                        
                        # Try to create page directly - no validation, just create
                        # If it fails, then check only in the current module's parent
                        try:
                            print(f"   ➕ Creating new page...")
                            page = self.client.create_page(
                                title=page_title,
                                content=wiki_content,
                                parent_id=parent_id,
                                content_format='wiki'
                            )
                            page_id = page.get('id')
                            print(f"   ✅ Created page: {page_title} (ID: {page_id})")
                            results["pages"].append({
                                "success": True,
                                "file": file_name,
                                "mapping": map_name,
                                "table": table_name,
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
                                    existing_page_id = existing_page.get('id')
                                    existing_version = existing_page.get('version', {}).get('number')
                                    new_version = existing_version + 1 if existing_version else 1
                                    print(f"   🔄 Updating existing page (ID: {existing_page_id}, version: {existing_version} -> {new_version})...")
                                    
                                    self.client.update_page(
                                        page_id=existing_page_id,
                                        title=page_title,
                                        content=wiki_content,
                                        version=new_version,
                                        parent_id=None,  # Preserve existing parent
                                        content_format='wiki'
                                    )
                                    print(f"   ✅ Updated page: {page_title} (ID: {existing_page_id})")
                                    results["pages"].append({
                                        "success": True,
                                        "file": file_name,
                                        "mapping": map_name,
                                        "table": table_name,
                                        "title": page_title,
                                        "page_id": existing_page_id,
                                        "action": "updated"
                                    })
                                else:
                                    # Page doesn't exist in this module but creation failed
                                    # Re-raise the original error
                                    raise create_error
                            except Exception as update_error:
                                # If check/update also fails, re-raise original creation error
                                error_msg = f"Error creating/updating page: {str(create_error)}"
                                print(f"   ❌ {error_msg}")
                                import traceback
                                traceback.print_exc()
                                
                                results["errors"].append({
                                    "file": file_name,
                                    "mapping": map_name,
                                    "table": table_name,
                                    "error": error_msg
                                })
                                results["pages"].append({
                                    "success": False,
                                    "file": file_name,
                                    "mapping": map_name,
                                    "table": table_name,
                                    "error": error_msg
                                })
                                continue
                                error_msg = f"Error creating page: {str(e)}"
                                print(f"   ❌ {error_msg}")
                                import traceback
                                traceback.print_exc()
                                
                                # Provide more specific error messages
                                if "ancestor" in str(e).lower() or "parent" in str(e).lower():
                                    error_msg += " (Possible issue with parent page relationship)"
                                elif "duplicate" in str(e).lower() or "already exists" in str(e).lower():
                                    error_msg += " (Page with this title may already exist)"
                                
                                results["errors"].append({
                                    "file": file_name,
                                    "mapping": map_name,
                                    "table": table_name,
                                    "error": error_msg
                                })
                                results["pages"].append({
                                    "success": False,
                                    "file": file_name,
                                    "mapping": map_name,
                                    "table": table_name,
                                    "error": error_msg
                                })
                                continue
                        
                        # Add delay to avoid rate limiting
                        if mapping_index < total_mappings:
                            print(f"   ⏳ Waiting 1 second before next page...")
                            time.sleep(1)
                            
                    except Exception as e:
                        map_name = mapping.get('map', 'unknown')
                        table_name = mapping.get('table', 'unknown')
                        error_msg = f"Unexpected error publishing mapping {map_name} ({table_name}): {str(e)}"
                        print(f"\n[{mapping_index}/{total_mappings}] ❌ {error_msg}")
                        import traceback
                        traceback.print_exc()
                        results["errors"].append({
                            "file": file_name,
                            "mapping": map_name,
                            "table": table_name,
                            "error": error_msg
                        })
                        results["pages"].append({
                            "success": False,
                            "file": file_name,
                            "mapping": map_name,
                            "table": table_name,
                            "error": error_msg
                        })
                        continue
            
            # Check if all pages were successful
            successful_pages = [p for p in results["pages"] if p.get("success")]
            if len(successful_pages) < total_mappings:
                results["success"] = False
                results["message"] = f"Some pages failed to publish. {len(successful_pages)}/{total_mappings} succeeded."
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

