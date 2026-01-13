"""
Service to process portal_system .md files and generate portal_system.json
"""

import re
import json
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime


class PortalTurboSystemGeneratorService:
    """Service to process .md files and generate portal_system.json"""
    
    def __init__(self, system_path: Path, output_path: Path):
        """
        Initialize the service
        
        Args:
            system_path: Path to portal_system directory with .md files
            output_path: Path to save portal_system.json
        """
        self.system_path = Path(system_path)
        self.output_path = Path(output_path)
    
    def extract_mapa_name(self, filename: str) -> str:
        """Extract mapa name from filename"""
        # Remove _unused_fields.md suffix
        name = filename.replace('_unused_fields.md', '')
        return name
    
    def parse_markdown_file(self, filepath: Path) -> Dict[str, Any]:
        """
        Parse a markdown file to extract unused fields information
        
        Format:
        # Campos Não Utilizados: {mapa_name}
        **Fonte:** `{source_file}`
        **Total de campos não utilizados:** {count}
        ## Tabela: `{table_name}`
        | STD | Laborit | Tipo |
        | ... | ... | ... |
        """
        content = filepath.read_text(encoding='utf-8')
        
        # Extract mapa name from title
        mapa_match = re.search(r'# Campos Não Utilizados:\s*(.+)', content)
        mapa_name = mapa_match.group(1).strip() if mapa_match else ''
        
        # Extract table name
        table_match = re.search(r'## Tabela:\s*`(.+?)`', content)
        table_name = table_match.group(1).strip() if table_match else ''
        
        # Extract unused columns from table
        unused_columns = []
        
        # Find the table section
        table_section_match = re.search(r'\| STD\s*\| Laborit\s*\| Tipo\s*\|', content)
        if table_section_match:
            # Get everything after the header
            table_content = content[table_section_match.end():]
            
            # Extract rows (lines starting with |)
            for line in table_content.split('\n'):
                line = line.strip()
                if not line.startswith('|') or line.startswith('|---') or '---' in line:
                    continue
                
                # Parse markdown table row: | STD | Laborit | Tipo |
                parts = [p.strip() for p in line.split('|')]
                if len(parts) >= 4:  # Empty first, STD, Laborit, Tipo, empty last
                    std_name = parts[1].strip('`').strip()
                    laborit_name = parts[2].strip('`').strip()
                    tipo = parts[3].strip('`').strip()
                    
                    # Only add if laborit_name is not empty and not a separator line
                    if laborit_name and not laborit_name.startswith('-') and laborit_name != '':
                        unused_columns.append(laborit_name)
        
        return {
            'mapa': mapa_name,
            'tabela': table_name,
            'colunas_nao_utilizadas': unused_columns
        }
    
    def generate_portal_system_json(self) -> List[Dict[str, Any]]:
        """Generate portal_system.json from all .md files"""
        print("📝 Processing portal_system .md files...")
        
        if not self.system_path.exists():
            print(f"⚠️  System path does not exist: {self.system_path}")
            return []
        
        # Find all .md files (excluding Zone.Identifier files)
        md_files = [
            f for f in self.system_path.glob("*.md")
            if "Zone.Identifier" not in f.name and "_unused_fields.md" in f.name
        ]
        
        print(f"📁 Found {len(md_files)} unused_fields .md files")
        
        system_data = []
        
        for md_file in md_files:
            try:
                parsed_data = self.parse_markdown_file(md_file)
                
                if parsed_data['mapa'] and parsed_data['tabela']:
                    system_data.append(parsed_data)
                    print(f"   ✅ Processed: {md_file.name} -> {parsed_data['mapa']} ({parsed_data['tabela']}, {len(parsed_data['colunas_nao_utilizadas'])} unused columns)")
                else:
                    print(f"   ⚠️  Skipped {md_file.name}: missing mapa or tabela")
            except Exception as e:
                print(f"   ❌ Error processing {md_file.name}: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        # Save JSON file in output directory
        portal_system_dir = self.output_path / "portal_system"
        portal_system_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = portal_system_dir / "portal_system.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(system_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n✅ Portal system JSON saved to: {output_file}")
        print(f"   Total tables: {len(system_data)}")
        print(f"   Total unused columns: {sum(len(item['colunas_nao_utilizadas']) for item in system_data)}")
        
        return system_data

