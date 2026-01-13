"""
Service to handle system metadata (chama_system.json) for Chama
"""

import json
from pathlib import Path
from typing import Dict, Any, Tuple


class ChamaFromSystemService:
    """Service to load and process system metadata (chama_system.json)"""
    
    def __init__(self, base_dir: Path, output_path: Path):
        """
        Initialize the service
        
        Args:
            base_dir: Base directory of the project
            output_path: Output directory (not used here, kept for compatibility)
        """
        self.base_dir = Path(base_dir)
        self.output_path = Path(output_path)
    
    def _normalize_name(self, name: str) -> str:
        """Normalize column name for comparison"""
        return name.lower().strip()
    
    def _load_system_metadata(self, system_metadata_path: Path) -> Dict[str, Any]:
        """
        Load and process system metadata (chama_system.json)
        
        Args:
            system_metadata_path: Path to chama_system.json
            
        Returns:
            Dictionary mapping table names to system metadata
        """
        system_metadata = []
        if system_metadata_path.exists():
            with open(system_metadata_path, 'r', encoding='utf-8') as f:
                system_metadata = json.load(f)
        
        # Create mapping for quick lookup by table name
        system_map = {}
        for item in system_metadata:
            tabela = item.get('tabela', '')
            if tabela:
                system_map[tabela] = item
        
        return system_map
    
    def _get_unused_columns(self, system_data: Dict[str, Any]) -> Tuple[set, Dict[str, Any]]:
        """
        Extract unused column names from system data
        
        Args:
            system_data: System metadata for a specific table
            
        Returns:
            Tuple of (unused_columns_set, unused_columns_mapping)
        """
        unused_columns_dict = system_data.get('colunas_nao_utilizadas', {})
        unused_columns_set = set()
        unused_columns_mapping = {}
        
        if isinstance(unused_columns_dict, dict):
            # Dict format: {portuguese_name: english_name}
            for pt_name, en_name in unused_columns_dict.items():
                if en_name is not None:
                    normalized = self._normalize_name(en_name)
                    unused_columns_set.add(normalized)
                    unused_columns_mapping[normalized] = {
                        'portuguese_name': pt_name,
                        'english_name': en_name
                    }
        elif isinstance(unused_columns_dict, list):
            # List format: [english_name1, english_name2, ...]
            for en_name in unused_columns_dict:
                if en_name is not None:
                    unused_columns_set.add(self._normalize_name(en_name))
        
        return unused_columns_set, unused_columns_mapping

