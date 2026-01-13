"""
Generic utilities for file manipulation
"""

import json
from pathlib import Path
from typing import Dict, List, Any


def read_json_file(file_path: Path) -> Dict[str, Any]:
    """
    Read a generic JSON file.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        Dictionary with JSON content
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def read_json_files_from_dir(directory: Path) -> List[Dict[str, Any]]:
    """
    Read all JSON files from a directory.
    
    Args:
        directory: Directory containing JSON files
        
    Returns:
        List of dictionaries with JSON contents
    """
    configs = []
    
    if not directory.exists():
        return configs
    
    for json_file in sorted(directory.glob("*.json")):
        try:
            config = read_json_file(json_file)
            config['_filename'] = json_file.name
            config['_filepath'] = str(json_file)
            configs.append(config)
        except Exception as e:
            print(f"⚠️  Error reading {json_file.name}: {e}")
    
    return configs






