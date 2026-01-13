#!/usr/bin/env python3
"""
Script para gerar apenas os metadados ETL do Chama
"""

import sys
import yaml
from pathlib import Path

# Add project path
BASE_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from domains.chama.controller import ChamaController


def load_project_config(project_name: str) -> dict:
    """Load project configuration from projects.yaml"""
    config_file = BASE_DIR / "config" / "projects.yaml"
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config.get('projects', {}).get(project_name, {})


def main():
    """Gera apenas os metadados ETL em JSON para Chama"""
    
    project_name = 'chama'
    
    print("=" * 70)
    print("🚀 Gerando Metadados ETL - Chama")
    print("=" * 70)
    
    try:
        # Carregar configuração do projeto
        project_config = load_project_config(project_name)
        config_path = Path(project_config['config_path'])
        output_dir = project_config.get('output_dir', f'output/{project_name}')
        output_path = BASE_DIR / output_dir
        
        print(f"📁 Config path: {config_path}")
        print(f"📁 Output path: {output_path}")
        
        # Inicializar controller
        controller = ChamaController(config_path, output_path)
        
        # Carregar configurações ETL
        print(f"\n📖 Carregando configurações ETL...")
        configs_raw = controller.config_loader.load_all()
        
        if not configs_raw:
            print(f"❌ Nenhuma configuração encontrada!")
            return 1
        
        print(f"✅ {len(configs_raw)} configurações encontradas")
        
        # Extrair informações
        print(f"🔍 Extraindo informações...")
        configs_info = [
            controller.config_loader.extract_config_info(config)
            for config in configs_raw
        ]
        
        # Gerar apenas o JSON de metadados ETL
        print(f"📝 Gerando JSON de metadados ETL...")
        controller.etl_metadata_service.generate_etl_metadata_json(configs_info)
        
        # Mostrar onde foi salvo
        json_file = output_path / "etl_metadata" / "chama_etl_metadata.json"
        print(f"✅ JSON gerado: {json_file}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Erro ao processar: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

