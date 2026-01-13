#!/usr/bin/env python3
"""
Script para consolidar todos os metadados (ETL, S3, System) do Chama em um único JSON
"""

import sys
import yaml
from pathlib import Path

# Add project path
BASE_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from domains.chama.services.consolidated.consolidated_metadata_service import ChamaConsolidatedMetadataService


def load_project_config(project_name: str) -> dict:
    """Load project configuration from projects.yaml"""
    config_file = BASE_DIR / "config" / "projects.yaml"
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config.get('projects', {}).get(project_name, {})


def main():
    """Consolida todos os metadados (ETL, S3, System) do Chama em um único JSON"""
    
    project_name = 'chama'
    
    print("=" * 70)
    print("🚀 Consolidando Metadados - Chama")
    print("=" * 70)
    
    try:
        # Carregar configuração do projeto
        project_config = load_project_config(project_name)
        output_dir = project_config.get('output_dir', f'output/{project_name}')
        output_path = BASE_DIR / output_dir
        
        print(f"📁 Output path: {output_path}")
        
        # Verificar se os JSONs necessários existem
        etl_metadata_path = output_path / "etl_metadata" / "chama_etl_metadata.json"
        s3_metadata_path = output_path / "from_santander_metadata" / "from_santander_metadata.json"
        s3_comparison_path = output_path / "s3_vs_etl" / "s3_vs_etl_metadata.json"
        system_comparison_path = output_path / "etl_vs_system" / "etl_vs_system_metadata.json"
        
        missing_files = []
        if not etl_metadata_path.exists():
            missing_files.append(f"ETL: {etl_metadata_path}")
        if not s3_metadata_path.exists():
            missing_files.append(f"S3: {s3_metadata_path}")
        if not s3_comparison_path.exists():
            missing_files.append(f"S3 vs ETL: {s3_comparison_path}")
        if not system_comparison_path.exists():
            missing_files.append(f"ETL vs System: {system_comparison_path}")
        
        if missing_files:
            print(f"\n⚠️  Alguns arquivos não foram encontrados:")
            for missing in missing_files:
                print(f"   - {missing}")
            print(f"\n💡 Execute as etapas anteriores primeiro:")
            print(f"   1. python3 scripts/chama/chama_run_etl_metadata.py")
            print(f"   2. python3 scripts/chama/chama_run_s3_metadata.py")
            print(f"   3. python3 scripts/chama/chama_run_s3_etl_comparison.py")
            print(f"   4. python3 scripts/chama/chama_run_etl_system_comparison.py")
            if not etl_metadata_path.exists():
                return 1
        
        # Inicializar serviço de consolidação
        consolidator = ChamaConsolidatedMetadataService(
            base_dir=BASE_DIR,
            output_path=output_path
        )
        
        # Executar consolidação
        print(f"\n📝 Consolidando todas as informações...")
        consolidator.consolidate()
        
        # Mostrar onde foi salvo
        consolidated_file = output_path / "consolidated_metadata" / "consolidated_metadata.json"
        print(f"\n✅ JSON consolidado gerado: {consolidated_file}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Erro ao processar: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

