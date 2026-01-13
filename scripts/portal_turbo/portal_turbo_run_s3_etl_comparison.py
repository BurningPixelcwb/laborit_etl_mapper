#!/usr/bin/env python3
"""
Script para comparar metadados S3 vs ETL do Portal Turbo
"""

import sys
import yaml
from pathlib import Path

# Add project path
BASE_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from domains.portal_turbo.services.s3.s3_comparator_service import PortalTurboS3ComparatorService


def load_project_config(project_name: str) -> dict:
    """Load project configuration from projects.yaml"""
    config_file = BASE_DIR / "config" / "projects.yaml"
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config.get('projects', {}).get(project_name, {})


def main():
    """Compara metadados S3 vs ETL para Portal Turbo"""
    
    project_name = 'portal_turbo'
    
    print("=" * 70)
    print("🚀 Comparando Metadados S3 vs ETL - Portal Turbo")
    print("=" * 70)
    
    try:
        # Carregar configuração do projeto
        project_config = load_project_config(project_name)
        output_dir = project_config.get('output_dir', f'output/{project_name}')
        output_path = BASE_DIR / output_dir
        
        # Definir caminhos dos JSONs
        etl_metadata_path = output_path / "etl_metadata" / "portal_etl_metadata.json"
        s3_metadata_path = output_path / "from_santander_metadata" / "from_santander_metadata.json"
        
        print(f"📁 ETL metadata: {etl_metadata_path}")
        print(f"📁 S3 metadata: {s3_metadata_path}")
        print(f"📁 Output path: {output_path}")
        
        # Verificar se os JSONs existem
        if not etl_metadata_path.exists():
            print(f"❌ JSON de metadados ETL não encontrado: {etl_metadata_path}")
            print(f"💡 Execute primeiro: python3 scripts/portal_turbo/portal_turbo_run_etl_metadata.py")
            return 1
        
        if not s3_metadata_path.exists():
            print(f"❌ JSON de metadados S3 não encontrado: {s3_metadata_path}")
            print(f"💡 Execute primeiro: python3 scripts/portal_turbo/portal_turbo_run_s3_metadata.py")
            return 1
        
        # Inicializar serviço de comparação
        comparator = PortalTurboS3ComparatorService(
            output_path=output_path,
            etl_metadata_path=etl_metadata_path,
            s3_metadata_path=s3_metadata_path
        )
        
        # Executar comparação
        print(f"\n📝 Executando comparação...")
        comparator.compare()
        
        # Mostrar onde foi salvo
        comparison_file = output_path / "s3_vs_etl" / "s3_vs_etl_metadata.json"
        print(f"\n✅ JSON de comparação gerado: {comparison_file}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Erro ao processar: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

