#!/usr/bin/env python3
"""
Script para consolidar todos os metadados (ETL, S3, System) do Consultor Turbo em um único JSON
"""

import sys
import yaml
from pathlib import Path

# Add project path
BASE_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from domains.consultor_turbo.services.consolidated.consolidated_metadata_service import ConsultorTurboConsolidatedMetadataService


def load_project_config(project_name: str) -> dict:
    """Load project configuration from projects.yaml"""
    config_file = BASE_DIR / "config" / "projects.yaml"
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config.get('projects', {}).get(project_name, {})


def main():
    """Consolida todos os metadados (ETL, S3, System) do Consultor Turbo em um único JSON"""
    
    project_name = 'consultor_turbo'
    
    print("=" * 70)
    print("🚀 Consolidando Metadados - Consultor Turbo")
    print("=" * 70)
    
    try:
        # Carregar configuração do projeto
        project_config = load_project_config(project_name)
        output_dir = project_config.get('output_dir', f'output/{project_name}')
        output_path = BASE_DIR / output_dir
        
        print(f"📁 Output path: {output_path}")
        
        # Verificar se os arquivos necessários existem
        etl_metadata_path = BASE_DIR / "output" / "consultor_turbo" / "etl_metadata" / "consultor_etl_metadata.json"
        s3_metadata_path = BASE_DIR / "output" / "consultor_turbo" / "from_santander_metadata" / "from_santander_metadata.json"
        s3_comparison_path = BASE_DIR / "output" / "consultor_turbo" / "s3_vs_etl" / "s3_vs_etl_metadata.json"
        system_comparison_path = BASE_DIR / "output" / "consultor_turbo" / "etl_vs_system" / "etl_vs_system_metadata.json"
        
        print(f"\n📋 Verificando arquivos necessários...")
        missing_files = []
        
        if not etl_metadata_path.exists():
            missing_files.append(f"ETL metadata: {etl_metadata_path}")
        else:
            print(f"   ✅ ETL metadata encontrado")
        
        if not s3_metadata_path.exists():
            missing_files.append(f"S3 metadata: {s3_metadata_path}")
        else:
            print(f"   ✅ S3 metadata encontrado")
        
        if not s3_comparison_path.exists():
            missing_files.append(f"S3 comparison: {s3_comparison_path}")
        else:
            print(f"   ✅ S3 comparison encontrado")
        
        if not system_comparison_path.exists():
            missing_files.append(f"System comparison: {system_comparison_path}")
        else:
            print(f"   ✅ System comparison encontrado")
        
        if missing_files:
            print(f"\n❌ Arquivos necessários não encontrados:")
            for file in missing_files:
                print(f"   - {file}")
            print(f"\n💡 Execute os scripts de geração de metadados primeiro:")
            print(f"   - python3 scripts/consultor_turbo/consultor_turbo_run_etl_metadata.py")
            print(f"   - python3 scripts/consultor_turbo/consultor_turbo_run_s3_metadata.py")
            print(f"   - python3 scripts/consultor_turbo/consultor_turbo_run_s3_etl_comparison.py")
            print(f"   - python3 scripts/consultor_turbo/consultor_turbo_run_etl_system_comparison.py")
            return 1
        
        # Inicializar serviço de consolidação
        print(f"\n📝 Inicializando serviço de consolidação...")
        consolidator = ConsultorTurboConsolidatedMetadataService(
            base_dir=BASE_DIR,
            output_path=output_path
        )
        
        # Executar consolidação
        print(f"📝 Consolidando todas as informações...")
        consolidator.consolidate()
        
        # Mostrar onde foi salvo
        consolidated_file = output_path / "consolidated_metadata" / "consolidated_metadata.json"
        print(f"\n✅ Metadados consolidados salvos em: {consolidated_file}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Erro ao processar: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

