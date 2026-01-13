#!/usr/bin/env python3
"""
Script para gerar apenas os metadados S3 do Consultor Turbo
"""

import sys
import yaml
from pathlib import Path

# Add project path
BASE_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from domains.consultor_turbo.controller import ConsultorTurboController
from domains.consultor_turbo.services.s3.s3_metadata_service import ConsultorTurboS3MetadataService


def load_project_config(project_name: str) -> dict:
    """Load project configuration from projects.yaml"""
    config_file = BASE_DIR / "config" / "projects.yaml"
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config.get('projects', {}).get(project_name, {})


def main():
    """Gera apenas os metadados S3 em JSON para Consultor Turbo"""
    
    project_name = 'consultor_turbo'
    
    print("=" * 70)
    print("🚀 Gerando Metadados S3 - Consultor Turbo")
    print("=" * 70)
    
    try:
        # Carregar configuração do projeto
        project_config = load_project_config(project_name)
        config_path = Path(project_config['config_path'])
        output_dir = project_config.get('output_dir', f'output/{project_name}')
        output_path = BASE_DIR / output_dir
        
        # Obter caminho S3
        s3_config = project_config.get('s3_comparison', {})
        s3_path = Path(s3_config.get('s3_path', ''))
        if not s3_path.is_absolute():
            s3_path = BASE_DIR / s3_path
        
        print(f"📁 S3 path: {s3_path}")
        print(f"📁 Output path: {output_path}")
        
        if not s3_path.exists():
            print(f"❌ Caminho S3 não encontrado: {s3_path}")
            return 1
        
        # Inicializar serviço S3
        s3_metadata_service = ConsultorTurboS3MetadataService(
            s3_path=s3_path,
            output_path=output_path
        )
        
        # Gerar apenas o JSON de metadados S3
        print(f"\n📝 Gerando JSON de metadados S3...")
        metadata = s3_metadata_service.generate_from_santander_metadata()
        
        # Mostrar onde foi salvo
        json_file = output_path / "from_santander_metadata" / "from_santander_metadata.json"
        print(f"\n✅ JSON gerado: {json_file}")
        print(f"📊 Total de arquivos processados: {metadata.get('total_files', 0)}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Erro ao processar: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

