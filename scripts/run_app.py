#!/usr/bin/env python3
"""
Script principal para gerar metadados ETL, S3 e comparações
"""

import sys
import yaml
from pathlib import Path

# Add project path
BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))

from domains.consultor_turbo.controller import ConsultorTurboController
from domains.portal_turbo.controller import PortalTurboController
from domains.chama.controller import ChamaController
from domains.consultor_turbo.services.s3.s3_metadata_service import ConsultorTurboS3MetadataService
from domains.portal_turbo.services.s3.s3_metadata_service import PortalTurboS3MetadataService
from domains.chama.services.s3.s3_metadata_service import ChamaS3MetadataService
from domains.consultor_turbo.services.s3.s3_comparator_service import ConsultorTurboS3ComparatorService
from domains.portal_turbo.services.s3.s3_comparator_service import PortalTurboS3ComparatorService
from domains.chama.services.s3.s3_comparator_service import ChamaS3ComparatorService
from domains.consultor_turbo.services.system.etl_system_comparator_service import ConsultorTurboETLSystemComparatorService
from domains.portal_turbo.services.system.etl_system_comparator_service import PortalTurboETLSystemComparatorService
from domains.chama.services.system.etl_system_comparator_service import ChamaETLSystemComparatorService
from domains.consultor_turbo.services.consolidated.consolidated_metadata_service import ConsultorTurboConsolidatedMetadataService
from domains.portal_turbo.services.consolidated.consolidated_metadata_service import PortalTurboConsolidatedMetadataService
from domains.chama.services.consolidated.consolidated_metadata_service import ChamaConsolidatedMetadataService

# Try to import Confluence integration (optional)
try:
    from domains.confluence_integration.controller import ConfluenceIntegrationController
    from domains.confluence_integration.services import (
        ConsultorTurboJsonPublicationService,
        PortalTurboJsonPublicationService,
        ChamaJsonPublicationService
    )
    CONFLUENCE_AVAILABLE = True
    CHAMA_CONFLUENCE_AVAILABLE = True
except ImportError:
    CONFLUENCE_AVAILABLE = False
    CHAMA_CONFLUENCE_AVAILABLE = False


def load_project_config(project_name: str) -> dict:
    """Load project configuration from projects.yaml"""
    config_file = BASE_DIR / "config" / "projects.yaml"
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config.get('projects', {}).get(project_name, {})


def main():
    """Gera metadados ETL, S3, comparações (S3 vs ETL, ETL vs System), consolida tudo em JSON e publica no Confluence para cada projeto"""
    
    print("=" * 70)
    print("🚀 Gerando Metadados ETL, S3 e Comparações")
    print("=" * 70)
    
    projects = ['consultor_turbo', 'portal_turbo', 'chama']
    
    for project_name in projects:
        print(f"\n{'=' * 70}")
        print(f"📦 Processando: {project_name}")
        print(f"{'=' * 70}")
        
        try:
            # Carregar configuração do projeto
            project_config = load_project_config(project_name)
            config_path = Path(project_config['config_path'])
            output_dir = project_config.get('output_dir', f'output/{project_name}')
            output_path = BASE_DIR / output_dir
            
            print(f"📁 Config path: {config_path}")
            print(f"📁 Output path: {output_path}")
            
            # Inicializar controller
            if project_name == 'consultor_turbo':
                controller = ConsultorTurboController(config_path, output_path)
            elif project_name == 'portal_turbo':
                controller = PortalTurboController(config_path, output_path)
            elif project_name == 'chama':
                controller = ChamaController(config_path, output_path)
            else:
                print(f"❌ Unknown project: {project_name}")
                continue
            
            # ========== ETAPA 1: Gerar Metadados ETL ==========
            print(f"\n{'─' * 70}")
            print(f"📝 ETAPA 1: Gerando Metadados ETL")
            print(f"{'─' * 70}")
            
            # Carregar configurações ETL
            print(f"📖 Carregando configurações ETL...")
            configs_raw = controller.config_loader.load_all()
            
            if not configs_raw:
                print(f"❌ Nenhuma configuração encontrada!")
                continue
            
            print(f"✅ {len(configs_raw)} configurações encontradas")
            
            # Extrair informações
            print(f"🔍 Extraindo informações...")
            configs_info = [
                controller.config_loader.extract_config_info(config)
                for config in configs_raw
            ]
            
            # Gerar JSON de metadados ETL
            print(f"📝 Gerando JSON de metadados ETL...")
            controller.etl_metadata_service.generate_etl_metadata_json(configs_info)
            
            # Mostrar onde foi salvo
            if project_name == 'consultor_turbo':
                etl_json_file = output_path / "etl_metadata" / "consultor_etl_metadata.json"
            elif project_name == 'portal_turbo':
                etl_json_file = output_path / "etl_metadata" / "portal_etl_metadata.json"
            elif project_name == 'chama':
                etl_json_file = output_path / "etl_metadata" / "chama_etl_metadata.json"
            else:
                etl_json_file = output_path / "etl_metadata" / "etl_metadata.json"
            
            print(f"✅ JSON ETL gerado: {etl_json_file}")
            
            # ========== ETAPA 2: Gerar Metadados S3 ==========
            print(f"\n{'─' * 70}")
            print(f"📝 ETAPA 2: Gerando Metadados S3")
            print(f"{'─' * 70}")
            
            # Obter caminho S3
            s3_config = project_config.get('s3_comparison', {})
            s3_path = Path(s3_config.get('s3_path', ''))
            if not s3_path.is_absolute():
                s3_path = BASE_DIR / s3_path
            
            print(f"📁 S3 path: {s3_path}")
            
            if not s3_path.exists():
                print(f"⚠️  Caminho S3 não encontrado: {s3_path}")
                print(f"⏭️  Pulando geração de metadados S3...")
            else:
                # Inicializar serviço S3
                if project_name == 'consultor_turbo':
                    s3_metadata_service = ConsultorTurboS3MetadataService(
                        s3_path=s3_path,
                        output_path=output_path
                    )
                elif project_name == 'portal_turbo':
                    s3_metadata_service = PortalTurboS3MetadataService(
                        s3_path=s3_path,
                        output_path=output_path
                    )
                elif project_name == 'chama':
                    s3_metadata_service = ChamaS3MetadataService(
                        s3_path=s3_path,
                        output_path=output_path
                    )
                else:
                    print(f"❌ Unknown project for S3 service: {project_name}")
                    continue
                
                # Gerar JSON de metadados S3
                print(f"📝 Gerando JSON de metadados S3...")
                metadata = s3_metadata_service.generate_from_santander_metadata()
                
                # Mostrar onde foi salvo
                s3_json_file = output_path / "from_santander_metadata" / "from_santander_metadata.json"
                print(f"✅ JSON S3 gerado: {s3_json_file}")
                print(f"📊 Total de arquivos S3 processados: {metadata.get('total_files', 0)}")
            
            # ========== ETAPA 3: Comparar Metadados S3 vs ETL ==========
            print(f"\n{'─' * 70}")
            print(f"📝 ETAPA 3: Comparando Metadados S3 vs ETL")
            print(f"{'─' * 70}")
            
            # Definir caminhos dos JSONs
            if project_name == 'consultor_turbo':
                etl_metadata_path = output_path / "etl_metadata" / "consultor_etl_metadata.json"
            elif project_name == 'portal_turbo':
                etl_metadata_path = output_path / "etl_metadata" / "portal_etl_metadata.json"
            elif project_name == 'chama':
                etl_metadata_path = output_path / "etl_metadata" / "chama_etl_metadata.json"
            else:
                etl_metadata_path = output_path / "etl_metadata" / "etl_metadata.json"
            
            s3_metadata_path = output_path / "from_santander_metadata" / "from_santander_metadata.json"
            
            # Verificar se os JSONs existem
            if not etl_metadata_path.exists():
                print(f"⚠️  JSON de metadados ETL não encontrado: {etl_metadata_path}")
                print(f"⏭️  Pulando comparação...")
            elif not s3_metadata_path.exists():
                print(f"⚠️  JSON de metadados S3 não encontrado: {s3_metadata_path}")
                print(f"⏭️  Pulando comparação...")
            else:
                # Inicializar serviço de comparação
                if project_name == 'consultor_turbo':
                    comparator = ConsultorTurboS3ComparatorService(
                        output_path=output_path,
                        etl_metadata_path=etl_metadata_path,
                        s3_metadata_path=s3_metadata_path
                    )
                elif project_name == 'portal_turbo':
                    comparator = PortalTurboS3ComparatorService(
                        output_path=output_path,
                        etl_metadata_path=etl_metadata_path,
                        s3_metadata_path=s3_metadata_path
                    )
                elif project_name == 'chama':
                    comparator = ChamaS3ComparatorService(
                        output_path=output_path,
                        etl_metadata_path=etl_metadata_path,
                        s3_metadata_path=s3_metadata_path
                    )
                else:
                    print(f"❌ Unknown project for S3 comparator: {project_name}")
                    continue
                
                # Executar comparação
                print(f"📝 Executando comparação...")
                comparator.compare()
                
                # Mostrar onde foi salvo
                comparison_file = output_path / "s3_vs_etl" / "s3_vs_etl_metadata.json"
                print(f"✅ JSON de comparação gerado: {comparison_file}")
            
            # ========== ETAPA 4: Comparar Metadados ETL vs System ==========
            print(f"\n{'─' * 70}")
            print(f"📝 ETAPA 4: Comparando Metadados ETL vs System")
            print(f"{'─' * 70}")
            
            # Definir caminhos dos JSONs
            if project_name == 'consultor_turbo':
                etl_metadata_path = output_path / "etl_metadata" / "consultor_etl_metadata.json"
                system_metadata_path = BASE_DIR / "domains" / "consultor_turbo" / "consultor_system" / "consultor_system.json"
            elif project_name == 'portal_turbo':
                etl_metadata_path = output_path / "etl_metadata" / "portal_etl_metadata.json"
                system_metadata_path = output_path / "portal_system" / "portal_system.json"
            elif project_name == 'chama':
                etl_metadata_path = output_path / "etl_metadata" / "chama_etl_metadata.json"
                system_metadata_path = BASE_DIR / "domains" / "chama" / "chama_system" / "chama_system.json"
            else:
                etl_metadata_path = output_path / "etl_metadata" / "etl_metadata.json"
                system_metadata_path = None
            
            # Verificar se os JSONs existem
            if not etl_metadata_path.exists():
                print(f"⚠️  JSON de metadados ETL não encontrado: {etl_metadata_path}")
                print(f"⏭️  Pulando comparação...")
            elif not system_metadata_path.exists():
                print(f"⚠️  JSON de metadados System não encontrado: {system_metadata_path}")
                print(f"⏭️  Pulando comparação...")
            else:
                # Inicializar serviço de comparação
                if project_name == 'consultor_turbo':
                    system_comparator = ConsultorTurboETLSystemComparatorService(
                        output_path=output_path,
                        etl_metadata_path=etl_metadata_path,
                        system_metadata_path=system_metadata_path
                    )
                elif project_name == 'portal_turbo':
                    system_comparator = PortalTurboETLSystemComparatorService(
                        output_path=output_path,
                        etl_metadata_path=etl_metadata_path,
                        system_metadata_path=system_metadata_path
                    )
                elif project_name == 'chama':
                    system_comparator = ChamaETLSystemComparatorService(
                        output_path=output_path,
                        etl_metadata_path=etl_metadata_path,
                        system_metadata_path=system_metadata_path
                    )
                else:
                    print(f"❌ Unknown project for system comparator: {project_name}")
                    continue
                
                # Executar comparação
                print(f"📝 Executando comparação...")
                system_comparator.compare()
                
                # Mostrar onde foi salvo
                system_comparison_file = output_path / "etl_vs_system" / "etl_vs_system_metadata.json"
                print(f"✅ JSON de comparação gerado: {system_comparison_file}")
            
            # ========== ETAPA 5: Consolidar Metadados ==========
            print(f"\n{'─' * 70}")
            print(f"📝 ETAPA 5: Consolidando Metadados")
            print(f"{'─' * 70}")
            
            # Inicializar serviço de consolidação
            if project_name == 'consultor_turbo':
                consolidator = ConsultorTurboConsolidatedMetadataService(
                    base_dir=BASE_DIR,
                    output_path=output_path
                )
            elif project_name == 'portal_turbo':
                consolidator = PortalTurboConsolidatedMetadataService(
                    base_dir=BASE_DIR,
                    output_path=output_path
                )
            elif project_name == 'chama':
                consolidator = ChamaConsolidatedMetadataService(
                    base_dir=BASE_DIR,
                    output_path=output_path
                )
            else:
                print(f"❌ Unknown project for consolidator: {project_name}")
                continue
            
            # Executar consolidação
            print(f"📝 Consolidando todas as informações...")
            consolidator.consolidate()
            
            # Mostrar onde foi salvo
            consolidated_file = output_path / "consolidated_metadata" / "consolidated_metadata.json"
            print(f"✅ JSON consolidado gerado: {consolidated_file}")
            
            # ========== ETAPA 6: Publicar no Confluence ==========
            print(f"\n{'─' * 70}")
            print(f"📝 ETAPA 6: Publicando no Confluence")
            print(f"{'─' * 70}")
            
            if not CONFLUENCE_AVAILABLE:
                print(f"⚠️  Integração com Confluence não disponível")
                print(f"⏭️  Pulando publicação no Confluence...")
            elif not consolidated_file.exists():
                print(f"⚠️  JSON consolidado não encontrado: {consolidated_file}")
                print(f"⏭️  Pulando publicação no Confluence...")
            else:
                try:
                    # Inicializar controller do Confluence
                    confluence_controller = ConfluenceIntegrationController()
                    
                    if not confluence_controller.client:
                        print(f"⚠️  Cliente Confluence não inicializado")
                        print(f"💡 Configure CONFLUENCE_API_TOKEN no arquivo .env ou variável de ambiente")
                        print(f"⏭️  Pulando publicação no Confluence...")
                    else:
                        # Inicializar serviço de publicação JSON
                        if project_name == 'consultor_turbo':
                            json_publication_service = ConsultorTurboJsonPublicationService(
                                client=confluence_controller.client
                            )
                        elif project_name == 'portal_turbo':
                            json_publication_service = PortalTurboJsonPublicationService(
                                client=confluence_controller.client
                            )
                        elif project_name == 'chama':
                            if CHAMA_CONFLUENCE_AVAILABLE:
                                json_publication_service = ChamaJsonPublicationService(
                                    client=confluence_controller.client
                                )
                            else:
                                print(f"⚠️  ChamaJsonPublicationService não disponível")
                                print(f"⏭️  Pulando publicação no Confluence para chama...")
                                continue
                        else:
                            print(f"❌ Unknown project for Confluence publication: {project_name}")
                            continue
                        
                        # Publicar JSON no Confluence
                        print(f"📤 Publicando JSON consolidado no Confluence...")
                        results = json_publication_service.publish_from_json(
                            json_path=consolidated_file
                        )
                        
                        # Mostrar resultados
                        if results.get('success'):
                            print(f"✅ Publicação no Confluence concluída com sucesso!")
                            print(f"   Página pai: {results.get('parent_page_title')}")
                            successful_pages = [p for p in results.get('pages', []) if p.get('success')]
                            total_tables = len(results.get('pages', []))
                            print(f"   Páginas publicadas: {len(successful_pages)}/{total_tables}")
                            
                            if results.get('errors'):
                                print(f"   ⚠️  Erros: {len(results.get('errors', []))}")
                                for error in results.get('errors', []):
                                    print(f"      - {error.get('table')} ({error.get('config_name')}): {error.get('error')}")
                        else:
                            print(f"⚠️  Publicação no Confluence falhou: {results.get('message', 'Erro desconhecido')}")
                            if results.get('errors'):
                                print(f"   ⚠️  Erros:")
                                for error in results.get('errors', []):
                                    print(f"      - {error.get('table')} ({error.get('config_name')}): {error.get('error')}")
                except Exception as e:
                    print(f"⚠️  Erro ao publicar no Confluence: {e}")
                    print(f"⏭️  Continuando...")
            
        except Exception as e:
            print(f"❌ Erro ao processar {project_name}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    print(f"\n{'=' * 70}")
    print("✅ Concluído!")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    sys.exit(main())

