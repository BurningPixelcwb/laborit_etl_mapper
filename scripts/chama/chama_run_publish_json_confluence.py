#!/usr/bin/env python3
"""
Script para publicar metadados consolidados JSON do Chama diretamente no Confluence
"""

import sys
import yaml
from pathlib import Path

# Add project path
BASE_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

try:
    from domains.confluence_integration import ConfluenceIntegrationController
    from domains.confluence_integration.services import ChamaJsonPublicationService
    CONFLUENCE_AVAILABLE = True
except ImportError as e:
    CONFLUENCE_AVAILABLE = False
    IMPORT_ERROR = str(e)


def load_project_config(project_name: str) -> dict:
    """Load project configuration from projects.yaml"""
    config_file = BASE_DIR / "config" / "projects.yaml"
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config.get('projects', {}).get(project_name, {})


def main():
    """Publica metadados consolidados JSON do Chama diretamente no Confluence"""
    
    project_name = 'chama'
    
    print("=" * 70)
    print("🚀 Publicando Metadados Consolidados JSON no Confluence - Chama")
    print("=" * 70)
    
    if not CONFLUENCE_AVAILABLE:
        print("❌ Confluence integration not available.")
        print(f"💡 Error: {IMPORT_ERROR}")
        print("💡 Check if domains.confluence_integration is properly installed.")
        return 1
    
    try:
        # Carregar configuração do projeto
        project_config = load_project_config(project_name)
        output_dir = project_config.get('output_dir', f'output/{project_name}')
        output_path = BASE_DIR / output_dir
        
        # Definir caminho do JSON consolidado
        consolidated_json_path = output_path / "consolidated_metadata" / "consolidated_metadata.json"
        
        print(f"📁 Output path: {output_path}")
        print(f"📄 Consolidated JSON path: {consolidated_json_path}")
        
        # Verificar se o JSON consolidado existe
        if not consolidated_json_path.exists():
            print(f"❌ JSON consolidado não encontrado: {consolidated_json_path}")
            print(f"💡 Execute primeiro: python3 scripts/chama/chama_run_consolidated_metadata.py")
            return 1
        
        print(f"✅ JSON consolidado encontrado")
        
        # Inicializar controller do Confluence para obter o client
        confluence_controller = ConfluenceIntegrationController()
        
        if not confluence_controller.client:
            print(f"❌ Confluence client não inicializado.")
            print(f"💡 Configure CONFLUENCE_API_TOKEN no arquivo .env ou variável de ambiente")
            return 1
        
        # Inicializar serviço de publicação JSON
        json_publication_service = ChamaJsonPublicationService(
            client=confluence_controller.client
        )
        
        # Publicar JSON diretamente no Confluence
        print(f"\n📤 Publicando JSON consolidado no Confluence...")
        results = json_publication_service.publish_from_json(
            json_path=consolidated_json_path
        )
        
        # Mostrar resultados
        if results.get('success'):
            print(f"\n✅ Publicação concluída com sucesso!")
            print(f"   Página pai: {results.get('parent_page_title')}")
            successful_pages = [p for p in results.get('pages', []) if p.get('success')]
            total_tables = len(results.get('pages', []))
            print(f"   Páginas publicadas: {len(successful_pages)}/{total_tables}")
            
            if results.get('errors'):
                print(f"   ⚠️  Erros: {len(results.get('errors', []))}")
                for error in results.get('errors', []):
                    print(f"      - {error.get('table')} ({error.get('config_name')}): {error.get('error')}")
        else:
            print(f"\n❌ Publicação falhou: {results.get('message', 'Erro desconhecido')}")
            if results.get('errors'):
                print(f"   ⚠️  Erros:")
                for error in results.get('errors', []):
                    print(f"      - {error.get('table')} ({error.get('config_name')}): {error.get('error')}")
            return 1
        
        return 0
        
    except Exception as e:
        print(f"❌ Erro ao processar: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())



