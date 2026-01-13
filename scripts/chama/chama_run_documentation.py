#!/usr/bin/env python3
"""
Script para publicar JSON consolidado do Chama no Confluence
"""

import sys
import yaml
from pathlib import Path

# Add project path
BASE_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

# Try to import Confluence integration
try:
    from domains.confluence_integration.controller import ConfluenceIntegrationController
    from domains.confluence_integration.services.chama_json_publication_service import ChamaJsonPublicationService
    CONFLUENCE_AVAILABLE = True
except ImportError:
    CONFLUENCE_AVAILABLE = False
    print("⚠️  Confluence integration not available")


def load_project_config(project_name: str) -> dict:
    """Load project configuration from projects.yaml"""
    config_file = BASE_DIR / "config" / "projects.yaml"
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config.get('projects', {}).get(project_name, {})


def main():
    """Publica JSON consolidado do Chama no Confluence"""
    
    project_name = 'chama'
    
    print("=" * 70)
    print("🚀 Publicando JSON Consolidado no Confluence - Chama")
    print("=" * 70)
    
    if not CONFLUENCE_AVAILABLE:
        print(f"\n❌ Integração com Confluence não disponível")
        print(f"💡 Verifique se o módulo domains.confluence_integration está instalado")
        return 1
    
    try:
        # Carregar configuração do projeto
        project_config = load_project_config(project_name)
        output_dir = project_config.get('output_dir', f'output/{project_name}')
        output_path = BASE_DIR / output_dir
        
        print(f"📁 Output path: {output_path}")
        
        # Verificar se o JSON consolidado existe
        consolidated_file = output_path / "consolidated_metadata" / "consolidated_metadata.json"
        
        if not consolidated_file.exists():
            print(f"\n⚠️  JSON consolidado não encontrado: {consolidated_file}")
            print(f"\n💡 Execute a etapa de consolidação primeiro:")
            print(f"   python3 scripts/chama/chama_run_consolidated_metadata.py")
            return 1
        
        print(f"✅ JSON consolidado encontrado: {consolidated_file}")
        
        # Inicializar controller do Confluence
        print(f"\n🔌 Inicializando cliente Confluence...")
        confluence_controller = ConfluenceIntegrationController()
        
        if not confluence_controller.client:
            print(f"⚠️  Cliente Confluence não inicializado")
            print(f"💡 Configure CONFLUENCE_API_TOKEN no arquivo .env ou variável de ambiente")
            return 1
        
        print(f"✅ Cliente Confluence inicializado")
        
        # Inicializar serviço de publicação JSON
        json_publication_service = ChamaJsonPublicationService(
            client=confluence_controller.client
        )
        
        # Publicar JSON no Confluence
        print(f"\n📤 Publicando JSON consolidado no Confluence...")
        results = json_publication_service.publish_from_json(
            json_path=consolidated_file
        )
        
        # Mostrar resultados
        print(f"\n{'=' * 70}")
        print(f"📊 Resultados da Publicação")
        print(f"{'=' * 70}")
        print(f"✅ Sucesso: {results.get('success', False)}")
        print(f"📄 Página Pai: {results.get('parent_page_title', 'N/A')}")
        print(f"🆔 ID da Página Pai: {results.get('parent_page_id', 'N/A')}")
        
        pages = results.get('pages', [])
        successful_pages = [p for p in pages if p.get('success')]
        failed_pages = [p for p in pages if not p.get('success')]
        
        print(f"\n📈 Estatísticas:")
        print(f"   ✅ Páginas publicadas com sucesso: {len(successful_pages)}")
        print(f"   ❌ Páginas com erro: {len(failed_pages)}")
        print(f"   📝 Total: {len(pages)}")
        
        if failed_pages:
            print(f"\n❌ Erros encontrados:")
            for page in failed_pages[:10]:  # Mostrar apenas os primeiros 10 erros
                print(f"   - {page.get('table', 'unknown')}: {page.get('error', 'Unknown error')}")
            if len(failed_pages) > 10:
                print(f"   ... e mais {len(failed_pages) - 10} erros")
        
        if results.get('message'):
            print(f"\n💬 Mensagem: {results.get('message')}")
        
        return 0 if results.get('success', False) else 1
        
    except Exception as e:
        print(f"❌ Erro ao processar: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

