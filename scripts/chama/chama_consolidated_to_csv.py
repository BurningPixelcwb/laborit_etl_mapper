#!/usr/bin/env python3
"""
Script para converter consolidated_metadata.json do Chama para CSV
"""

import sys
import json
import csv
from pathlib import Path

# Add project path
BASE_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(BASE_DIR))


def normalize_value(value):
    """Normalize value for CSV output"""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, dict)):
        return str(value)
    return str(value)


def main():
    """Converte consolidated_metadata.json do Chama para CSV"""
    
    # Caminhos
    json_path = BASE_DIR / "output" / "chama" / "consolidated_metadata" / "consolidated_metadata.json"
    output_dir = BASE_DIR / "output" / "chama" / "consolidated_csv"
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "chama_consolidated_metadata.csv"
    
    print("=" * 70)
    print("🔄 Convertendo consolidated_metadata.json para CSV")
    print("=" * 70)
    print(f"📄 JSON: {json_path}")
    print(f"💾 CSV: {csv_path}")
    print()
    
    # Verificar se o arquivo JSON existe
    if not json_path.exists():
        print(f"❌ Erro: Arquivo JSON não encontrado: {json_path}")
        sys.exit(1)
    
    # Carregar JSON
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
    except Exception as e:
        print(f"❌ Erro ao carregar JSON: {str(e)}")
        sys.exit(1)
    
    # Extrair dados
    files = json_data.get('files', [])
    
    if not files:
        print("❌ Erro: Nenhum arquivo encontrado no JSON")
        sys.exit(1)
    
    print(f"📊 Processando {len(files)} arquivos...")
    
    # Preparar linhas do CSV
    rows = []
    
    for file_data in files:
        file_name = file_data.get('name', '')
        mappings = file_data.get('mappings', [])
        
        for mapping in mappings:
            map_name = mapping.get('map', '')
            table = mapping.get('table', '')
            
            # Tratar statistics
            statistics = mapping.get('statistics') or {}
            total_fields = statistics.get('total_fields', 0) if isinstance(statistics, dict) else 0
            exists_in_s3_count = statistics.get('exists_in_s3_count', 0) if isinstance(statistics, dict) else 0
            used_in_system_count = statistics.get('used_in_system_count', 0) if isinstance(statistics, dict) else 0
            unused_in_system_count = statistics.get('unused_in_system_count', 0) if isinstance(statistics, dict) else 0
            
            # Tratar s3_comparison
            s3_comparison = mapping.get('s3_comparison') or {}
            has_s3_file = s3_comparison.get('has_s3_file', False) if isinstance(s3_comparison, dict) else False
            fields_in_both_count = s3_comparison.get('fields_in_both_count', 0) if isinstance(s3_comparison, dict) else 0
            fields_only_in_s3_count = s3_comparison.get('fields_only_in_s3_count', 0) if isinstance(s3_comparison, dict) else 0
            fields_only_in_etl_count = s3_comparison.get('fields_only_in_etl_count', 0) if isinstance(s3_comparison, dict) else 0
            
            # Tratar system_comparison
            system_comparison = mapping.get('system_comparison') or {}
            has_system_data = system_comparison.get('has_system_data', False) if isinstance(system_comparison, dict) else False
            system_unused_columns_count = system_comparison.get('system_unused_columns_count', 0) if isinstance(system_comparison, dict) else 0
            
            fields = mapping.get('fields', [])
            for field in fields:
                from_santander = field.get('from_santander', '')
                laborit = field.get('laborit', '')
                field_type = field.get('type', '')
                exists_in_s3 = field.get('exists_in_s3', False)
                is_used_in_system = field.get('is_used_in_system', False)
                
                row = {
                    'file_name': file_name,
                    'map': map_name,
                    'table': table,
                    'from_santander': from_santander,
                    'laborit': laborit,
                    'type': field_type,
                    'exists_in_s3': exists_in_s3,
                    'is_used_in_system': is_used_in_system,
                    'total_fields': total_fields,
                    'exists_in_s3_count': exists_in_s3_count,
                    'used_in_system_count': used_in_system_count,
                    'unused_in_system_count': unused_in_system_count,
                    'has_s3_file': has_s3_file,
                    'fields_in_both_count': fields_in_both_count,
                    'fields_only_in_s3_count': fields_only_in_s3_count,
                    'fields_only_in_etl_count': fields_only_in_etl_count,
                    'has_system_data': has_system_data,
                    'system_unused_columns_count': system_unused_columns_count
                }
                rows.append(row)
    
    if not rows:
        print("❌ Erro: Nenhuma linha gerada para o CSV")
        sys.exit(1)
    
    # Obter todas as chaves únicas para criar o cabeçalho
    all_keys = set()
    for row in rows:
        all_keys.update(row.keys())
    
    # Ordenar as chaves para consistência
    fieldnames = sorted(all_keys)
    
    # Escrever CSV
    try:
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=',')
            writer.writeheader()
            
            for row in rows:
                # Normalizar todos os valores
                normalized_row = {key: normalize_value(row.get(key, '')) for key in fieldnames}
                writer.writerow(normalized_row)
        
        print(f"✅ CSV criado com sucesso!")
        print(f"📊 Total de linhas: {len(rows)}")
        print(f"💾 Arquivo: {csv_path}")
        print()
        print("=" * 70)
        
    except Exception as e:
        print(f"❌ Erro ao escrever CSV: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()


