#!/usr/bin/env python3
"""
Script para converter consolidated_metadata.json do Portal Turbo para CSV
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
    """Converte consolidated_metadata.json do Portal Turbo para CSV"""
    
    # Caminhos
    json_path = BASE_DIR / "output" / "portal_turbo" / "consolidated_metadata" / "consolidated_metadata.json"
    output_dir = BASE_DIR / "output" / "portal_turbo" / "consolidated_csv"
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "portal_turbo_consolidated_metadata.csv"
    
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
    tables = json_data.get('tables', [])
    
    if not tables:
        print("❌ Erro: Nenhuma tabela encontrada no JSON")
        sys.exit(1)
    
    print(f"📊 Processando {len(tables)} tabelas...")
    
    # Preparar linhas do CSV
    rows = []
    
    for table in tables:
        config_name = table.get('config_name', '')
        config_file = table.get('config_file', '')
        final_table = table.get('final_table', '')
        temp_table = table.get('temp_table', '')
        temp_db = table.get('temp_db', '')
        version = table.get('version', '')
        keys = ', '.join(table.get('keys', []))
        update_fields = ', '.join(table.get('update_fields', []))
        period_column = table.get('period_column', '')
        
        # Tratar soft_delete
        soft_delete = table.get('soft_delete') or {}
        soft_delete_enabled = soft_delete.get('enabled', False) if isinstance(soft_delete, dict) else False
        soft_delete_fields = ', '.join(soft_delete.get('fields', [])) if isinstance(soft_delete, dict) else ''
        
        columns_count = table.get('columns_count', 0)
        
        columns = table.get('columns', [])
        for column in columns:
            from_santander = column.get('from_santander', '')
            type_santander = column.get('type_santander', '')
            from_etl = column.get('from_etl', '')
            type_etl = column.get('type_etl', '')
            is_key = column.get('is_key', False)
            is_used_in_system = column.get('is_used_in_system', False)
            
            # Tratar s3_info
            s3_info = column.get('s3_info') or {}
            exists_in_s3 = s3_info.get('exists_in_s3', False) if isinstance(s3_info, dict) else False
            s3_name = s3_info.get('s3_name', '') if isinstance(s3_info, dict) else ''
            
            row = {
                'config_name': config_name,
                'config_file': config_file,
                'final_table': final_table,
                'temp_table': temp_table,
                'temp_db': temp_db,
                'version': version,
                'keys': keys,
                'update_fields': update_fields,
                'period_column': period_column,
                'soft_delete_enabled': soft_delete_enabled,
                'soft_delete_fields': soft_delete_fields,
                'columns_count': columns_count,
                'from_santander': from_santander,
                'type_santander': type_santander,
                'from_etl': from_etl,
                'type_etl': type_etl,
                'is_key': is_key,
                'is_used_in_system': is_used_in_system,
                'exists_in_s3': exists_in_s3,
                's3_name': s3_name
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


