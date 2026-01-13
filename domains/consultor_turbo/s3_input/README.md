# Pasta para arquivos CSV do S3 - Consultor Turbo

Coloque aqui os arquivos CSV baixados do S3 que você quer comparar com os arquivos do ETL.

## Como usar:

1. Baixe os arquivos CSV do S3
2. Coloque-os nesta pasta (`domains/consultor_turbo/s3_input/`)
3. Execute: `python scripts/compare_s3_etl.py --project consultor_turbo`

## Formato esperado:

- Arquivos CSV com cabeçalho na primeira linha
- Delimitador pode ser vírgula (`,`) ou ponto e vírgula (`;`)
- O script detecta automaticamente o delimitador

## Nomenclatura:

Os arquivos devem ter nomes que correspondam aos arquivos do ETL (sem a data/hora).
Por exemplo:
- S3: `tabela_20240115_1430.csv`
- ETL: `tabela.csv`
- O script vai fazer o match removendo `_yyyymmdd_hhmm`






