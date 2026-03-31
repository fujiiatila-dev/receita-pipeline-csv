"""
Script de ingestão dos CSVs processados no ClickHouse.
Pode ser chamado via BashOperator ou PythonOperator no Airflow.
Uso: python load_to_clickhouse.py <mes_ano> <tipo>
  ex: python load_to_clickhouse.py 2025-05 Empresas
"""
import sys
import os
import pandas as pd

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db_clickhouse import get_clickhouse_client

# Mapeamento tipo → nome da tabela no ClickHouse
TABLE_MAP = {
    "Empresas": "receita.empresas_{periodo}",
    "Estabelecimentos": "receita.estabelecimentos_{periodo}",
    "Socios": "receita.socios_{periodo}",
    "Simples": "receita.simples_{periodo}",
}

BASE_DIR = "/opt/airflow/data"


def load_csv_to_clickhouse(mes_ano, type_name):
    """Carrega o CSV processado no ClickHouse."""
    client = get_clickhouse_client()
    if client is None:
        print("[Load] Sem conexao com ClickHouse. Pulando ingestao.")
        return False

    periodo = mes_ano.replace("-", "")
    csv_path = f"{BASE_DIR}/output/{type_name}_{mes_ano}.csv"

    if not os.path.exists(csv_path):
        print(f"[Load] CSV nao encontrado: {csv_path}")
        return False

    table_template = TABLE_MAP.get(type_name)
    if not table_template:
        print(f"[Load] Tipo desconhecido: {type_name}")
        return False

    table_name = table_template.format(periodo=periodo)

    print(f"[Load] Carregando {csv_path} -> {table_name}")

    df = pd.read_csv(csv_path, sep=";", encoding="ISO-8859-1", dtype=str)
    print(f"[Load] {len(df)} registros lidos do CSV")

    # Drop e recria tabela
    client.command(f"DROP TABLE IF EXISTS {table_name}")

    cols = []
    for col in df.columns:
        cols.append(f"`{col}` Nullable(String)")
    cols_sql = ",\n        ".join(cols)

    ddl = f"""
    CREATE TABLE {table_name}
    (
        {cols_sql}
    )
    ENGINE = MergeTree
    ORDER BY tuple()
    SETTINGS index_granularity = 8192
    """
    client.command(ddl)
    print(f"[Load] Tabela {table_name} criada com {len(df.columns)} colunas")

    # Inserir dados em batches
    columns = list(df.columns)
    df = df.where(pd.notnull(df), None)
    data = df.values.tolist()
    batch_size = 50000

    for i in range(0, len(data), batch_size):
        batch = data[i : i + batch_size]
        client.insert(table_name, batch, column_names=columns)
        print(f"[Load] Inseridos {min(i + batch_size, len(data))}/{len(data)} registros")

    print(f"[Load] Ingestao concluida: {table_name}")
    return True


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: load_to_clickhouse.py <mes_ano> <tipo>")
        print("  ex: load_to_clickhouse.py 2025-05 Empresas")
        sys.exit(1)

    mes_ano = sys.argv[1]
    type_name = sys.argv[2]

    success = load_csv_to_clickhouse(mes_ano, type_name)
    if not success:
        print("[Load] Ingestao nao realizada (sem credenciais ou erro).")
        # Nao falha — graceful degradation
        sys.exit(0)
