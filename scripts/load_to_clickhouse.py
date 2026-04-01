"""
Ingestao dos CSVs processados no ClickHouse como tabelas staging.
Aplica saneamento: limpeza de aspas, trim de chaves, ORDER BY cnpj_basico.
Uso: python load_to_clickhouse.py <mes_ano> <tipo>
  ex: python load_to_clickhouse.py 2026-03 Empresas
"""
import sys
import os
import csv
import re

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db_clickhouse import get_clickhouse_client

# Schema de cada tipo de dado
SCHEMAS = {
    "Empresas": [
        "cnpj_basico", "razao_social", "natureza_juridica",
        "qualificacao_responsavel", "capital_social", "porte_empresa",
        "ente_federativo_responsavel",
    ],
    "Estabelecimentos": [
        "cnpj_basico", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial",
        "nome_fantasia", "situacao_cadastral", "data_situacao_cadastral",
        "motivo_situacao_cadastral", "nome_cidade_exterior", "pais",
        "data_inicio_atividade", "cnae_fiscal_principal", "cnae_fiscal_secundaria",
        "tipo_logradouro", "logradouro", "numero", "complemento", "bairro",
        "cep", "uf", "municipio", "ddd_1", "telefone_1", "ddd_2", "telefone_2",
        "ddd_fax", "fax", "correio_eletronico", "situacao_especial",
        "data_situacao_especial",
    ],
    "Socios": [
        "cnpj_basico", "identificador_socio", "nome_socio_razao_social",
        "cpf_cnpj_socio", "qualificacao_socio", "data_entrada_sociedade",
        "pais", "representante_legal", "nome_representante",
        "qualificacao_representante_legal", "faixa_etaria",
    ],
    "Simples": [
        "cnpj_basico", "opcao_simples", "data_opcao_simples",
        "data_exclusao_simples", "opcao_mei", "data_opcao_mei",
        "data_exclusao_mei",
    ],
}

# Chaves de join que precisam de trim rigoroso
JOIN_KEYS = {"cnpj_basico", "cnpj_ordem", "cnpj_dv"}

# Campos de razao social que precisam de limpeza de CPF/CNPJ
RAZAO_SOCIAL_FIELDS = {"razao_social", "nome_socio_razao_social"}

# Campos de data que devem tratar '00000000' e '' como vazio
DATE_FIELDS = {
    "data_situacao_cadastral", "data_inicio_atividade", "data_situacao_especial",
    "data_entrada_sociedade", "data_opcao_simples", "data_exclusao_simples",
    "data_opcao_mei", "data_exclusao_mei",
}

# Regex para limpar CPF/CNPJ do final da razao social
REGEX_DOC_CLEANUP = re.compile(r'[\d./-]+\s*$')

BASE_DIR = "/opt/airflow/data"
DATABASE = "empresas_ativas_do_brasil"


def _clean_field(value, col_name):
    """Aplica saneamento por campo."""
    # 1. Remover aspas duplas e barras invertidas excedentes
    value = value.replace('"', '').replace('\\', '').strip()

    # 2. Trim rigoroso em chaves de join
    if col_name in JOIN_KEYS:
        value = value.strip()

    # 3. Limpeza de razao social: remover CPF/CNPJ concatenado
    if col_name in RAZAO_SOCIAL_FIELDS and value:
        value = REGEX_DOC_CLEANUP.sub('', value).strip()

    # 4. Tratar datas invalidas ('00000000', '', '0') como vazio
    if col_name in DATE_FIELDS:
        if value in ('', '00000000', '0'):
            value = ''

    # 5. Capital social: substituir virgula por ponto
    if col_name == 'capital_social':
        value = value.replace(',', '.')

    # 6. CNAE: garantir apenas numeros
    if col_name in ('cnae_fiscal_principal', 'cnae_fiscal_secundaria') and value:
        value = re.sub(r'[^0-9,]', '', value)

    return value


def load_csv_to_clickhouse(mes_ano, type_name):
    """Carrega o CSV processado no ClickHouse com saneamento."""
    client = get_clickhouse_client()
    if client is None:
        print("[Load] Sem conexao com ClickHouse. Pulando ingestao.")
        return False

    periodo = mes_ano.replace("-", "")
    csv_path = f"{BASE_DIR}/output/{type_name}_{mes_ano}.csv"

    if not os.path.exists(csv_path):
        print(f"[Load] CSV nao encontrado: {csv_path}")
        return False

    columns = SCHEMAS.get(type_name)
    if not columns:
        print(f"[Load] Tipo desconhecido: {type_name}")
        return False

    table_name = f"{DATABASE}.{type_name.lower()}_{periodo}"

    print(f"[Load] Carregando {csv_path} -> {table_name}")

    # Drop e recria tabela com ORDER BY cnpj_basico (Prioridade 2 - otimizacao)
    client.command(f"DROP TABLE IF EXISTS {table_name}")

    cols_sql = ",\n        ".join([f"`{col}` String" for col in columns])
    ddl = f"""
    CREATE TABLE {table_name}
    (
        {cols_sql}
    )
    ENGINE = MergeTree
    ORDER BY (cnpj_basico)
    SETTINGS index_granularity = 8192
    """
    client.command(ddl)
    print(f"[Load] Tabela {table_name} criada (ORDER BY cnpj_basico)")

    # Ler CSV com delimitador ; e inserir em batches com saneamento
    batch_size = 50000
    batch = []
    total = 0

    with open(csv_path, "r", encoding="ISO-8859-1") as f:
        reader = csv.reader(f, delimiter=";")
        next(reader, None)  # skip header

        for row in reader:
            # Garantir numero correto de colunas
            if len(row) < len(columns):
                row.extend([""] * (len(columns) - len(row)))
            elif len(row) > len(columns):
                row = row[: len(columns)]

            # Aplicar saneamento campo a campo
            row = [_clean_field(row[i], columns[i]) for i in range(len(columns))]

            batch.append(row)
            total += 1

            if len(batch) >= batch_size:
                client.insert(table_name, batch, column_names=columns)
                print(f"[Load] Inseridos {total} registros...")
                batch = []

    # Inserir batch restante
    if batch:
        client.insert(table_name, batch, column_names=columns)

    print(f"[Load] Ingestao concluida: {table_name} ({total} registros)")

    # Limpar arquivos locais apos ingestao bem sucedida
    try:
        os.remove(csv_path)
        print(f"[Load] CSV removido: {csv_path}")

        raw_dir = os.path.join(BASE_DIR, "raw", mes_ano)
        if os.path.exists(raw_dir):
            for f in os.listdir(raw_dir):
                if f.startswith(type_name):
                    raw_path = os.path.join(raw_dir, f)
                    os.remove(raw_path)
                    print(f"[Load] Raw removido: {raw_path}")
    except OSError as e:
        print(f"[Load] Aviso ao limpar arquivos: {e}")

    return True


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: load_to_clickhouse.py <mes_ano> <tipo>")
        print("  ex: load_to_clickhouse.py 2026-03 Empresas")
        sys.exit(1)

    mes_ano = sys.argv[1]
    type_name = sys.argv[2]

    success = load_csv_to_clickhouse(mes_ano, type_name)
    if not success:
        print("[Load] Ingestao nao realizada.")
        sys.exit(0)
