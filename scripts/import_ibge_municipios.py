"""
Importa o CSV de municipios do IBGE em dim_municipio_geo no ClickHouse.

CSV esperado em /opt/airflow/data/municipios_ibge.csv com colunas:
codigo_ibge, nome, latitude, longitude, capital, codigo_uf, ...

Aplica normalizacao no nome (UPPER + remove acentos) e deriva sigla UF
a partir dos 2 primeiros digitos do codigo IBGE.

Uso: python import_ibge_municipios.py
"""
import sys
import os
import csv
import unicodedata
import urllib.request

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db_clickhouse import get_clickhouse_client

DATABASE = "empresas_ativas_do_brasil"
TABLE = f"{DATABASE}.dim_municipio_geo"
CSV_PATH = "/opt/airflow/data/municipios_ibge.csv"
CSV_URL = (
    "https://raw.githubusercontent.com/kelvins/"
    "municipios-brasileiros/main/csv/municipios.csv"
)

# Mapeamento codigo_uf IBGE -> sigla
UF_MAP = {
    11: "RO", 12: "AC", 13: "AM", 14: "RR", 15: "PA", 16: "AP", 17: "TO",
    21: "MA", 22: "PI", 23: "CE", 24: "RN", 25: "PB", 26: "PE", 27: "AL",
    28: "SE", 29: "BA", 31: "MG", 32: "ES", 33: "RJ", 35: "SP", 41: "PR",
    42: "SC", 43: "RS", 50: "MS", 51: "MT", 52: "GO", 53: "DF",
}


def _normalize(s):
    """UPPER + remove acentos. Bate com formato Receita."""
    nfkd = unicodedata.normalize("NFKD", s)
    only_ascii = "".join(c for c in nfkd if not unicodedata.combining(c))
    return only_ascii.upper().strip()


def _download_csv():
    """Baixa o CSV do IBGE se nao existir localmente."""
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    print(f"[Import] Baixando CSV de {CSV_URL}...")
    urllib.request.urlretrieve(CSV_URL, CSV_PATH)
    print(f"[Import] Salvo em {CSV_PATH}")


def main():
    if not os.path.exists(CSV_PATH):
        print(f"[Import] CSV nao encontrado em {CSV_PATH}, baixando...")
        try:
            _download_csv()
        except Exception as e:
            print(f"[Import] Falha no download: {e}")
            return False

    client = get_clickhouse_client()
    if client is None:
        print("[Import] Sem conexao com ClickHouse")
        return False

    # 1. Recriar tabela
    client.command(f"DROP TABLE IF EXISTS {TABLE}")
    client.command(f"""
    CREATE TABLE {TABLE}
    (
        codigo_ibge       UInt32,
        nome              String,
        nome_normalizado  String,
        uf                LowCardinality(String),
        latitude          Float32,
        longitude         Float32
    )
    ENGINE = MergeTree()
    ORDER BY (uf, nome_normalizado)
    """)
    print(f"[Import] Tabela {TABLE} recriada")

    # 2. Ler CSV e preparar batch
    rows = []
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                codigo_ibge = int(row["codigo_ibge"])
                nome = row["nome"].strip()
                lat = float(row["latitude"])
                lon = float(row["longitude"])
                codigo_uf = int(row["codigo_uf"])
                uf = UF_MAP.get(codigo_uf)
                if not uf:
                    print(f"[Import] UF desconhecida (codigo {codigo_uf}) para {nome}")
                    continue
                rows.append((codigo_ibge, nome, _normalize(nome), uf, lat, lon))
            except (ValueError, KeyError) as e:
                print(f"[Import] Linha invalida: {row} ({e})")

    print(f"[Import] {len(rows)} municipios prontos para insert")

    # 3. Insert
    client.insert(
        TABLE,
        rows,
        column_names=["codigo_ibge", "nome", "nome_normalizado", "uf", "latitude", "longitude"],
    )

    # 4. Verificar
    total = client.query(f"SELECT count() FROM {TABLE}").result_rows[0][0]
    by_uf = client.query(
        f"SELECT uf, count() AS qtd FROM {TABLE} GROUP BY uf ORDER BY uf"
    ).result_rows
    print(f"[Import] Total inserido: {total}")
    print("[Import] Por UF:")
    for uf, qtd in by_uf:
        print(f"  {uf}: {qtd}")

    return True


if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
