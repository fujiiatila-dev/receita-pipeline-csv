"""
Fallback: completa cnpj_geolocalizacao com centroides do IBGE para
todos os CNPJs ativos que ainda nao tem geolocalizacao.

Roda APOS geocoding_cnpjs no DAG: o que Nominatim resolveu fica;
o resto recebe centroide de municipio (precisao de cidade) com
fonte=ibge_centroide.

ReplacingMergeTree(data_geocoding) na cnpj_geolocalizacao garante que
geocoding mais precisos (Nominatim, geocoding por endereco) sobrescrevem
o ibge_centroide automaticamente quando aparecerem em runs futuros.

Uso: python geocoding_ibge_fallback.py
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db_clickhouse import get_clickhouse_client
import import_ibge_municipios

DATABASE = "empresas_ativas_do_brasil"
GEO_TABLE = f"{DATABASE}.cnpj_geolocalizacao"
DIM_TABLE = f"{DATABASE}.dim_municipio_geo"
SOURCE_TABLE = f"{DATABASE}.mt_empresa_socios_enriquecido_nov"


def _ensure_dim_municipio_geo(client):
    """Garante que dim_municipio_geo existe com dados. Importa se necessario."""
    exists = client.query(
        f"SELECT count() FROM system.tables "
        f"WHERE database = '{DATABASE}' AND name = 'dim_municipio_geo'"
    ).result_rows[0][0]

    if not exists:
        print("[IBGE Fallback] dim_municipio_geo nao existe, importando...")
        return import_ibge_municipios.main()

    count = client.query(f"SELECT count() FROM {DIM_TABLE}").result_rows[0][0]
    if count == 0:
        print("[IBGE Fallback] dim_municipio_geo vazia, reimportando...")
        return import_ibge_municipios.main()

    print(f"[IBGE Fallback] dim_municipio_geo OK ({count:,} municipios)")
    return True


def main():
    client = get_clickhouse_client()
    if client is None:
        print("[IBGE Fallback] Sem conexao com ClickHouse. Pulando.")
        return True

    if not _ensure_dim_municipio_geo(client):
        print("[IBGE Fallback] Falha ao garantir dim_municipio_geo. Pulando.")
        return False

    # Snapshot antes
    before = client.query(f"SELECT count() FROM {GEO_TABLE} FINAL").result_rows[0][0]
    print(f"[IBGE Fallback] Total antes: {before:,}")

    # Bulk INSERT: somente para CNPJs ativos sem nenhuma fonte registrada
    print("[IBGE Fallback] Inserindo centroides para CNPJs pendentes...")
    client.command(f"""
        INSERT INTO {GEO_TABLE} (cnpj, latitude, longitude, fonte, data_geocoding)
        SELECT
            m.cnpj,
            geo.latitude,
            geo.longitude,
            'ibge_centroide' AS fonte,
            now() AS data_geocoding
        FROM {SOURCE_TABLE} m
        INNER JOIN {DIM_TABLE} geo
            ON m.desc_municipio = geo.nome_normalizado
           AND m.uf = geo.uf
        LEFT JOIN (SELECT cnpj FROM {GEO_TABLE} FINAL) g
            ON trim(m.cnpj) = g.cnpj
        WHERE g.cnpj IS NULL
          AND m.desc_situacao_cadastral = 'Ativa'
          AND m.uf_geo != 'nao informado'
    """)

    # Snapshot depois
    after = client.query(f"SELECT count() FROM {GEO_TABLE} FINAL").result_rows[0][0]
    inserted = after - before
    print(f"[IBGE Fallback] {inserted:,} novos pontos com fonte=ibge_centroide")
    print(f"[IBGE Fallback] Total depois: {after:,}")

    # Distribuicao por fonte (visibilidade)
    by_source = client.query(
        f"SELECT fonte, count() AS qtd FROM {GEO_TABLE} FINAL "
        f"GROUP BY fonte ORDER BY qtd DESC"
    ).result_rows
    print("[IBGE Fallback] Distribuicao por fonte:")
    for fonte, qtd in by_source:
        print(f"  {fonte}: {qtd:,}")

    return True


if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
