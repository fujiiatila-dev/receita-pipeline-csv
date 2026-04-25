"""
Limpeza da tabela cnpj_geolocalizacao: remove CNPJs que nao estao mais
ativos na base atual (mt_empresa_socios_enriquecido_nov), evitando crescimento
infinito da tabela auxiliar.

Sanity check: aborta se mt_empresa_socios_enriquecido_nov estiver vazia ou
com volume suspeito (proteje contra deletar tudo apos um transform falho).

Uso: python cleanup_cnpj_geolocalizacao.py
"""
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db_clickhouse import get_clickhouse_client

DATABASE = "empresas_ativas_do_brasil"
GEO_TABLE = f"{DATABASE}.cnpj_geolocalizacao"
SOURCE_TABLE = f"{DATABASE}.mt_empresa_socios_enriquecido_nov"

# Threshold de seguranca: aborta cleanup se a base atual estiver abaixo disso.
# Brasil tem ~50M de empresas ativas; 10M e um piso conservador.
MIN_ACTIVE_THRESHOLD = int(os.environ.get("CLEANUP_GEO_MIN_THRESHOLD", "10000000"))


def main():
    client = get_clickhouse_client()
    if client is None:
        print("[CleanupGeo] Sem conexao com ClickHouse. Pulando.")
        return True

    # 1. Sanity check: existe e tem volume razoavel?
    result = client.query(
        f"SELECT count() FROM {SOURCE_TABLE} WHERE desc_situacao_cadastral = 'Ativa'"
    )
    active_count = result.result_rows[0][0]

    if active_count < MIN_ACTIVE_THRESHOLD:
        print(
            f"[CleanupGeo] ABORTADO: base de empresas ativas com volume suspeito "
            f"({active_count:,} < {MIN_ACTIVE_THRESHOLD:,}). "
            f"Cleanup nao executado para nao corromper {GEO_TABLE}."
        )
        return False

    # 2. Contagem antes
    before = client.query(f"SELECT count() FROM {GEO_TABLE} FINAL").result_rows[0][0]
    print(f"[CleanupGeo] {GEO_TABLE} antes: {before:,} registros")

    # 3. Mutation: deletar CNPJs nao mais ativos
    print("[CleanupGeo] Removendo CNPJs nao mais ativos (mutation assincrona)...")
    client.command(f"""
        ALTER TABLE {GEO_TABLE}
        DELETE WHERE cnpj NOT IN (
            SELECT cnpj
            FROM {SOURCE_TABLE}
            WHERE desc_situacao_cadastral = 'Ativa'
        )
    """)

    # 4. Otimizar (forca merge para compactar partes apos delete)
    print("[CleanupGeo] OPTIMIZE FINAL para compactar...")
    client.command(f"OPTIMIZE TABLE {GEO_TABLE} FINAL")

    # 5. Contagem depois
    after = client.query(f"SELECT count() FROM {GEO_TABLE} FINAL").result_rows[0][0]
    removed = before - after
    print(
        f"[CleanupGeo] {GEO_TABLE} depois: {after:,} registros "
        f"({removed:,} removidos)"
    )

    return True


if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
