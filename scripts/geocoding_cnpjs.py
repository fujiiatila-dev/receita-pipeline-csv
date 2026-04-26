"""
Geocoding incremental dos CNPJs ativos sem latitude/longitude.

Le os CNPJs pendentes (existem em mt_empresa_socios_enriquecido_nov mas
nao estao em cnpj_geolocalizacao) e tenta resolver via:
  - Camada 1: Nominatim (OSM) usando endereco completo
  (Camada 2 / dump de CEP fica para quando importarmos a base local)

Bounded por GEOCODING_BATCH_LIMIT (padrao 1000) para nao consumir todo
o budget de uma execucao do DAG. O processo e cumulativo: a cada run
o backlog diminui.

Variaveis de ambiente:
  GEOCODING_PROVIDER       'nominatim' | 'off' (padrao: 'off')
  GEOCODING_BATCH_LIMIT    Quantidade maxima de CNPJs por execucao (padrao: 1000)
  GEOCODING_NOMINATIM_URL  URL da instancia Nominatim (padrao: https://nominatim.openstreetmap.org)
  GEOCODING_USER_AGENT     User-Agent obrigatorio para Nominatim publico
  GEOCODING_RATE_LIMIT     Segundos entre requests (padrao: 1.1 para respeitar ToS publico)
  GEOCODING_DESC_CNAE_LIKE Filtro ILIKE em desc_cnae_principal (padrao: vazio = todos)
                           Ex: '%combust%veis%ve%culos%' para priorizar postos de gasolina
  GEOCODING_PRIORITY_TABLE Tabela curada com schema {cnpj, tipo_logradouro, logradouro,
                           numero, bairro, desc_municipio, uf, cep, latitude} de onde ler
                           CNPJs pendentes (WHERE latitude IS NULL). Se setada, ignora
                           SOURCE_TABLE e GEOCODING_DESC_CNAE_LIKE.
                           Ex: 'empresas_ativas_do_brasil.postos_ativos_geolocalizacao'
  GEOCODING_FLUSH_EVERY    Quantos resolvidos acumular antes de fazer INSERT (padrao: 500).
                           Reduz perda em caso de interrupcao em runs longos.

Uso: python geocoding_cnpjs.py
"""
import os
import sys
import time

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db_clickhouse import get_clickhouse_client

DATABASE = "empresas_ativas_do_brasil"
GEO_TABLE = f"{DATABASE}.cnpj_geolocalizacao"
SOURCE_TABLE = f"{DATABASE}.mt_empresa_socios_enriquecido_nov"


def _build_address(row):
    """Monta endereco para query no Nominatim."""
    tipo_logradouro, logradouro, numero, bairro, municipio, uf, cep = row
    parts = []
    if logradouro:
        loc = f"{tipo_logradouro} {logradouro}".strip() if tipo_logradouro else logradouro
        if numero and numero not in ("0", "S/N", "SN"):
            loc = f"{loc}, {numero}"
        parts.append(loc)
    if bairro:
        parts.append(bairro)
    if municipio:
        parts.append(municipio)
    if uf:
        parts.append(uf)
    if cep:
        parts.append(cep)
    parts.append("Brasil")
    return ", ".join(parts)


def _geocode_nominatim(session, base_url, user_agent, address, max_retries=3):
    """Consulta Nominatim com retry em falhas transientes (timeout, 503, 502, 429).
    Retorna (lat, lon) em sucesso, None em endereco nao encontrado, ou
    levanta excecao apos esgotar retries em falhas de rede.
    """
    backoff = 2
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(
                f"{base_url}/search",
                params={"q": address, "format": "json", "limit": 1, "countrycodes": "br"},
                headers={"User-Agent": user_agent},
                timeout=15,
            )
            # Falhas transientes: retry com backoff
            if resp.status_code in (429, 502, 503, 504):
                last_err = f"HTTP {resp.status_code}"
                if attempt < max_retries:
                    sleep_s = backoff ** attempt
                    print(f"[Geocoding] {last_err} (tentativa {attempt}/{max_retries}), aguardando {sleep_s}s")
                    time.sleep(sleep_s)
                    continue
                return None
            if resp.status_code != 200:
                # 4xx nao-recuperavel: nao retry
                return None
            data = resp.json()
            if not data:
                # Endereco nao encontrado, nao e erro - nao retry
                return None
            return float(data[0]["lat"]), float(data[0]["lon"])
        except Exception as e:
            last_err = str(e)
            if attempt < max_retries:
                sleep_s = backoff ** attempt
                print(f"[Geocoding] Erro de rede '{last_err}' (tentativa {attempt}/{max_retries}), aguardando {sleep_s}s")
                time.sleep(sleep_s)
                continue
            print(f"[Geocoding] Falha definitiva apos {max_retries} tentativas para '{address[:80]}...': {last_err}")
            return None
    return None


def _flush_batch(client, batch):
    """Insere o batch acumulado em cnpj_geolocalizacao."""
    if not batch:
        return 0
    client.insert(
        GEO_TABLE,
        batch,
        column_names=["cnpj", "latitude", "longitude", "fonte"],
    )
    return len(batch)


def main():
    provider = os.environ.get("GEOCODING_PROVIDER", "off").lower()
    if provider == "off":
        print("[Geocoding] GEOCODING_PROVIDER=off. Pulando etapa.")
        return True

    if provider != "nominatim":
        print(f"[Geocoding] Provider '{provider}' nao suportado. Pulando.")
        return True

    batch_limit = int(os.environ.get("GEOCODING_BATCH_LIMIT", "1000"))
    base_url = os.environ.get(
        "GEOCODING_NOMINATIM_URL",
        "https://nominatim.openstreetmap.org",
    ).rstrip("/")
    user_agent = os.environ.get(
        "GEOCODING_USER_AGENT",
        "receita-pipeline-csv/1.0 (data-pipeline)",
    )
    rate_limit = float(os.environ.get("GEOCODING_RATE_LIMIT", "1.1"))
    desc_cnae_like = os.environ.get("GEOCODING_DESC_CNAE_LIKE", "").strip()
    priority_table = os.environ.get("GEOCODING_PRIORITY_TABLE", "").strip()

    client = get_clickhouse_client()
    if client is None:
        print("[Geocoding] Sem conexao com ClickHouse. Pulando.")
        return True

    try:
        import requests
    except ImportError:
        print("[Geocoding] requests nao instalado. Pulando.")
        return True

    # 1. Buscar CNPJs pendentes
    if priority_table:
        # Modo priority table: ler diretamente de uma tabela curada (ex: postos_ativos_geolocalizacao)
        # A tabela ja contem os enderecos e marca quem nao tem geo via latitude IS NULL
        print(f"[Geocoding] Modo priority table: {priority_table}")
        print(f"[Geocoding] Buscando ate {batch_limit} CNPJs pendentes (latitude IS NULL)...")
        query = f"""
        SELECT
            cnpj,
            tipo_logradouro,
            logradouro,
            numero,
            bairro,
            ifNull(desc_municipio, '') AS municipio,
            uf,
            cep
        FROM {priority_table}
        WHERE latitude IS NULL
          AND (logradouro != '' OR cep != '')
        LIMIT {batch_limit}
        """
    else:
        # Modo padrao: ler de mt_empresa_socios_enriquecido_nov com filtros de elegibilidade
        cnae_filter_clause = ""
        if desc_cnae_like:
            safe = desc_cnae_like.replace("'", "''")
            cnae_filter_clause = f"AND m.desc_cnae_principal ILIKE '{safe}'"
            print(f"[Geocoding] Filtro CNAE ativo: desc_cnae_principal ILIKE '{desc_cnae_like}'")

        print(f"[Geocoding] Buscando ate {batch_limit} CNPJs pendentes...")
        query = f"""
        SELECT
            m.cnpj,
            m.tipo_logradouro,
            m.logradouro,
            m.numero,
            m.bairro,
            ifNull(m.desc_municipio, '') AS municipio,
            m.uf,
            m.cep
        FROM {SOURCE_TABLE} m
        LEFT JOIN (SELECT cnpj FROM {GEO_TABLE} FINAL) g ON m.cnpj = g.cnpj
        WHERE g.cnpj IS NULL
          AND m.desc_situacao_cadastral = 'Ativa'
          AND m.uf_geo != 'nao informado'
          AND (m.logradouro != '' OR m.cep != '')
          {cnae_filter_clause}
        LIMIT {batch_limit}
        """
    rows = client.query(query).result_rows
    total_pending = len(rows)
    print(f"[Geocoding] {total_pending} CNPJs para processar nesta execucao")

    if total_pending == 0:
        print("[Geocoding] Nada a fazer.")
        return True

    # 2. Geocodar com rate limit + mini-batches (a cada N resolvidos, faz INSERT)
    flush_every = int(os.environ.get("GEOCODING_FLUSH_EVERY", "500"))
    session = requests.Session()
    pending_batch = []
    total_inserted = 0
    success = 0
    failure = 0

    try:
        for idx, row in enumerate(rows, start=1):
            cnpj = row[0]
            address_row = row[1:]
            address = _build_address(address_row)

            coords = _geocode_nominatim(session, base_url, user_agent, address)
            if coords:
                lat, lon = coords
                pending_batch.append((cnpj, lat, lon, "nominatim"))
                success += 1
            else:
                failure += 1

            # Flush periodico para nao perder trabalho em caso de interrupcao
            if len(pending_batch) >= flush_every:
                inserted = _flush_batch(client, pending_batch)
                total_inserted += inserted
                pending_batch = []
                print(
                    f"[Geocoding] FLUSH: {inserted} inseridos | "
                    f"acumulado={total_inserted} | progresso={idx}/{total_pending} "
                    f"(ok={success}, falha={failure})"
                )

            elif idx % 100 == 0:
                print(
                    f"[Geocoding] Progresso: {idx}/{total_pending} "
                    f"(ok={success}, falha={failure}, inseridos={total_inserted})"
                )

            time.sleep(rate_limit)
    except KeyboardInterrupt:
        print("[Geocoding] Interrompido pelo usuario. Salvando o que ja foi resolvido...")
    finally:
        # Flush final do que sobrou no batch
        if pending_batch:
            inserted = _flush_batch(client, pending_batch)
            total_inserted += inserted
            print(f"[Geocoding] FLUSH FINAL: {inserted} inseridos")

    print(
        f"[Geocoding] Concluido: {success} resolvidos, {failure} falhas, "
        f"{total_inserted} inseridos em {GEO_TABLE} "
        f"(falhas serao retentadas no proximo run)"
    )
    return True


if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
