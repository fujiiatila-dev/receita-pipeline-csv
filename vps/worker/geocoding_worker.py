"""
Geocoding worker para Nominatim self-hosted, com fallback em 3 camadas.

Fluxo por CNPJ:
  Camada 1: Nominatim com endereco completo (logradouro+numero+bairro+municipio+uf+cep)
  Camada 2: Nominatim sem numero (logradouro+bairro+municipio+uf)
  Camada 3: IBGE centroide via dim_municipio_geo (lookup em RAM)

Cobre 99,9% dos CNPJs - apenas casos onde municipio nao existe em
dim_municipio_geo nao serao resolvidos (raros, e raramente endereco
inexistente).

Variaveis de ambiente:
  CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD,
    CLICKHOUSE_DATABASE, CLICKHOUSE_SECURE
  NOMINATIM_URL            URL do Nominatim local (padrao http://nominatim:8080)
  WORKER_THREADS           Workers paralelos (padrao 8)
  WORKER_BATCH_SIZE        CNPJs lidos por iteracao (padrao 5000)
  WORKER_FLUSH_EVERY       Resolvidos antes de INSERT (padrao 1000)
  WORKER_IDLE_SLEEP        Segundos sem pendentes antes de retry (padrao 300)
  WORKER_REPROCESS_IBGE    true|false: reprocessa entradas ibge_centroide (padrao true)
"""
import os
import sys
import time
import socket
import signal
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event

import requests
import clickhouse_connect

# Forca IPv4 (Docker frequentemente nao tem rota IPv6 valida)
_ORIG_GETADDRINFO = socket.getaddrinfo


def _ipv4_only(host, port, _family=0, type=0, proto=0, flags=0):
    return _ORIG_GETADDRINFO(host, port, socket.AF_INET, type, proto, flags)


socket.getaddrinfo = _ipv4_only

# Configuracao via env
DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "empresas_ativas_do_brasil")
GEO_TABLE = f"{DATABASE}.cnpj_geolocalizacao"
SOURCE_TABLE = f"{DATABASE}.mt_empresa_socios_enriquecido_nov"
DIM_TABLE = f"{DATABASE}.dim_municipio_geo"

NOMINATIM_URL = os.environ.get("NOMINATIM_URL", "http://nominatim:8080").rstrip("/")
WORKERS = int(os.environ.get("WORKER_THREADS", "8"))
BATCH_SIZE = int(os.environ.get("WORKER_BATCH_SIZE", "5000"))
FLUSH_EVERY = int(os.environ.get("WORKER_FLUSH_EVERY", "1000"))
IDLE_SLEEP = int(os.environ.get("WORKER_IDLE_SLEEP", "300"))
REPROCESS_IBGE = os.environ.get("WORKER_REPROCESS_IBGE", "true").lower() == "true"

FONTE_NOMINATIM = "nominatim_local"
FONTE_IBGE = "ibge_centroide"

shutdown_event = Event()

# Cache global do dim_municipio_geo (carregado no boot)
# Chave: (uf, nome_normalizado), Valor: (lat, lon)
MUNICIPIO_GEO = {}


def _handle_signal(signum, _frame):
    print(f"[Worker] Sinal {signum} recebido, encerrando graciosamente...", flush=True)
    shutdown_event.set()


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


def _normalize(s):
    """UPPER + remove acentos. Compativel com import_ibge_municipios.py."""
    if not s:
        return ""
    nfkd = unicodedata.normalize("NFKD", s)
    only_ascii = "".join(c for c in nfkd if not unicodedata.combining(c))
    return only_ascii.upper().strip()


def get_ch_client():
    """Cria cliente ClickHouse novo (usado para reconexao apos zombie)."""
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=DATABASE,
        secure=os.environ.get("CLICKHOUSE_SECURE", "false").lower() == "true",
    )


def load_municipio_geo(client):
    """Carrega dim_municipio_geo na memoria (chave: (uf, nome_normalizado))."""
    global MUNICIPIO_GEO
    try:
        rows = client.query(
            f"SELECT uf, nome_normalizado, latitude, longitude FROM {DIM_TABLE}"
        ).result_rows
        MUNICIPIO_GEO = {(uf, nome): (lat, lon) for (uf, nome, lat, lon) in rows}
        print(
            f"[Worker] {len(MUNICIPIO_GEO):,} municipios IBGE carregados em memoria",
            flush=True,
        )
        return True
    except Exception as e:
        print(f"[Worker] Falha ao carregar dim_municipio_geo: {e}", flush=True)
        print("[Worker] Camada 3 (IBGE) ficara desabilitada", flush=True)
        return False


def build_address_full(cep, uf, municipio, tipo_logradouro, logradouro, numero, bairro):
    """Camada 1: endereco completo (logradouro+numero+bairro+municipio+uf+cep)."""
    parts = []
    if logradouro:
        loc = f"{tipo_logradouro} {logradouro}".strip() if tipo_logradouro else logradouro
        if numero and numero not in ("0", "S/N", "SN"):
            loc = f"{loc}, {numero}"
        parts.append(loc)
    for p in (bairro, municipio, uf, cep):
        if p:
            parts.append(p)
    parts.append("Brasil")
    return ", ".join(parts)


def build_address_no_number(uf, municipio, tipo_logradouro, logradouro, bairro):
    """Camada 2: sem numero (logradouro+bairro+municipio+uf)."""
    parts = []
    if logradouro:
        loc = f"{tipo_logradouro} {logradouro}".strip() if tipo_logradouro else logradouro
        parts.append(loc)
    for p in (bairro, municipio, uf):
        if p:
            parts.append(p)
    parts.append("Brasil")
    return ", ".join(parts)


def geocode(session, address):
    """Consulta Nominatim local. Retorna (lat, lon) ou None."""
    try:
        r = session.get(
            f"{NOMINATIM_URL}/search",
            params={
                "q": address,
                "format": "json",
                "limit": 1,
                "countrycodes": "br",
            },
            timeout=30,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        if not data:
            return None
        return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception:
        return None


def lookup_ibge(uf, municipio):
    """Camada 3: lookup local em dim_municipio_geo (RAM). Retorna (lat, lon) ou None."""
    if not uf or not municipio:
        return None
    return MUNICIPIO_GEO.get((uf, _normalize(municipio)))


def geocode_with_fallback(session, cep, uf, municipio, tipo_logradouro, logradouro, numero, bairro):
    """3 camadas: completo -> sem numero -> IBGE. Retorna (lat, lon, fonte) ou None."""
    if logradouro:
        # Camada 1: endereco completo
        addr = build_address_full(cep, uf, municipio, tipo_logradouro, logradouro, numero, bairro)
        coords = geocode(session, addr)
        if coords:
            return coords[0], coords[1], FONTE_NOMINATIM

        # Camada 2: sem numero (so se tinha numero antes)
        if numero and numero not in ("0", "S/N", "SN"):
            addr2 = build_address_no_number(uf, municipio, tipo_logradouro, logradouro, bairro)
            coords = geocode(session, addr2)
            if coords:
                return coords[0], coords[1], FONTE_NOMINATIM

    # Camada 3: IBGE centroide do municipio
    ibge = lookup_ibge(uf, municipio)
    if ibge:
        return ibge[0], ibge[1], FONTE_IBGE

    return None


def fetch_batch(client, batch_size):
    """Busca CNPJs pendentes (sem geo OU com fonte=ibge_centroide se REPROCESS_IBGE).

    NOTA: SETTINGS join_use_nulls = 1 obrigatorio - sem isso, 'g.cnpj IS NULL'
    nao retorna nada (ClickHouse usa string vazia em LEFT JOIN sem match por padrao).
    """
    fonte_filter = (
        "(g.cnpj IS NULL OR g.fonte = 'ibge_centroide')"
        if REPROCESS_IBGE
        else "g.cnpj IS NULL"
    )
    query = f"""
        SELECT
            trim(m.cnpj) AS cnpj,
            m.cep,
            m.uf,
            ifNull(m.desc_municipio, '') AS municipio,
            m.tipo_logradouro,
            m.logradouro,
            m.numero,
            m.bairro
        FROM {SOURCE_TABLE} m
        LEFT JOIN (SELECT cnpj, fonte FROM {GEO_TABLE} FINAL) g
            ON trim(m.cnpj) = g.cnpj
        WHERE {fonte_filter}
          AND m.desc_situacao_cadastral = 'Ativa'
          AND (m.logradouro != '' OR m.cep != '' OR m.uf != '')
        GROUP BY cnpj, cep, uf, municipio, tipo_logradouro, logradouro, numero, bairro
        LIMIT {batch_size}
        SETTINGS join_use_nulls = 1
    """
    return client.query(query).result_rows


def insert_batch(client_holder, batch, max_retries=3):
    """INSERT com verificacao pos-insert e retry com reconexao."""
    if not batch:
        return 0

    expected = len(batch)
    cnpjs = [r[0] for r in batch]
    cnpjs_quoted = ",".join(f"'{c}'" for c in cnpjs)
    # Conta cnpjs unicos do batch com fonte do worker (nominatim_local OU ibge_centroide)
    verify_sql = (
        f"SELECT uniqExact(cnpj) FROM {GEO_TABLE} "
        f"WHERE cnpj IN ({cnpjs_quoted}) "
        f"AND fonte IN ('{FONTE_NOMINATIM}', '{FONTE_IBGE}')"
    )

    for attempt in range(1, max_retries + 1):
        try:
            client_holder["client"].insert(
                GEO_TABLE,
                batch,
                column_names=[
                    "cnpj", "cep", "uf", "municipio",
                    "logradouro", "numero", "bairro",
                    "latitude", "longitude", "fonte",
                ],
            )
            actual = client_holder["client"].query(verify_sql).result_rows[0][0]
            if actual >= expected:
                return expected

            print(
                f"[Worker] FLUSH inconsistente "
                f"(esperado={expected}, persistido={actual}, tentativa={attempt}/{max_retries}). "
                f"Reconectando...",
                flush=True,
            )
            client_holder["client"] = get_ch_client()
        except Exception as e:
            print(
                f"[Worker] FLUSH excecao (tentativa {attempt}/{max_retries}): {e}. "
                f"Reconectando...",
                flush=True,
            )
            try:
                client_holder["client"] = get_ch_client()
            except Exception as conn_err:
                print(f"[Worker] Falha ao reconectar: {conn_err}", flush=True)
            time.sleep(5)

    # Fallback: dump em arquivo para recuperacao manual
    fallback_path = f"/tmp/failed_batch_{int(time.time())}.csv"
    try:
        with open(fallback_path, "a", encoding="utf-8") as f:
            f.write("cnpj,cep,uf,municipio,logradouro,numero,bairro,latitude,longitude,fonte\n")
            for row in batch:
                f.write(",".join(str(c).replace(",", " ") for c in row) + "\n")
        print(
            f"[Worker] FLUSH ABORTADO apos {max_retries} tentativas. "
            f"Batch salvo em {fallback_path}",
            flush=True,
        )
    except Exception as fallback_err:
        print(f"[Worker] CRITICO: falha ao salvar fallback: {fallback_err}", flush=True)

    return 0


def process_one(session, row):
    """row: (cnpj, cep, uf, municipio, tipo_logradouro, logradouro, numero, bairro)"""
    cnpj, cep, uf, municipio, tipo_logradouro, logradouro, numero, bairro = row
    result = geocode_with_fallback(
        session, cep, uf, municipio, tipo_logradouro, logradouro, numero, bairro
    )
    if result is None:
        return cnpj, cep, uf, municipio, logradouro, numero, bairro, None, None, None
    lat, lon, fonte = result
    return cnpj, cep, uf, municipio, logradouro, numero, bairro, lat, lon, fonte


def process_batch_parallel(rows):
    """Processa rows em paralelo. Retorna (resolvidos, falhas, by_source_dict)."""
    resolved = []
    failed = 0
    by_source = {FONTE_NOMINATIM: 0, FONTE_IBGE: 0}
    session = requests.Session()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = [ex.submit(process_one, session, row) for row in rows]
        for f in as_completed(futures):
            try:
                cnpj, cep, uf, municipio, logradouro, numero, bairro, lat, lon, fonte = f.result()
            except Exception as e:
                print(f"[Worker] Erro no future: {e}", flush=True)
                failed += 1
                continue
            if fonte:
                resolved.append((
                    cnpj, cep, uf, municipio, logradouro, numero, bairro,
                    lat, lon, fonte,
                ))
                by_source[fonte] = by_source.get(fonte, 0) + 1
            else:
                failed += 1
    return resolved, failed, by_source


def wait_nominatim_ready():
    """Aguarda Nominatim responder. Retorna False se shutdown sinalizado."""
    while not shutdown_event.is_set():
        try:
            r = requests.get(f"{NOMINATIM_URL}/status", timeout=10)
            if r.status_code == 200:
                print("[Worker] Nominatim respondendo", flush=True)
                return True
        except Exception:
            pass
        print("[Worker] Nominatim ainda nao pronto, aguardando 30s...", flush=True)
        if shutdown_event.wait(30):
            return False
    return False


def main():
    print("=" * 60, flush=True)
    print("[Worker] Geocoding worker iniciando (fallback em 3 camadas)", flush=True)
    print(f"[Worker] Threads paralelas: {WORKERS}", flush=True)
    print(f"[Worker] Batch size: {BATCH_SIZE}", flush=True)
    print(f"[Worker] Flush every: {FLUSH_EVERY}", flush=True)
    print(f"[Worker] Idle sleep: {IDLE_SLEEP}s", flush=True)
    print(f"[Worker] Reprocess IBGE: {REPROCESS_IBGE}", flush=True)
    print(f"[Worker] Nominatim: {NOMINATIM_URL}", flush=True)
    print("=" * 60, flush=True)

    if not wait_nominatim_ready():
        return

    try:
        client = get_ch_client()
    except Exception as e:
        print(f"[Worker] Falha inicial ao conectar ClickHouse: {e}", flush=True)
        sys.exit(1)
    print("[Worker] Conectado ao ClickHouse", flush=True)

    # Carrega dim_municipio_geo em memoria para Camada 3
    load_municipio_geo(client)

    client_holder = {"client": client}

    total_inserted = 0
    total_by_source = {FONTE_NOMINATIM: 0, FONTE_IBGE: 0}
    pending_buffer = []
    iteration = 0

    while not shutdown_event.is_set():
        iteration += 1
        try:
            print(
                f"[Worker] Iteracao #{iteration}: buscando ate {BATCH_SIZE} pendentes...",
                flush=True,
            )
            rows = fetch_batch(client_holder["client"], BATCH_SIZE)

            if not rows:
                print(
                    f"[Worker] Nenhum pendente. Dormindo {IDLE_SLEEP}s antes de tentar de novo...",
                    flush=True,
                )
                if shutdown_event.wait(IDLE_SLEEP):
                    break
                continue

            print(f"[Worker] {len(rows)} pendentes neste lote", flush=True)
            resolved, failed, by_source = process_batch_parallel(rows)
            for k, v in by_source.items():
                total_by_source[k] = total_by_source.get(k, 0) + v
            print(
                f"[Worker] Lote: {len(resolved)} resolvidos "
                f"(nominatim={by_source.get(FONTE_NOMINATIM, 0)}, "
                f"ibge={by_source.get(FONTE_IBGE, 0)}), "
                f"{failed} falhas sem geo (cidade nao mapeada)",
                flush=True,
            )

            pending_buffer.extend(resolved)

            # Flush em chunks de FLUSH_EVERY
            while len(pending_buffer) >= FLUSH_EVERY:
                chunk = pending_buffer[:FLUSH_EVERY]
                pending_buffer = pending_buffer[FLUSH_EVERY:]
                inserted = insert_batch(client_holder, chunk)
                total_inserted += inserted
                print(
                    f"[Worker] FLUSH: {inserted} inseridos | "
                    f"acumulado: {total_inserted} "
                    f"(nominatim={total_by_source.get(FONTE_NOMINATIM, 0)}, "
                    f"ibge={total_by_source.get(FONTE_IBGE, 0)})",
                    flush=True,
                )

        except Exception as e:
            print(f"[Worker] Erro no loop principal: {e}. Aguardando 30s...", flush=True)
            if shutdown_event.wait(30):
                break
            try:
                client_holder["client"] = get_ch_client()
            except Exception as conn_err:
                print(f"[Worker] Reconexao falhou: {conn_err}", flush=True)

    # Flush final do buffer pendente
    if pending_buffer:
        print(
            f"[Worker] Flush final: {len(pending_buffer)} pendentes no buffer",
            flush=True,
        )
        inserted = insert_batch(client_holder, pending_buffer)
        total_inserted += inserted

    print(
        f"[Worker] Encerrando. Total inserido: {total_inserted} "
        f"(nominatim={total_by_source.get(FONTE_NOMINATIM, 0)}, "
        f"ibge={total_by_source.get(FONTE_IBGE, 0)})",
        flush=True,
    )


if __name__ == "__main__":
    main()
