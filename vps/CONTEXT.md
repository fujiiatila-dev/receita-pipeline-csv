# Contexto e estado atual — Geocoding self-hosted na VPS

> Documento de retomada. Criado em 2026-05-06. Use isso para entender onde paramos e como continuar de qualquer máquina.

## TL;DR

Estamos migrando o geocoding em massa de Nominatim público (que nos baniu) para Nominatim self-hosted numa VPS dedicada. **Nominatim importando agora** (~6-12h pro import inicial concluir). Falta executar a Fase 1 (TRUNCATE + reseed via PGFN) antes do Nominatim ficar healthy, para evitar reprocessamento desnecessário de 13M CNPJs.

---

## Estado atual (2026-05-06)

| Item | Status |
|---|---|
| Stack VPS deployado | ✅ Rodando |
| Nominatim import inicial | 🔄 Em progresso (~456 MB / ~60 GB) |
| Bug PG14 vs PG16 | ✅ Corrigido (commit `4eabadf`) |
| Volume persistente confirmado | ✅ 456 MB no `vps_nominatim_data` |
| Worker geocoding | ⏸️ Esperando Nominatim ficar healthy |
| Fase 1 (reset PGFN) | ❌ Pendente — usuário rodando query de correção da `_nov` |

---

## Arquitetura

```
VPS Ubuntu 24.10 — IP 162.243.210.51 (DigitalOcean NYC2, 4 vCPU, 16GB RAM, 320GB)
├── ⚪ ClickHouse local (porta 8123)         [projeto pausado, intocado]
├── ⚪ MinIO existente (9000/9001)            [projeto pausado, intocado]
├── ⚪ CH-UI (5521)                           [projeto pausado, intocado]
├── 🆕 postgres-airflow                       [metadata Airflow]
├── 🆕 minio-receita (9100/9101)              [dedicado ao receita, separado]
├── 🆕 airflow-init / webserver (8080) / scheduler
├── 🆕 nominatim (8090, mediagis/nominatim:4.4)
└── 🆕 geocoding-worker (8 threads paralelas)

ClickHouse fonte de verdade: 177.67.54.126:8123 (externo, mantido)
```

---

## Decisões arquiteturais (alinhadas)

1. **ClickHouse externo continua sendo a fonte de verdade** (não migrar dados)
2. **MinIO dedicado** para o receita na VPS (porta 9100/9101)
3. **Pausar worker durante transform mensal** (manual: `docker compose stop geocoding-worker`)
4. **DAG mensal continua manual** (sem schedule automático)
5. **Acesso Airflow via IP público** (sem domínio/SSL por enquanto)

---

## Estratégia de geocoding

### Tabela auxiliar: `empresas_ativas_do_brasil.cnpj_geolocalizacao`

Schema atual:
```sql
CREATE TABLE cnpj_geolocalizacao (
    cnpj            String,
    latitude        Float32,
    longitude       Float32,
    fonte           LowCardinality(String),
    data_geocoding  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(data_geocoding)
ORDER BY (cnpj);
```

`ReplacingMergeTree(data_geocoding)` deduplica por `cnpj` — quem inserir por **último** (data_geocoding mais nova) ganha. Permite upgrades de fonte (ex: ibge_centroide → nominatim_local) automáticos.

### Fontes em ordem de precisão

| Fonte | Precisão | Uso |
|---|---|---|
| `pgfn_novembro` | Logradouro (oficial PGFN) | **Seed inicial** — Fase 1 pendente |
| `nominatim_local` | Logradouro (OSM) | Worker contínuo |
| `nominatim` | Logradouro (público, banido) | Histórico, não usar mais |
| `estabelecimentos_novembro` | Logradouro | Seed antigo, será substituído pela PGFN |
| `ibge_centroide` | Centro de cidade (5570 municípios) | Fallback quando outras falham |

### Plano em fases

**Fase 1 — Reset + seed via PGFN** ⏳ pendente

Pré-requisito: usuário tem query rodando que corrige `mt_empresa_socios_enriquecido_nov.latitude/longitude` usando `mt_empresa_socios_pgfn` como referência (mais precisa, oficial).

Quando concluir, executar:

```sql
TRUNCATE TABLE empresas_ativas_do_brasil.cnpj_geolocalizacao;

INSERT INTO empresas_ativas_do_brasil.cnpj_geolocalizacao
    (cnpj, latitude, longitude, fonte, data_geocoding)
SELECT
    trim(cnpj) AS cnpj,
    latitude,
    longitude,
    'pgfn_novembro' AS fonte,
    now() AS data_geocoding
FROM empresas_ativas_do_brasil.mt_empresa_socios_enriquecido_nov
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL
  AND trim(cnpj) != ''
  AND desc_situacao_cadastral = 'Ativa';

OPTIMIZE TABLE empresas_ativas_do_brasil.cnpj_geolocalizacao FINAL;

SELECT fonte, count() FROM empresas_ativas_do_brasil.cnpj_geolocalizacao FINAL GROUP BY fonte;
```

Resultado esperado: ~13M+ entradas com fonte=`pgfn_novembro`.

**Fase 2 — Worker contínuo (Nominatim self-hosted)** 🔄 em curso

- Roda na VPS 24/7
- 8 threads paralelas, sem rate limit (Nominatim local)
- Lê de `mt_empresa_socios_enriquecido_nov` os CNPJs que estão pendentes (sem geo) OU com `fonte='ibge_centroide'` (upgrade)
- Insere com `fonte='nominatim_local'` em `cnpj_geolocalizacao`
- Backlog estimado pós-Fase 1: ~50k-200k CNPJs novos pós-novembro

**Fase 3 — DAG mensal** ✅ implementado

DAG `receita_federal_csv_generator` no Airflow:

```
get_latest_date
  → download_* (paralelo)
  → process_* (Spark)
  → load_* (CH staging)
  → transform_final (mt_empresa_socios_enriquecido_nov)
  → geocoding_cnpjs            (off por default; worker dedicado faz o trabalho)
  → geocoding_ibge_fallback    (sempre roda, fallback de cidade pra novos)
  → cleanup_cnpj_geolocalizacao (remove CNPJs inativos)
  → cleanup_local_data
```

DAG é manual (`schedule=None`). Pausar worker antes de rodar (RAM apertada com Spark).

---

## Bugs encontrados e fixes

### 1. Nominatim público baniu IP

**Sintoma:** após 4h30min de uso a 1.1s/req com 230k requests, IP banido. 99,9% das queries começaram a falhar com timeout.

**Diagnóstico:** ToS do Nominatim público é vago, mas comunidade indica ~5k/dia como soft cap. Excedemos.

**Solução:** migrar para Nominatim self-hosted (sem ToS). Em curso.

### 2. Docker container sem rota IPv6

**Sintoma:** primeiras tentativas de geocoding travavam silenciosamente. Containers Docker frequentemente não têm rota IPv6 funcional, e DNS de `nominatim.openstreetmap.org` retornava só IPv6.

**Solução:** patch em `socket.getaddrinfo` no script Python forçando AF_INET. Aplicado em `scripts/geocoding_cnpjs.py` e `vps/worker/geocoding_worker.py` (commit `3a1fe74`).

### 3. ClickHouse-connect zombie connections

**Sintoma:** em runs longos (>1h), `client.insert()` retornava 200 OK mas dados não persistiam. 67% de perda silenciosa.

**Solução:** verificação pós-insert + retry com nova conexão + dump em CSV de fallback após 3 tentativas falhadas. Aplicado em `_flush_batch()` (commit `f67726e`).

### 4. Volume Nominatim no path errado (PG 14 vs 16)

**Sintoma:** `mediagis/nominatim:4.4` usa PostgreSQL 14, mas eu montei o volume em `/var/lib/postgresql/16/main` por engano. Volume ficava vazio, dados iam para filesystem efêmero.

**Confirmação em runtime:**
- `/var/lib/postgresql/14/main` = 1.3 GB (dados reais)
- `/var/lib/postgresql/16/main` = 4 KB (mountpoint vazio)

**Solução:** corrigido em `vps/docker-compose.vps.yml` para usar PG 14. Stack reiniciada com volume vazio (perdemos ~7 min de import). Volume agora persistente confirmado: 456 MB no `vps_nominatim_data`. Commit `4eabadf`.

---

## Pendências imediatas (próximas ações do usuário)

1. ⏳ **Aguardar query de correção da `_nov`** terminar (usuário está rodando)
2. ⏳ **Executar Fase 1** (TRUNCATE + reseed PGFN) — SQL acima neste documento
3. ⏳ **Aguardar Nominatim ficar healthy** (~6-12h após start)
4. ✅ **Worker entra automaticamente em ação** quando os 2 acima estiverem prontos

---

## Branch e commits relevantes

Branch atual: `feat/nominatim-self-hosted-vps`

Commits importantes (em ordem cronológica):

| Commit | Descrição |
|---|---|
| `7f7a7a6` | feat: integrar IBGE fallback no DAG mensal |
| `74c6b1d` | feat: stack VPS completo (Airflow + Nominatim + worker) |
| `4eabadf` | fix: volume Nominatim deve montar PG 14 (não 16) |

Branch já com push em `origin/feat/nominatim-self-hosted-vps`. Acessível em outra máquina via:

```bash
git clone https://github.com/fujiiatila-dev/receita-pipeline-csv.git
cd receita-pipeline-csv
git checkout feat/nominatim-self-hosted-vps
```

---

## Arquivos-chave

| Arquivo | Propósito |
|---|---|
| `vps/docker-compose.vps.yml` | Stack VPS completo |
| `vps/worker/geocoding_worker.py` | Worker paralelo Nominatim |
| `vps/worker/Dockerfile` | Build do worker |
| `vps/worker/requirements.txt` | clickhouse-connect, requests |
| `vps/.env.example` | Template das credenciais |
| `vps/README.md` | Deploy passo-a-passo, troubleshooting, operação |
| `scripts/geocoding_cnpjs.py` | Geocoding usado no DAG mensal (off por default) |
| `scripts/geocoding_ibge_fallback.py` | Fallback IBGE pós-Nominatim |
| `scripts/import_ibge_municipios.py` | One-shot: importa CSV IBGE como `dim_municipio_geo` |
| `dags/download_receita_federal.py` | DAG mensal completo |

---

## Comandos para retomar

### Acessar VPS

```bash
ssh root@162.243.210.51
cd /opt/receita-pipeline-csv
git pull origin feat/nominatim-self-hosted-vps
```

### Ver status atual da stack

```bash
cd /opt/receita-pipeline-csv/vps
docker compose -f docker-compose.vps.yml ps
```

### Ver progresso do import Nominatim

```bash
docker logs --tail 30 nominatim
docker exec nominatim du -sh /var/lib/postgresql/14/main
```

### Verificar volume persistente

```bash
docker run --rm -v vps_nominatim_data:/data busybox du -sh /data
```

### Verificar se Nominatim está healthy

```bash
curl http://localhost:8090/status
# OK = pronto. Erro = ainda importando ou problema.
```

### Acompanhar worker (após Nominatim healthy)

```bash
docker logs -f geocoding-worker
```

### Verificar contagem por fonte no ClickHouse (DBeaver)

```sql
SELECT fonte, count() AS total
FROM empresas_ativas_do_brasil.cnpj_geolocalizacao FINAL
GROUP BY fonte
ORDER BY fonte;
```

### Pausar worker (antes do DAG mensal pesado)

```bash
docker compose -f docker-compose.vps.yml stop geocoding-worker
```

### Retomar worker

```bash
docker compose -f docker-compose.vps.yml start geocoding-worker
```

---

## Configuração do worker (env vars)

| Variável | Default | Descrição |
|---|---|---|
| `WORKER_THREADS` | 8 | Threads paralelas |
| `WORKER_BATCH_SIZE` | 5000 | CNPJs lidos por iteração |
| `WORKER_FLUSH_EVERY` | 1000 | Resolvidos antes de INSERT |
| `WORKER_IDLE_SLEEP` | 300 | Segundos entre tentativas quando sem pendentes |
| `WORKER_REPROCESS_IBGE` | true | Se reprocessa entradas `ibge_centroide` para upgrade |

Ajustar no `.env` da VPS e reiniciar worker:
```bash
docker compose -f docker-compose.vps.yml up -d geocoding-worker
```

---

## Throughput esperado pós-Nominatim healthy

- Conservador: ~150 req/s = ~13M/dia
- Otimista (queries simples, SSD): ~500 req/s = ~43M/dia
- Backlog de ~50k-200k pós-Fase 1: **horas** (em vez dos anos no Nominatim público)

---

## Considerações futuras (não-urgentes)

1. **Upgrade Ubuntu 24.10 → 24.04 LTS ou 25.04** — Oracular EOL, repos retornam 404
2. **Domínio + SSL para Airflow** — atualmente IP público sem cert
3. **Schedule automático do DAG mensal** — hoje manual
4. **Updates incrementais OSM** mensais via `nominatim replication --once` (cron)
5. **Backup periódico** dos volumes (sobretudo `postgres_airflow_data`)

---

## Acessos

- **VPS SSH:** `root@162.243.210.51` (chave SSH do usuário)
- **Airflow UI:** http://162.243.210.51:8080 (admin / valor de `AIRFLOW_ADMIN_PASSWORD`)
- **MinIO Console (receita):** http://162.243.210.51:9101
- **Nominatim UI nativa:** http://162.243.210.51:8090 (após import concluir)
- **ClickHouse externo (fonte de dados):** 177.67.54.126:8123
- **GitHub:** https://github.com/fujiiatila-dev/receita-pipeline-csv

---

**Última atualização:** 2026-05-06 ~00:30 UTC
**Próxima ação esperada:** Aguardar query de correção da `_nov` finalizar → executar Fase 1.
