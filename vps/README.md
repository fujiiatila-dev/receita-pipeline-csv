# Deploy VPS — Pipeline Receita + Nominatim self-hosted

Stack completo para rodar o pipeline da Receita Federal **independente da maquina pessoal**, incluindo Nominatim self-hosted para geocoding em massa sem rate limits.

## Arquitetura

```
VPS (Ubuntu)
+-- postgres-airflow (metadata Airflow, dedicado)
+-- minio-receita (porta 9100/9101, separado do MinIO existente)
+-- airflow-init / webserver (8080) / scheduler
+-- nominatim (porta 8090, ~80GB disco, ~4-8GB RAM)
+-- geocoding-worker (8 workers paralelos, sem rate limit)

ClickHouse externo: 177.67.54.126:8123 (mantido como fonte de verdade)
```

## Recursos esperados

| Estado | RAM ativa | Disco usado |
|---|---|---|
| Idle (steady) | ~7-9 GB | ~100 GB |
| Pico (transform mensal) | ~12-14 GB | ~120 GB |

VPS de 16 GB / 320 GB cabe com folga, mas durante o transform mensal recomenda-se **pausar o Nominatim worker** (ver secao "Operacao").

## Pre-requisitos na VPS

- Ubuntu 22.04+ (testado em 24.10)
- Docker 26+ (`docker --version`)
- Docker Compose v2 (`docker compose version`)
- Acesso SSH como root ou usuario com permissao sudo
- Acesso de rede ao ClickHouse externo (`curl http://177.67.54.126:8123/ping`)

## Deploy passo a passo

### 1. Clonar o repositorio

```bash
cd /opt
git clone https://github.com/fujiiatila-dev/receita-pipeline-csv.git
cd receita-pipeline-csv
git checkout feat/nominatim-self-hosted-vps
```

### 2. Configurar variaveis de ambiente

```bash
cd vps
cp .env.example .env
# Edite .env com suas credenciais reais (especialmente CLICKHOUSE_PASSWORD se aplicavel)
nano .env
```

### 3. Subir o stack

```bash
docker compose -f docker-compose.vps.yml --env-file .env up -d
```

A primeira execucao vai:
- Buildar a imagem do Airflow (~10 min)
- Buildar a imagem do worker (~1 min)
- Baixar imagem do Nominatim (~500 MB)
- Iniciar Postgres metadata + MinIO
- Rodar `airflow-init` (cria DB + usuario admin)
- Subir Airflow webserver + scheduler
- **Nominatim inicia o import inicial** (download de ~3 GB de PBF Brasil + import no Postgres interno)
- O worker fica aguardando Nominatim ficar healthy

### 4. Acompanhar o import inicial do Nominatim (~6-12h)

```bash
# Status geral dos containers
docker compose -f docker-compose.vps.yml ps

# Log do Nominatim em tempo real
docker logs -f nominatim

# Status atual via HTTP (retorna texto entre 0 e 200000 = unidades de progresso)
curl http://localhost:8090/status
```

Durante o import:
- O container Nominatim aparece como `unhealthy` (esperado, e o `start_period: 12h`)
- O worker fica em loop "Nominatim ainda nao pronto, aguardando 30s"
- O resto do pipeline (Airflow, MinIO) funciona normalmente

Quando o import termina:
- `curl http://localhost:8090/status` retorna `OK`
- `docker compose ps` mostra Nominatim `healthy`
- Worker detecta e comeca a processar pendentes automaticamente

### 5. Acessar Airflow

http://IP_DA_VPS:8080

Login padrao: `admin` / valor de `AIRFLOW_ADMIN_PASSWORD` no `.env`.

> Sem dominio/SSL. Para producao seria recomendavel colocar um proxy reverso com Cloudflare ou Let's Encrypt na frente.

### 6. Liberar firewall (se aplicavel)

Se a VPS tem UFW/iptables, abra:
- 8080/tcp (Airflow webserver) — somente do seu IP, idealmente
- 9100/tcp e 9101/tcp (MinIO) — somente do seu IP
- 8090/tcp (Nominatim) — fechado externo, so acesso interno via Docker net

```bash
ufw allow from <SEU_IP> to any port 8080
ufw allow from <SEU_IP> to any port 9100
ufw allow from <SEU_IP> to any port 9101
```

## Operacao

### Pausar o worker durante o transform mensal

O DAG mensal usa Spark com 4GB de driver memory. Para evitar contencao com o Nominatim (que tambem usa muita RAM):

**Antes de disparar a DAG mensal:**
```bash
docker compose -f docker-compose.vps.yml stop geocoding-worker
```

**Depois que a DAG concluir:**
```bash
docker compose -f docker-compose.vps.yml start geocoding-worker
```

> Quando a DAG fica pesada raramente (1x por mes, 4-6h), essa pausa manual e suficiente. Pode ser automatizado no futuro com sensors do Airflow.

### Acompanhar progresso do worker

```bash
# Log em tempo real
docker logs -f geocoding-worker

# Stats agregadas (no DBeaver ou via clickhouse-client)
SELECT fonte, count() FROM empresas_ativas_do_brasil.cnpj_geolocalizacao FINAL GROUP BY fonte;
```

A linha `nominatim_local` deve crescer continuamente.

### Atualizar o codigo (DAG/scripts)

```bash
cd /opt/receita-pipeline-csv
git pull
# DAGs e scripts sao montados via bind mount, atualizam imediatamente
# Worker requer rebuild se mudar geocoding_worker.py:
cd vps
docker compose -f docker-compose.vps.yml build geocoding-worker
docker compose -f docker-compose.vps.yml up -d geocoding-worker
```

### Atualizar dados OSM do Nominatim (opcional)

OSM publica updates incrementais. Para pegar:

```bash
docker exec -it nominatim sudo -u nominatim nominatim replication --once
```

Recomendado: rodar mensalmente. Pode virar cron no futuro.

### Backup dos volumes

Volumes a salvar antes de qualquer manutencao destrutiva:
- `vps_nominatim_data` (~60 GB) — DB do Nominatim. Reproduzivel via reimport (~12h)
- `vps_postgres_airflow_data` (<2 GB) — historico de runs do Airflow
- `vps_minio_receita_storage` — depende do uso

```bash
docker run --rm -v vps_postgres_airflow_data:/data -v $(pwd):/backup busybox tar czf /backup/postgres-airflow-backup.tar.gz -C /data .
```

## Troubleshooting

### Nominatim trava no import

Logs detalhados:
```bash
docker logs nominatim --tail 200
```

Causas comuns:
- Disco encheu (verificar `df -h`)
- Download do PBF falhou (rede instavel) — restart resolve
- RAM insuficiente — aumentar swap ou reduzir THREADS

### Worker reporta "FLUSH inconsistente"

Significa que o ClickHouse esta com conexao instavel (zombie). O proprio worker reconecta e retenta automaticamente. Se persistir, verificar conectividade:

```bash
docker exec geocoding-worker curl -s --connect-timeout 5 http://$CLICKHOUSE_HOST:8123/ping
```

### Airflow webserver 500/502

```bash
docker logs airflow-webserver --tail 100
docker compose -f docker-compose.vps.yml restart airflow-webserver
```

### Reset completo (limpa tudo)

⚠️ Destrutivo. Vai apagar dados do Nominatim (12h de reimport), historico Airflow, MinIO local.

```bash
docker compose -f docker-compose.vps.yml down -v
docker compose -f docker-compose.vps.yml --env-file .env up -d
```

## Migrar de volta para a maquina local (rollback)

Se precisar voltar a rodar localmente:

1. Pare a VPS:
   ```bash
   docker compose -f docker-compose.vps.yml down
   ```
2. Na maquina local, use o `docker-compose.yaml` original (raiz do repo)
3. ClickHouse externo continua o mesmo, sem perda de dados

O `docker-compose.yaml` raiz **nao foi alterado** nesse setup, justamente para permitir rollback rapido.
