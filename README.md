# Receita Federal Data Pipeline

Pipeline de dados automatizado para download, processamento e analise de dados abertos da Receita Federal (CNPJ), utilizando Apache Airflow, Apache Spark, MinIO, PostgreSQL e ClickHouse.

## Indice

- [Visao Geral](#visao-geral)
- [Tecnologias](#tecnologias)
- [Arquitetura](#arquitetura)
- [Pre-requisitos](#pre-requisitos)
- [Instalacao](#instalacao)
- [Uso](#uso)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Funcionalidades](#funcionalidades)
- [Configuracao](#configuracao)
- [Migracao Airflow 3.x](#migracao-airflow-3x)

## Visao Geral

Este projeto implementa um pipeline completo de dados que:

1. **Detecta** automaticamente a versao mais recente dos dados abertos da Receita Federal
2. **Baixa** arquivos ZIP de empresas, estabelecimentos, socios e simples (31 downloads em paralelo)
3. **Extrai** e organiza os arquivos CSV
4. **Processa** os dados utilizando Apache Spark (limpeza, schema, merge)
5. **Ingere** no ClickHouse para consultas analiticas (opcional - graceful degradation)
6. **Armazena** no MinIO (Data Lake S3-compatible) e PostgreSQL (Data Warehouse)

## Tecnologias

| Tecnologia | Versao | Finalidade |
|---|---|---|
| **Apache Airflow** | 3.1.8 | Orquestracao de workflows |
| **Apache Spark** | 3.5.0 | Processamento distribuido de dados |
| **Delta Lake** | 3.0.0 | Formato de tabela transacional |
| **MinIO** | Latest | Data Lake (Storage S3) |
| **PostgreSQL** | 16 | Data Warehouse e Airflow Metadata |
| **ClickHouse** | Cloud/Self-hosted | Banco analitico (opcional) |
| **Docker** | Compose v2.14+ | Containerizacao |
| **Python** | 3.12+ | Scripts de processamento |
| **Java** | OpenJDK 17 | Runtime Spark |

### Bibliotecas Python

- `pyspark==3.5.0` - Processamento distribuido
- `delta-spark==3.0.0` - Formato Delta Lake
- `pandas` - Manipulacao de dados
- `requests` - Download de arquivos
- `minio` - Cliente MinIO
- `clickhouse-connect` - Integracao com ClickHouse
- `dbt-core` e `dbt-postgres` - Transformacao de dados
- `apache-airflow-providers-standard` - Operators padrao Airflow 3.x

## Arquitetura

```
                        Receita Federal
          https://arquivos.receitafederal.gov.br
                            |
                            | Download ZIP (31 arquivos)
                            v
+-----------------------------------------------------------+
|                    Apache Airflow 3.1.8                    |
|  +-----------------------------------------------------+  |
|  | DAG: receita_federal_csv_generator (TaskFlow API)    |  |
|  |  1. get_latest_date        (@task)                   |  |
|  |  2. download_and_extract   (@task, paralelo)         |  |
|  |  3. process_*              (Spark jobs)              |  |
|  |  4. load_*                 (ClickHouse, opcional)    |  |
|  +-----------------------------------------------------+  |
+-----------------------------------------------------------+
              |                |               |
    +---------+------+  +-----+------+  +-----+--------+
    |    MinIO       |  | PostgreSQL |  |  ClickHouse  |
    |   Data Lake    |  |     DW     |  |  Analitico   |
    |   (S3 API)     |  |            |  |  (opcional)  |
    +----------------+  +------------+  +--------------+
```

### Servicos Docker (Airflow 3.x)

| Servico | Descricao |
|---|---|
| `airflow-api-server` | UI + API REST (substitui webserver do 2.x) |
| `airflow-scheduler` | Agendamento e execucao de tasks |
| `airflow-dag-processor` | Processamento de DAGs (novo no 3.x, obrigatorio) |
| `airflow-init` | Migracao do banco e criacao do usuario admin |
| `postgres` | Metadata do Airflow + Data Warehouse |
| `minio` | Data Lake S3-compatible |

## Pre-requisitos

- **Docker** e **Docker Compose** v2.14+ instalados
- **4GB+ RAM** disponivel para containers
- **Espaco em disco**: ~50GB para processamento dos dados

## Instalacao

### 1. Clone o repositorio

```bash
git clone https://github.com/fujiiatila-dev/receita-pipeline-csv.git
cd receita-pipeline-csv
```

### 2. Crie a rede Docker

```bash
docker network create data-network
```

### 3. Configure variaveis de ambiente (opcional)

```bash
# UID do Airflow
export AIRFLOW_UID=$(id -u)

# ClickHouse (opcional - sem estas variaveis, pipeline para nos arquivos)
export CLICKHOUSE_HOST=meu-servidor.clickhouse.cloud
export CLICKHOUSE_PORT=8443
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=minha-senha
export CLICKHOUSE_DATABASE=receita
export CLICKHOUSE_SECURE=true
```

### 4. Inicie os containers

```bash
docker compose build
docker compose up -d
```

### 5. Aguarde inicializacao

```bash
docker compose logs -f
```

## Uso

### Acessar Interfaces

| Servico | URL | Credenciais |
|---|---|---|
| **Airflow UI** | http://localhost:8080 | admin / admin |
| **MinIO Console** | http://localhost:9010 | minioadmin / minioadmin |
| **PostgreSQL** | localhost:5432 | airflow / airflow |

### Executar o Pipeline

1. Acesse o Airflow em http://localhost:8080
2. Localize a DAG `receita_federal_csv_generator`
3. Ative a DAG (toggle)
4. Clique em "Trigger DAG" para executar manualmente

### Fluxo de Execucao

```
get_latest_date
      |
      v
download_Empresas0..9        download_Estabelecimentos0..9     ...
      |                              |
      v                              v
process_Empresas             process_Estabelecimentos           ...
      |                              |
      v                              v
load_Empresas (ClickHouse)   load_Estabelecimentos (ClickHouse) ...
```

- Se as credenciais do ClickHouse **nao estiverem configuradas**, as tasks `load_*` finalizam com sucesso sem fazer nada
- Se as credenciais **estiverem configuradas**, os CSVs processados sao carregados nas tabelas staging do ClickHouse

## Estrutura do Projeto

```
receita-pipeline-csv/
├── dags/
│   └── download_receita_federal.py     # DAG principal (TaskFlow API)
├── scripts/
│   ├── process_empresas.py             # Spark: processar empresas
│   ├── process_estabelecimentos.py     # Spark: processar estabelecimentos
│   ├── process_simples.py              # Spark: processar simples nacional
│   ├── process_socios.py               # Spark: processar socios
│   ├── spark_utils.py                  # Configuracao SparkSession + Delta + S3
│   ├── db_clickhouse.py                # Conexao ClickHouse (variaveis de ambiente)
│   └── load_to_clickhouse.py           # Ingestao CSVs no ClickHouse
├── data/                               # Dados baixados (gitignored)
│   ├── raw/                            # CSVs extraidos por periodo
│   └── output/                         # CSVs processados pelo Spark
├── logs/                               # Logs do Airflow (gitignored)
├── plugins/                            # Plugins Airflow (vazio)
├── Dockerfile                          # Imagem: Airflow 3.1.8 + Spark + Java
├── docker-compose.yaml                 # Orquestracao de containers
└── README.md
```

## Funcionalidades

### 1. Download Inteligente
- Detecta automaticamente a data mais recente dos dados
- Download paralelo de 31 arquivos via TaskFlow API
- Extracao automatica de ZIPs com renomeacao padronizada
- Skip de arquivos ja baixados (idempotente)

### 2. Processamento com Spark
- Processamento distribuido de grandes volumes
- Schema consistente para todos os tipos de dados
- Limpeza de dados (encoding, aspas, delimitadores)
- Integracao com MinIO (S3 API) e Delta Lake

### 3. Ingestao no ClickHouse (opcional)
- Carregamento automatico dos CSVs processados
- Graceful degradation: sem credenciais = sem erro
- Tabelas staging criadas automaticamente
- Insercao em batches de 50k linhas

### 4. Monitoramento
- Interface visual do Airflow (React UI no 3.x)
- Logs detalhados por task
- Retry automatico em caso de falhas

## Configuracao

### Variaveis de Ambiente

As configuracoes estao no `docker-compose.yaml`:

```yaml
# Airflow
AIRFLOW__CORE__EXECUTOR: LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow

# PostgreSQL (Data Warehouse)
PG_HOST: postgres
PG_USER: airflow
PG_PASS: airflow

# MinIO (Data Lake)
MINIO_ENDPOINT: http://minio:9000
MINIO_ACCESS_KEY: minioadmin
MINIO_SECRET_KEY: minioadmin

# ClickHouse (opcional)
CLICKHOUSE_HOST: ${CLICKHOUSE_HOST:-}
CLICKHOUSE_PORT: ${CLICKHOUSE_PORT:-8443}
CLICKHOUSE_USER: ${CLICKHOUSE_USER:-}
CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD:-}
CLICKHOUSE_DATABASE: ${CLICKHOUSE_DATABASE:-datahealth}
CLICKHOUSE_SECURE: ${CLICKHOUSE_SECURE:-true}
```

### Customizacao

Para modificar credenciais ou configuracoes:

```bash
docker compose down
# Editar docker-compose.yaml ou exportar variaveis
docker compose build
docker compose up -d
```

## Migracao Airflow 3.x

Este projeto foi migrado de Airflow 2.9.0 para 3.1.8. Principais mudancas:

### Infraestrutura

| Aspecto | Airflow 2.9 | Airflow 3.1 |
|---|---|---|
| Webserver | `airflow webserver` | `airflow api-server` (FastAPI + React) |
| DAG Processor | Inline no scheduler | Servico separado obrigatorio |
| PostgreSQL | 13 | 16 |
| Config key | `AIRFLOW__CORE__SQL_ALCHEMY_CONN` | `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` |

### DAG / Codigo

| Aspecto | Airflow 2.9 | Airflow 3.1 |
|---|---|---|
| Imports | `from airflow.models import DAG` | `from airflow.sdk import DAG, dag, task` |
| Operators | `from airflow.operators.python` | `from airflow.providers.standard.operators.python` |
| DAG syntax | `with DAG(...) as dag:` | `@dag` decorator (TaskFlow API) |
| Tasks | `PythonOperator(callable=fn)` | `@task` decorator |
| XCom | Pickling suportado | Apenas JSON serializavel |
| Schedule default | `timedelta(days=1)` | `None` |
| Catchup default | `True` | `False` |

### Novos recursos utilizados

- **TaskFlow API**: `@dag` e `@task` decorators para codigo mais limpo
- **Passagem de dados**: retorno de funcao `@task` = XCom automatico (JSON)
- **React UI**: nova interface mais rapida e moderna
- **DAG Versioning**: DAG runs completam na versao que iniciaram

## Tipos de Dados Processados

| Tipo | Arquivos | Descricao |
|---|---|---|
| **Empresas** | Empresas0-9.zip | Dados cadastrais das empresas (7 colunas) |
| **Estabelecimentos** | Estabelecimentos0-9.zip | Filiais e localizacoes (30 colunas) |
| **Socios** | Socios0-9.zip | Quadro societario (11 colunas) |
| **Simples** | Simples.zip | Optantes do Simples Nacional (7 colunas) |

## Troubleshooting

### Erro "network data-network not found"

```bash
docker network create data-network
```

### Containers nao sobem

```bash
docker compose down -v
docker compose build
docker compose up -d
```

### Falta de memoria

Aumente recursos do Docker:
- Docker Desktop > Settings > Resources
- Minimo recomendado: 4GB RAM, 4 CPUs

### DAG nao aparece na UI

Verifique se o `airflow-dag-processor` esta rodando:
```bash
docker compose logs airflow-dag-processor
```

### Erro de importacao (airflow.sdk)

Certifique-se de que a imagem foi reconstruida com Airflow 3.1.8:
```bash
docker compose build --no-cache
```

## Licenca

Este projeto e de codigo aberto e esta disponivel para uso educacional e comercial.

## Contribuindo

1. Fork o projeto
2. Crie uma branch (`git checkout -b feature/nova-feature`)
3. Commit suas mudancas (`git commit -m 'feat: descricao'`)
4. Push para a branch (`git push origin feature/nova-feature`)
5. Abra um Pull Request

---

Desenvolvido para facilitar o acesso aos dados abertos da Receita Federal.
