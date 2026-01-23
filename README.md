# 🏭 Receita Federal Data Pipeline

Pipeline de dados automatizado para download, processamento e análise de dados abertos da Receita Federal (CNPJ), utilizando Apache Airflow, Apache Spark, MinIO e PostgreSQL.

## 📋 Índice

- [Visão Geral](#visão-geral)
- [Tecnologias](#tecnologias)
- [Arquitetura](#arquitetura)
- [Pré-requisitos](#pré-requisitos)
- [Instalação](#instalação)
- [Uso](#uso)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Funcionalidades](#funcionalidades)
- [Configuração](#configuração)

## 🎯 Visão Geral

Este projeto implementa um pipeline completo de dados que:

1. **Detecta** automaticamente a versão mais recente dos dados abertos da Receita Federal
2. **Baixa** arquivos ZIP de empresas, estabelecimentos, sócios e simples
3. **Extrai** e organiza os arquivos CSV
4. **Processa** os dados utilizando Apache Spark
5. **Armazena** no MinIO (Data Lake S3-compatible) e PostgreSQL (Data Warehouse)

## 🛠️ Tecnologias

| Tecnologia | Versão | Finalidade |
|-----------|---------|-----------|
| **Apache Airflow** | 2.9.0 | Orquestração de workflows |
| **Apache Spark** | 3.5.0 | Processamento distribuído de dados |
| **MinIO** | Latest | Data Lake (Storage S3) |
| **PostgreSQL** | 13 | Data Warehouse e Airflow Metadata |
| **Docker** | - | Containerização |
| **Python** | 3.x | Scripts de processamento |

### Bibliotecas Python Principais

- `pyspark==3.5.0` - Processamento distribuído
- `delta-spark==3.0.0` - Formato Delta Lake
- `pandas` - Manipulação de dados
- `requests` - Download de arquivos
- `minio` - Cliente MinIO
- `dbt-core` e `dbt-postgres` - Transformação de dados

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────────┐
│                     Receita Federal                         │
│         https://arquivos.receitafederal.gov.br              │
└────────────────────────┬────────────────────────────────────┘
                         │
                         │ Download ZIP
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   Apache Airflow                            │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ DAG: receita_federal_csv_generator                   │   │
│  │  1. get_latest_date                                  │   │
│  │  2. download_* (parallel)                            │   │
│  │  3. process_* (Spark jobs)                           │   │
│  └──────────────────────────────────────────────────────┘   │
└────────────────────────┬────────────────────────────────────┘
                         │
            ┌────────────┴────────────┐
            ▼                         ▼
    ┌──────────────┐          ┌──────────────┐
    │    MinIO     │          │  PostgreSQL  │
    │  Data Lake   │          │ Data Warehouse│
    │  (S3 API)    │          │              │
    └──────────────┘          └──────────────┘
```

## 📦 Pré-requisitos

- **Docker** e **Docker Compose** instalados
- **4GB+ RAM** disponível para containers
- **Espaço em disco**: ~50GB para processamento dos dados

## 🚀 Instalação

### 1. Clone o repositório

```bash
git clone <url-do-repositorio>
cd receita-pipeline V2
```

### 2. Crie a rede Docker

```bash
docker network create data-network
```

### 3. Configure variáveis de ambiente (opcional)

```bash
export AIRFLOW_UID=$(id -u)
```

### 4. Inicie os containers

```bash
docker-compose up -d
```

### 5. Aguarde inicialização

O processo completo pode levar alguns minutos. Acompanhe com:

```bash
docker-compose logs -f
```

## 💻 Uso

### Acessar Interfaces

| Serviço | URL | Credenciais |
|---------|-----|------------|
| **Airflow Web UI** | http://localhost:8080 | admin / admin |
| **MinIO Console** | http://localhost:9010 | minioadmin / minioadmin |
| **PostgreSQL** | localhost:5432 | airflow / airflow |

### Executar o Pipeline

1. Acesse o Airflow em http://localhost:8080
2. Localize a DAG `receita_federal_csv_generator`
3. Ative a DAG (toggle)
4. Clique em "Trigger DAG" para executar manualmente

### Monitorar Execução

- Acompanhe o progresso na interface do Airflow
- Verifique logs individuais de cada task
- Monitore recursos no MinIO Console

## 📁 Estrutura do Projeto

```
receita-pipeline V2/
├── dags/
│   └── download_receita_federal.py    # DAG principal do Airflow
├── scripts/
│   ├── get_latest_date.py             # Detectar versão mais recente
│   ├── process_empresas.py            # Processar dados de empresas
│   ├── process_estabelecimentos.py    # Processar estabelecimentos
│   ├── process_simples.py             # Processar simples nacional
│   ├── process_socios.py              # Processar sócios
│   └── spark_utils.py                 # Utilitários Spark
├── data/                              # Dados baixados (gitignored)
│   └── raw/                           # CSVs extraídos
├── logs/                              # Logs do Airflow (gitignored)
├── plugins/                           # Plugins customizados do Airflow
├── Dockerfile                         # Imagem customizada do Airflow
├── docker-compose.yaml                # Orquestração de containers
└── README.md                          # Este arquivo
```

## ⚙️ Funcionalidades

### 1. Download Inteligente

- ✅ Detecta automaticamente a data mais recente dos dados
- ✅ Download paralelo de múltiplos arquivos
- ✅ Extração automática de ZIPs
- ✅ Renomeação padronizada de arquivos

### 2. Processamento com Spark

- ✅ Processamento distribuído de grandes volumes
- ✅ Schema consistente para todos os tipos de dados
- ✅ Integração com MinIO (S3 API)
- ✅ Suporte a Delta Lake

### 3. Monitoramento e Logs

- ✅ Interface visual do Airflow
- ✅ Logs detalhados por task
- ✅ Retry automático em caso de falhas
- ✅ Alertas de execução

## 🔧 Configuração

### Variáveis de Ambiente

As principais configurações estão no `docker-compose.yaml`:

```yaml
# Airflow
AIRFLOW__CORE__EXECUTOR: LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES: 'false'

# PostgreSQL (Data Warehouse)
PG_HOST: postgres
PG_USER: airflow
PG_PASS: airflow

# MinIO (Data Lake)
MINIO_ENDPOINT: http://minio:9000
MINIO_ACCESS_KEY: minioadmin
MINIO_SECRET_KEY: minioadmin

# Spark
JAVA_HOME: /usr/lib/jvm/java-17-openjdk-amd64
SPARK_HOME: /opt/airflow/spark
```

### Customização

Para modificar credenciais ou configurações:

1. Edite `docker-compose.yaml`
2. Recrie os containers:

```bash
docker-compose down
docker-compose up -d
```

## 📊 Tipos de Dados Processados

| Tipo | Arquivos | Descrição |
|------|----------|-----------|
| **Empresas** | Empresas0-9.zip | Dados cadastrais das empresas |
| **Estabelecimentos** | Estabelecimentos0-9.zip | Filiais e localizações |
| **Sócios** | Socios0-9.zip | Quadro societário |
| **Simples** | Simples.zip | Optantes do Simples Nacional |

## 🐛 Troubleshooting

### Erro "network data-network not found"

```bash
docker network create data-network
```

### Containers não sobem

```bash
docker-compose down -v
docker-compose up -d
```

### Falta de memória

Aumente recursos do Docker:
- Docker Desktop → Settings → Resources
- Mínimo recomendado: 4GB RAM

## 📝 Licença

Este projeto é de código aberto e está disponível para uso educacional e comercial.

## 👥 Contribuindo

Contribuições são bem-vindas! Para contribuir:

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📮 Contato

Para dúvidas ou sugestões, abra uma issue no repositório.

---

**Desenvolvido com ❤️ para facilitar o acesso aos dados abertos da Receita Federal**
