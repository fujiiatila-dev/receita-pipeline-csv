"""Módulo de conexão com ClickHouse - mesmo padrão do Data Health."""
import os


def get_clickhouse_client():
    """Cria e retorna um cliente ClickHouse usando variáveis de ambiente.
    Retorna None se as credenciais não estiverem configuradas ou a conexão falhar.
    """
    host = os.environ.get("CLICKHOUSE_HOST")
    port = os.environ.get("CLICKHOUSE_PORT", "8443")
    user = os.environ.get("CLICKHOUSE_USER")
    password = os.environ.get("CLICKHOUSE_PASSWORD")
    database = os.environ.get("CLICKHOUSE_DATABASE", "datahealth")
    secure = os.environ.get("CLICKHOUSE_SECURE", "true").lower() == "true"

    if not host or not user or not password:
        print(
            "[ClickHouse] Credenciais nao configuradas "
            "(CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD). "
            "Pulando etapas de banco."
        )
        return None

    try:
        import clickhouse_connect

        client = clickhouse_connect.get_client(
            host=host,
            port=int(port),
            username=user,
            password=password,
            database=database,
            secure=secure,
        )
        client.query("SELECT 1")
        print(f"[ClickHouse] Conexao estabelecida ({host}:{port}/{database})")
        return client
    except ImportError:
        print("[ClickHouse] clickhouse-connect nao instalado.")
        return None
    except Exception as e:
        print(f"[ClickHouse] Falha ao conectar: {e}")
        return None
