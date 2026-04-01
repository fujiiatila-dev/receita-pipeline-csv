"""Modulo de conexao com ClickHouse via variaveis de ambiente."""
import os


def get_clickhouse_client():
    """Cria e retorna um cliente ClickHouse.
    Retorna None se as credenciais nao estiverem configuradas.
    """
    host = os.environ.get("CLICKHOUSE_HOST")
    port = os.environ.get("CLICKHOUSE_PORT", "8443")
    user = os.environ.get("CLICKHOUSE_USER")
    password = os.environ.get("CLICKHOUSE_PASSWORD", "")
    database = os.environ.get("CLICKHOUSE_DATABASE", "empresas_ativas_do_brasil")
    secure = os.environ.get("CLICKHOUSE_SECURE", "true").lower() == "true"

    if not host or not user:
        print(
            "[ClickHouse] Credenciais nao configuradas (CLICKHOUSE_HOST e CLICKHOUSE_USER obrigatorios). "
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
