"""Modulo de conexao com ClickHouse.

Ordem de resolucao das credenciais:
1. Airflow Connection (conn_id em CLICKHOUSE_CONN_ID, padrao 'clickhouse_default'),
   quando rodando sob Airflow e a connection existir.
2. Variaveis de ambiente CLICKHOUSE_* (fallback / execucao standalone).
"""
import os

DEFAULT_CONN_ID = os.environ.get("CLICKHOUSE_CONN_ID", "clickhouse_default")


def _params_from_airflow_connection():
    """Le os parametros de uma Airflow Connection, se disponivel.

    Le direto do metadata DB via ORM: funciona tanto dentro de uma task quanto
    em execucao standalone (spark-submit / python), sem depender do contexto de
    execucao do Airflow 3. Retorna um dict de parametros ou None.
    """
    try:
        from airflow.models import Connection
        from airflow.settings import Session

        with Session() as session:
            conn = (
                session.query(Connection)
                .filter(Connection.conn_id == DEFAULT_CONN_ID)
                .first()
            )
    except Exception:
        return None

    if conn is None or not conn.host:
        return None

    extra = {}
    try:
        extra = conn.extra_dejson or {}
    except Exception:
        extra = {}

    secure = extra.get("secure")
    if secure is None:
        secure = os.environ.get("CLICKHOUSE_SECURE", "true").lower() == "true"

    return {
        "host": conn.host,
        "port": int(conn.port or extra.get("port") or 8443),
        "user": conn.login,
        "password": conn.password or "",
        "database": conn.schema or extra.get("database") or "empresas_ativas_do_brasil",
        "secure": bool(secure),
        "source": f"Airflow Connection '{DEFAULT_CONN_ID}'",
    }


def _params_from_env():
    """Le os parametros das variaveis de ambiente CLICKHOUSE_*. Retorna dict ou None."""
    host = os.environ.get("CLICKHOUSE_HOST")
    user = os.environ.get("CLICKHOUSE_USER")
    if not host or not user:
        return None

    return {
        "host": host,
        "port": int(os.environ.get("CLICKHOUSE_PORT", "8443")),
        "user": user,
        "password": os.environ.get("CLICKHOUSE_PASSWORD", ""),
        "database": os.environ.get("CLICKHOUSE_DATABASE", "empresas_ativas_do_brasil"),
        "secure": os.environ.get("CLICKHOUSE_SECURE", "true").lower() == "true",
        "source": "variaveis de ambiente CLICKHOUSE_*",
    }


def get_clickhouse_client():
    """Cria e retorna um cliente ClickHouse.

    Retorna None se as credenciais nao estiverem configuradas.
    """
    params = _params_from_airflow_connection() or _params_from_env()

    if not params or not params.get("host") or not params.get("user"):
        print(
            "[ClickHouse] Credenciais nao configuradas. "
            f"Defina a Airflow Connection '{DEFAULT_CONN_ID}' "
            "ou as env CLICKHOUSE_HOST/CLICKHOUSE_USER. Pulando etapas de banco."
        )
        return None

    try:
        import clickhouse_connect

        client = clickhouse_connect.get_client(
            host=params["host"],
            port=params["port"],
            username=params["user"],
            password=params["password"],
            database=params["database"],
            secure=params["secure"],
        )
        client.query("SELECT 1")
        print(
            f"[ClickHouse] Conexao estabelecida via {params['source']} "
            f"({params['host']}:{params['port']}/{params['database']})"
        )
        return client
    except ImportError:
        print("[ClickHouse] clickhouse-connect nao instalado.")
        return None
    except Exception as e:
        print(f"[ClickHouse] Falha ao conectar: {e}")
        return None
