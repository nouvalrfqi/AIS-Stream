import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool

_pool: SimpleConnectionPool | None = None


def get_pool(config: dict) -> SimpleConnectionPool:
    global _pool
    if _pool is None:
        _pool = SimpleConnectionPool(1, 10, dsn=config["postgres_url"])
    return _pool


def fetch_all(config: dict, sql: str, params: tuple = ()) -> list[dict]:
    pool = get_pool(config)
    conn = pool.getconn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    except Exception:
        pool.putconn(conn, close=True)
        raise
    finally:
        if not conn.closed:
            pool.putconn(conn)


def ping(config: dict) -> bool:
    try:
        fetch_all(config, "SELECT 1")
        return True
    except Exception:
        return False


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None