import psycopg2


def connect(config: dict):
    return psycopg2.connect(config["postgres_url"])


def track_cleanup(conn, retention_hours: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM vessel_track WHERE event_time < now() - make_interval(hours => %s)",
            (retention_hours,),
        )
        return cur.rowcount


def mark_stale(conn, stale_minutes: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE vessel_current_state SET status = 'STALE' "
            "WHERE last_seen_at < now() - make_interval(mins => %s) AND status <> 'STALE'",
            (stale_minutes,),
        )
        return cur.rowcount


def purge_stale(conn, purge_after_hours: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM vessel_current_state WHERE last_seen_at < now() - make_interval(hours => %s)",
            (purge_after_hours,),
        )
        return cur.rowcount


def count_aging_tracks(conn, retention_hours: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM vessel_track WHERE event_time < now() - make_interval(hours => %s)",
            (retention_hours,),
        )
        return cur.fetchone()[0]


def count_stale_states(conn, stale_minutes: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM vessel_current_state WHERE last_seen_at < now() - make_interval(mins => %s)",
            (stale_minutes,),
        )
        return cur.fetchone()[0]


def count_purge_candidates(conn, purge_after_hours: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM vessel_current_state WHERE last_seen_at < now() - make_interval(hours => %s)",
            (purge_after_hours,),
        )
        return cur.fetchone()[0]