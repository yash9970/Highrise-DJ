import os
import psycopg2
import psycopg2.extras
from contextlib import contextmanager

DATABASE_URL = os.environ.get("DATABASE_URL")


def get_connection():
    return psycopg2.connect(DATABASE_URL)


@contextmanager
def db_cursor():
    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with db_cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS song_queue (
                id SERIAL PRIMARY KEY,
                song_name TEXT NOT NULL,
                requested_by TEXT NOT NULL,
                added_at TIMESTAMP DEFAULT NOW()
            )
        """)
    print("[DB] song_queue table ready.")


def add_song(song_name: str, requested_by: str) -> int:
    with db_cursor() as cur:
        cur.execute(
            "INSERT INTO song_queue (song_name, requested_by) VALUES (%s, %s) RETURNING id",
            (song_name, requested_by)
        )
        row = cur.fetchone()
        return row["id"]


def get_queue() -> list:
    with db_cursor() as cur:
        cur.execute("SELECT * FROM song_queue ORDER BY added_at ASC")
        return cur.fetchall()


def get_next_song() -> dict | None:
    with db_cursor() as cur:
        cur.execute("SELECT * FROM song_queue ORDER BY added_at ASC LIMIT 1")
        return cur.fetchone()


def delete_song(song_id: int):
    with db_cursor() as cur:
        cur.execute("DELETE FROM song_queue WHERE id = %s", (song_id,))


def clear_queue():
    with db_cursor() as cur:
        cur.execute("DELETE FROM song_queue")
