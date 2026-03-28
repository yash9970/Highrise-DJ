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
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dancefloor_bounds (
                id INTEGER PRIMARY KEY,
                x1 FLOAT, y1 FLOAT, z1 FLOAT,
                x2 FLOAT, y2 FLOAT, z2 FLOAT
            )
        """)
    print("[DB] Tables ready.")


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


def set_floor_corner(corner: int, x: float, y: float, z: float):
    """
    Saves corner 1 or 2. If the row doesn't exist, creates it.
    """
    with db_cursor() as cur:
        # First ensure row exists
        cur.execute("INSERT INTO dancefloor_bounds (id) VALUES (1) ON CONFLICT (id) DO NOTHING")
        if corner == 1:
            cur.execute("UPDATE dancefloor_bounds SET x1=%s, y1=%s, z1=%s WHERE id=1", (x, y, z))
        elif corner == 2:
            cur.execute("UPDATE dancefloor_bounds SET x2=%s, y2=%s, z2=%s WHERE id=1", (x, y, z))


def get_floor_bounds() -> dict | None:
    """
    Returns {'min_x', 'max_x', 'min_y', 'max_y', 'min_z', 'max_z'}
    if both corners are set, else None.
    """
    with db_cursor() as cur:
        cur.execute("SELECT * FROM dancefloor_bounds WHERE id=1")
        row = cur.fetchone()
        
    if not row or row["x1"] is None or row["x2"] is None:
        return None
        
    return {
        "min_x": min(row["x1"], row["x2"]),
        "max_x": max(row["x1"], row["x2"]),
        "min_y": min(row["y1"], row["y2"]) - 0.5, # Small leniency for Y
        "max_y": max(row["y1"], row["y2"]) + 5.0, # Leniency for jumping
        "min_z": min(row["z1"], row["z2"]),
        "max_z": max(row["z1"], row["z2"]),
    }


def clear_floor_bounds():
    with db_cursor() as cur:
        cur.execute("DELETE FROM dancefloor_bounds WHERE id=1")


def delete_song(song_id: int):
    with db_cursor() as cur:
        cur.execute("DELETE FROM song_queue WHERE id = %s", (song_id,))


def clear_queue():
    with db_cursor() as cur:
        cur.execute("DELETE FROM song_queue")
