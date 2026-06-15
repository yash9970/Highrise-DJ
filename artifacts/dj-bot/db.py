import os
import time

# In-memory storage to replace Postgres
_queue = []
_next_id = 1
_bounds = None


def init_db():
    print("[DB] In-memory storage ready.")


def add_song(song_name: str, requested_by: str) -> int:
    global _next_id
    song_id = _next_id
    _next_id += 1
    _queue.append({
        "id": song_id,
        "song_name": song_name,
        "requested_by": requested_by,
        "added_at": time.time()
    })
    return song_id


def get_queue() -> list:
    return list(_queue)


def get_next_song() -> dict | None:
    if not _queue:
        return None
    return _queue[0]


def delete_song(song_id: int):
    global _queue
    _queue = [s for s in _queue if s["id"] != song_id]


def clear_queue():
    global _queue
    _queue.clear()


def set_floor_corner(corner: int, x: float, y: float, z: float):
    global _bounds
    if _bounds is None:
        _bounds = {"x1": None, "y1": None, "z1": None, "x2": None, "y2": None, "z2": None}
    
    if corner == 1:
        _bounds["x1"], _bounds["y1"], _bounds["z1"] = x, y, z
    elif corner == 2:
        _bounds["x2"], _bounds["y2"], _bounds["z2"] = x, y, z


def get_floor_bounds() -> dict | None:
    if not _bounds or _bounds["x1"] is None or _bounds["x2"] is None:
        return None
        
    return {
        "min_x": min(_bounds["x1"], _bounds["x2"]),
        "max_x": max(_bounds["x1"], _bounds["x2"]),
        "min_y": min(_bounds["y1"], _bounds["y2"]) - 0.5,
        "max_y": max(_bounds["y1"], _bounds["y2"]) + 5.0,
        "min_z": min(_bounds["z1"], _bounds["z2"]),
        "max_z": max(_bounds["z1"], _bounds["z2"]),
    }


def clear_floor_bounds():
    global _bounds
    _bounds = None
