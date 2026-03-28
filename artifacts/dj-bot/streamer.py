"""
streamer.py — Audio Broadcaster for Highrise DJ Bot

Stream pipeline:
  yt-dlp (search + resolve URL)
    → yt-dlp (download audio to OS pipe)
    → ffmpeg (re-encode to 128k MP3)
    → broadcast to all connected HTTP clients

Search strategy (waterfall):
  1. SoundCloud  — works on datacenter IPs, large catalog
  2. YouTube     — fallback with multi-client emulation

Key design decisions:
  - Title check uses `yt-dlp --print title --print webpage_url --skip-download`
    which returns BOTH fields in one call. The stream subprocess then uses the
    direct URL — guaranteeing announced title == audio played (no second search).
  - OS-level pipe (os.pipe()) connects yt-dlp → ffmpeg instead of asyncio
    StreamReader, which doesn't have .fileno() and crashes subprocess creation.
  - Global semaphore limits concurrent searches to 1, preventing zombie
    yt-dlp processes during reconnect storms.
  - _is_searching flag: True from search-start to stream-end, so callers can
    check `broadcaster.is_active` even during the search phase.
  - _interrupted flag: set by stop_current() when called externally; lets the
    song loop skip "✅ Done" messages for manually-stopped songs.
"""

import asyncio
import os
from typing import Optional, Callable, Awaitable


# ─────────────────────────────────────────────────────────────────────────────
# Sources — tried in order, first success wins
# ─────────────────────────────────────────────────────────────────────────────
SOURCES = [
    {
        "label": "SoundCloud",
        "query": "scsearch1:{song}",
        "extra": [],
    },
    {
        "label": "YouTube",
        "query": "ytsearch1:{song}",
        "extra": [
            "--extractor-args", "youtube:player_client=tv_embedded,ios,mweb",
        ],
    },
]

TITLE_TIMEOUT = 30  # seconds before giving up on a source

# One search at a time — prevents zombie yt-dlp pile-up during reconnect storms
_search_sem: Optional[asyncio.Semaphore] = None


def _get_search_sem() -> asyncio.Semaphore:
    global _search_sem
    if _search_sem is None:
        _search_sem = asyncio.Semaphore(1)
    return _search_sem


# ─────────────────────────────────────────────────────────────────────────────
# AudioBroadcaster
# ─────────────────────────────────────────────────────────────────────────────

class AudioBroadcaster:
    """
    Internet-radio-style broadcaster: yt-dlp → ffmpeg → HTTP chunked MP3.

    State flags:
      _is_searching : True while _find_source() is running
      _is_streaming : True while ffmpeg is broadcasting audio
      _interrupted  : True if stop_current() was called externally mid-play;
                      lets the song loop skip the "Done" announcement
    """

    def __init__(self):
        self._client_queues: dict[int, asyncio.Queue] = {}
        self._next_id = 0
        self._lock = asyncio.Lock()
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._ytdlp_proc: Optional[asyncio.subprocess.Process] = None

        self._is_searching = False
        self._is_streaming = False
        self._interrupted = False
        self._current_title = ""
        self._current_source = ""

    # ── Public state ──────────────────────────────────────────────────────────

    @property
    def is_active(self) -> bool:
        """True from search-start to stream-end (use this instead of is_playing)."""
        return self._is_searching or self._is_streaming

    @property
    def is_playing(self) -> bool:
        """True only while audio is actually streaming (not during search)."""
        return self._is_streaming

    @property
    def was_interrupted(self) -> bool:
        return self._interrupted

    @property
    def listener_count(self) -> int:
        return len(self._client_queues)

    @property
    def current_title(self) -> str:
        return self._current_title

    @property
    def current_source(self) -> str:
        return self._current_source

    # ── HTTP Streaming ────────────────────────────────────────────────────────

    async def stream_to_client(self, request):
        from aiohttp import web

        response = web.StreamResponse(
            headers={
                "Content-Type": "audio/mpeg",
                "Cache-Control": "no-cache, no-store",
                "icy-name": "Highrise DJ Bot Radio",
                "icy-genre": "Various",
                "Connection": "keep-alive",
                "Transfer-Encoding": "chunked",
            }
        )
        await response.prepare(request)

        client_id = self._next_id
        self._next_id += 1
        queue: asyncio.Queue = asyncio.Queue(maxsize=200)

        async with self._lock:
            self._client_queues[client_id] = queue

        print(f"[RADIO] Listener {client_id} connected — total: {len(self._client_queues)}")

        try:
            while True:
                try:
                    chunk = await asyncio.wait_for(queue.get(), timeout=90)
                    if chunk is None:
                        break
                    await response.write(chunk)
                except asyncio.TimeoutError:
                    break
        except Exception as e:
            print(f"[RADIO] Listener {client_id} error: {e}")
        finally:
            async with self._lock:
                self._client_queues.pop(client_id, None)
            print(f"[RADIO] Listener {client_id} left — total: {len(self._client_queues)}")

        return response

    async def _broadcast(self, chunk: bytes):
        async with self._lock:
            queues = list(self._client_queues.values())
        for q in queues:
            try:
                q.put_nowait(chunk)
            except asyncio.QueueFull:
                pass

    # ── Playback Control ──────────────────────────────────────────────────────

    async def stop_current(self, interrupted: bool = True):
        """
        Kill any running ffmpeg and yt-dlp processes.

        interrupted=True (default): marks the stop as external so the song loop
        skips the "Done" announcement for a manually-skipped song.
        """
        if interrupted:
            self._interrupted = True

        for attr in ("_proc", "_ytdlp_proc"):
            p = getattr(self, attr)
            if p and p.returncode is None:
                try:
                    p.kill()
                    await p.wait()
                except Exception:
                    pass
            setattr(self, attr, None)

        self._is_streaming = False
        self._is_searching = False
        self._current_title = ""
        self._current_source = ""

    async def play(
        self,
        song_name: str,
        on_found: Optional[Callable[[str, str], Awaitable[None]]] = None,
    ) -> tuple[bool, str]:
        """
        Search for song_name and stream it. Returns (success, title).

        on_found(title, source_label): called the instant search succeeds and
        streaming is about to begin — ideal for "Now playing" announcements.
        """
        # Reset interrupted flag for this new play() call
        self._interrupted = False
        await self.stop_current(interrupted=False)

        # ── Search ────────────────────────────────────────────────────────────
        self._is_searching = True
        try:
            found = await _find_source(song_name)
        finally:
            self._is_searching = False

        if not found:
            print(f"[RADIO] '{song_name}' — not found on any source.")
            return False, song_name

        title, direct_url, extra_args, source_label = found
        print(f"[RADIO] '{title}' found via {source_label} — starting stream...")

        self._current_title = title
        self._current_source = source_label

        # Fire the "Now playing" callback before we start the subprocess
        if on_found:
            try:
                await on_found(title, source_label)
            except Exception as e:
                print(f"[RADIO] on_found error: {e}")

        # ── Stream ────────────────────────────────────────────────────────────
        self._is_streaming = True
        try:
            # OS pipe: gives real fds with .fileno() — asyncio StreamReader
            # cannot be passed as stdin to another subprocess.
            pipe_read_fd, pipe_write_fd = os.pipe()

            try:
                ytdlp_proc = await asyncio.create_subprocess_exec(
                    "yt-dlp", "-o", "-", "-q", "--no-warnings", "--no-playlist",
                    "-f", "bestaudio/best", *extra_args, direct_url,
                    stdout=pipe_write_fd,
                    stderr=asyncio.subprocess.PIPE,
                )
            finally:
                os.close(pipe_write_fd)  # parent closes its copy
            self._ytdlp_proc = ytdlp_proc

            try:
                ffmpeg_proc = await asyncio.create_subprocess_exec(
                    "ffmpeg", "-re", "-i", "pipe:0", "-vn",
                    "-acodec", "libmp3lame", "-ab", "128k", "-ar", "44100",
                    "-f", "mp3", "-loglevel", "error", "pipe:1",
                    stdin=pipe_read_fd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
            finally:
                os.close(pipe_read_fd)  # parent closes its copy
            self._proc = ffmpeg_proc

            chunks_sent = 0
            while True:
                chunk = await ffmpeg_proc.stdout.read(8192)
                if not chunk:
                    break
                await self._broadcast(chunk)
                chunks_sent += 1

            await ffmpeg_proc.wait()

            if ffmpeg_proc.returncode not in (0, None, -9):
                err = await ffmpeg_proc.stderr.read(2048)
                print(f"[FFMPEG ERR] {err.decode(errors='replace').strip()}")

            try:
                if ytdlp_proc.returncode is None:
                    ytdlp_proc.kill()
                    await ytdlp_proc.wait()
                elif ytdlp_proc.returncode not in (0, -9):
                    err = await ytdlp_proc.stderr.read(2048)
                    print(f"[YTDLP ERR] {err.decode(errors='replace').strip()}")
            except Exception:
                pass

            if chunks_sent == 0:
                print(f"[RADIO] 0 bytes for '{title}' — stream failed or was killed before data.")
                return False, title

            print(f"[RADIO] Done: '{title}' [{source_label}] — {chunks_sent} chunks.")
            return True, title

        except asyncio.CancelledError:
            await self.stop_current(interrupted=True)
            raise
        except Exception as e:
            print(f"[RADIO] Playback error for '{song_name}': {e}")
            return False, song_name
        finally:
            self._is_streaming = False
            self._proc = None
            self._ytdlp_proc = None
            self._current_title = ""
            self._current_source = ""


# ─────────────────────────────────────────────────────────────────────────────
# Source resolution
# ─────────────────────────────────────────────────────────────────────────────

async def _find_source(song_name: str) -> Optional[tuple[str, str, list, str]]:
    """
    Returns (title, direct_url, extra_args, source_label) or None.
    Semaphore-protected: only 1 search runs at a time.
    """
    sem = _get_search_sem()
    async with sem:
        for source in SOURCES:
            label = source["label"]
            query = source["query"].format(song=song_name)
            extra = source["extra"]

            print(f"[SEARCH] [{label}] Checking: {song_name!r}...")
            result = await _get_title_and_url(query, extra, label)
            if result:
                title, url = result
                print(f"[SEARCH] [{label}] Found: {title!r} → {url}")
                return title, url, extra, label
            else:
                print(f"[SEARCH] [{label}] Not found.")

    return None


async def _get_title_and_url(
    query: str, extra_args: list, label: str
) -> Optional[tuple[str, str]]:
    """
    Single yt-dlp call that returns BOTH %(title)s AND %(webpage_url)s.
    Using the direct URL for streaming guarantees title == audio.
    Kills subprocess on CancelledError to prevent zombies.
    """
    cmd = [
        "yt-dlp",
        "--skip-download",
        "--print", "%(title)s",
        "--print", "%(webpage_url)s",
        "-q", "--no-warnings", "--no-playlist",
        *extra_args,
        query,
    ]
    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=TITLE_TIMEOUT)
        except asyncio.TimeoutError:
            _kill_proc(proc)
            print(f"[SEARCH] [{label}] Timed out.")
            return None
        except asyncio.CancelledError:
            _kill_proc(proc)
            raise  # propagate so song loop exits cleanly

        if proc.returncode != 0:
            err = stderr.decode(errors="replace").strip()
            if "ERROR" in err:
                print(f"[SEARCH] [{label}] {err[:200]}")
            return None

        lines = stdout.decode(errors="replace").strip().split("\n")
        if len(lines) < 2:
            return None
        title = lines[0].strip()
        url = lines[1].strip()
        return (title, url) if title and url and url != "NA" else None

    except FileNotFoundError:
        print("[SEARCH] ERROR: yt-dlp not in PATH.")
        return None
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"[SEARCH] [{label}] Error: {e}")
        return None


def _kill_proc(proc) -> None:
    try:
        if proc and proc.returncode is None:
            proc.kill()
    except Exception:
        pass


broadcaster = AudioBroadcaster()
