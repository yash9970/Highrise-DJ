"""
streamer.py — Audio Broadcaster for Highrise DJ Bot

Stream pipeline:
  yt-dlp (search + download to stdout)
    → ffmpeg (re-encode to 128k MP3)
    → broadcast to all connected HTTP listeners

Search strategy (in order):
  1. SoundCloud  — Works well on server IPs, large catalog
  2. YouTube     — Fallback, tries multiple player clients

Key design:
  - Title resolution uses `yt-dlp --print title --skip-download` subprocess
    (same binary/path as streaming, so if title check works, streaming WILL work)
  - No Python yt-dlp API used (avoids stale client_id cache, thread issues)
  - 30s timeout on title resolution per source
  - ffmpeg reconnect flags for resilient streaming
"""

import asyncio
import os
import json
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# Source definitions — tried in order, first success wins
# ─────────────────────────────────────────────────────────────────────────────
SOURCES = [
    {
        "label": "SoundCloud",
        "query": "scsearch1:{song}",
        "extra": [],                    # SoundCloud needs no extra flags
    },
    {
        "label": "YouTube",
        "query": "ytsearch1:{song}",
        "extra": [                      # Try multiple clients in sequence
            "--extractor-args", "youtube:player_client=tv_embedded,ios,mweb",
        ],
    },
]

# Lofi fallback: use a direct SoundCloud URL for 24/7 lofi stream
# This avoids search entirely for the fallback, making it rock-solid
LOFI_QUERY = "scsearch1:lofi hip hop radio beats to study relax"

# How long to wait for a title resolution before giving up on a source
TITLE_TIMEOUT = 30  # seconds

# Global semaphore: only ONE song search at a time.
# Prevents zombie yt-dlp processes from piling up during reconnect storms.
# If a search is already running and a new one is requested, the new one
# waits instead of spawning another yt-dlp process.
_search_sem: asyncio.Semaphore | None = None


def _get_search_sem() -> asyncio.Semaphore:
    """Lazily initialize the semaphore (needs running event loop)."""
    global _search_sem
    if _search_sem is None:
        _search_sem = asyncio.Semaphore(1)
    return _search_sem


class AudioBroadcaster:
    """
    Broadcasts an audio stream (MP3) to all connected HTTP clients.
    Works like an internet radio station — clients connect to /stream
    and receive live MP3 data via HTTP chunked transfer.
    """

    def __init__(self):
        self._client_queues: dict[int, asyncio.Queue] = {}
        self._next_id = 0
        self._lock = asyncio.Lock()
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._ytdlp_proc: Optional[asyncio.subprocess.Process] = None
        self._playing = False
        self._current_title = ""
        self._current_source = ""

    @property
    def listener_count(self) -> int:
        return len(self._client_queues)

    @property
    def is_playing(self) -> bool:
        return self._playing

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
                "icy-pub": "1",
                "icy-br": "128",
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

    async def broadcast(self, chunk: bytes):
        async with self._lock:
            queues = list(self._client_queues.values())
        for q in queues:
            try:
                q.put_nowait(chunk)
            except asyncio.QueueFull:
                pass

    # ── Playback Control ──────────────────────────────────────────────────────

    async def stop_current(self):
        """Kill any running ffmpeg and yt-dlp processes."""
        for proc_attr in ("_proc", "_ytdlp_proc"):
            p = getattr(self, proc_attr)
            if p and p.returncode is None:
                try:
                    p.kill()
                    await p.wait()
                except Exception:
                    pass
            setattr(self, proc_attr, None)

        self._playing = False
        self._current_title = ""
        self._current_source = ""

    async def play(
        self,
        song_name: str,
        on_found=None,          # Optional async callback: on_found(title, source_label)
    ) -> tuple[bool, str]:
        """
        Finds and streams a song. Returns (success, title).

        on_found: if provided, called with (title, source_label) the moment search
                  succeeds and streaming is about to begin — perfect for announcing
                  "Now playing" without guessing before search or announcing after.
        """
        await self.stop_current()

        # ── Step 1: Find which source has the song ────────────────────────────
        found = await _find_source(song_name)
        if not found:
            print(f"[RADIO] '{song_name}' — not found on any source.")
            return False, song_name

        title, direct_url, extra_args, source_label = found
        print(f"[RADIO] '{title}' found via {source_label} — starting stream...")

        # Notify caller that song was found and streaming is starting
        if on_found:
            try:
                await on_found(title, source_label)
            except Exception as e:
                print(f"[RADIO] on_found callback error: {e}")

        self._playing = True
        self._current_title = title
        self._current_source = source_label

        try:
            # ── Step 2: Create an OS-level pipe so yt-dlp → ffmpeg works ─────
            # asyncio proc.stdout is a StreamReader (no .fileno()), so we CANNOT
            # pass it as stdin to another subprocess — os.pipe() gives real fds.
            pipe_read_fd, pipe_write_fd = os.pipe()

            # ── Step 3: yt-dlp writes audio to the write end of the pipe ──────
            ytdlp_cmd = [
                "yt-dlp",
                "-o", "-",
                "-q",
                "--no-warnings",
                "--no-playlist",
                "-f", "bestaudio/best",
                *extra_args,
                direct_url,          # direct URL from title check — no second search!
            ]
            try:
                ytdlp_proc = await asyncio.create_subprocess_exec(
                    *ytdlp_cmd,
                    stdout=pipe_write_fd,   # Real OS fd — has fileno(), works!
                    stderr=asyncio.subprocess.PIPE,
                )
            finally:
                # Always close parent's copy of write end so ffmpeg gets EOF
                # when yt-dlp finishes, even if yt-dlp failed to start
                os.close(pipe_write_fd)
            self._ytdlp_proc = ytdlp_proc

            # ── Step 4: ffmpeg reads from the read end of the pipe ────────────
            ffmpeg_cmd = [
                "ffmpeg",
                "-i", "pipe:0",
                "-vn",
                "-acodec", "libmp3lame",
                "-ab", "128k",
                "-ar", "44100",
                "-f", "mp3",
                "-loglevel", "error",
                "pipe:1",
            ]
            try:
                ffmpeg_proc = await asyncio.create_subprocess_exec(
                    *ffmpeg_cmd,
                    stdin=pipe_read_fd,     # Real OS fd — has fileno(), works!
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
            finally:
                # Always close parent's copy of read end
                os.close(pipe_read_fd)
            self._proc = ffmpeg_proc

            # ── Step 4: Broadcast audio chunks ───────────────────────────────
            chunks_sent = 0
            while True:
                chunk = await ffmpeg_proc.stdout.read(8192)
                if not chunk:
                    break
                await self.broadcast(chunk)
                chunks_sent += 1

            await ffmpeg_proc.wait()

            # Log any errors
            if ffmpeg_proc.returncode not in (0, None):
                err = await ffmpeg_proc.stderr.read(2048)
                print(f"[FFMPEG ERR] code={ffmpeg_proc.returncode} :: {err.decode('utf-8', errors='replace').strip()}")

            try:
                if ytdlp_proc.returncode is None:
                    ytdlp_proc.kill()
                    await ytdlp_proc.wait()
                elif ytdlp_proc.returncode != 0:
                    err = await ytdlp_proc.stderr.read(2048)
                    print(f"[YTDLP ERR] code={ytdlp_proc.returncode} :: {err.decode('utf-8', errors='replace').strip()}")
            except Exception:
                pass

            if chunks_sent == 0:
                print(f"[RADIO] Stream produced 0 bytes for '{title}' [{source_label}] — likely blocked/invalid.")
                return False, title

            print(f"[RADIO] Done: '{title}' [{source_label}] — {chunks_sent} chunks streamed.")
            return True, title

        except asyncio.CancelledError:
            await self.stop_current()
            raise
        except Exception as e:
            print(f"[RADIO] Playback error for '{song_name}': {e}")
            return False, song_name
        finally:
            self._playing = False
            self._proc = None
            self._ytdlp_proc = None
            self._current_title = ""
            self._current_source = ""


# ─────────────────────────────────────────────────────────────────────────────
# Source Resolution
# ─────────────────────────────────────────────────────────────────────────────

async def _find_source(song_name: str) -> Optional[tuple[str, str, list, str]]:
    """
    Tries each source in SOURCES order.
    Returns (title, direct_url, extra_args, source_label) or None.

    Fetches BOTH title and webpage_url in a single yt-dlp call so that
    the stream subprocess uses the exact URL that was found — preventing
    any title/audio mismatch from two separate searches.

    Protected by a semaphore so only 1 search runs at a time — prevents
    zombie yt-dlp process buildup during bot reconnect cycles.
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
                print(f"[SEARCH] [{label}] Found: {title!r}")
                print(f"[SEARCH] [{label}] URL: {url}")
                return title, url, extra, label
            else:
                print(f"[SEARCH] [{label}] Not found — trying next source.")

    return None


async def _get_title_and_url(
    query: str,
    extra_args: list,
    label: str,
) -> Optional[tuple[str, str]]:
    """
    Runs yt-dlp with two --print fields to get BOTH title and webpage_url
    in a single call. Returns (title, direct_url) or None.

    Handles CancelledError by killing the subprocess before propagating —
    prevents zombie yt-dlp processes when the song loop is cancelled.
    """
    cmd = [
        "yt-dlp",
        "--skip-download",
        "--print", "%(title)s",
        "--print", "%(webpage_url)s",
        "-q",
        "--no-warnings",
        "--no-playlist",
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
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=TITLE_TIMEOUT
            )
        except asyncio.TimeoutError:
            _kill_proc(proc)
            print(f"[SEARCH] [{label}] Timed out after {TITLE_TIMEOUT}s.")
            return None
        except asyncio.CancelledError:
            # Task was cancelled (bot reconnecting) — kill subprocess immediately
            # so it doesn't linger as a zombie consuming resources.
            _kill_proc(proc)
            raise   # Re-raise so the song loop exits cleanly

        if proc.returncode != 0:
            err = stderr.decode("utf-8", errors="replace").strip()
            if err and "ERROR" in err:
                print(f"[SEARCH] [{label}] error: {err[:200]}")
            return None

        lines = stdout.decode("utf-8", errors="replace").strip().split("\n")
        # Two --print fields → yt-dlp outputs them on consecutive lines:
        #   line 0: title
        #   line 1: webpage_url
        if len(lines) < 2:
            return None
        title = lines[0].strip()
        url = lines[1].strip()
        if not title or not url or url == "NA":
            return None
        return title, url

    except FileNotFoundError:
        print("[SEARCH] ERROR: yt-dlp not found in PATH! Check your Dockerfile.")
        return None
    except asyncio.CancelledError:
        raise   # Always propagate cancellation
    except Exception as e:
        print(f"[SEARCH] [{label}] Unexpected error: {e}")
        return None


def _kill_proc(proc) -> None:
    """Kill a subprocess silently — used for cleanup on timeout/cancellation."""
    try:
        if proc and proc.returncode is None:
            proc.kill()
    except Exception:
        pass


broadcaster = AudioBroadcaster()
