import asyncio
import yt_dlp
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# Search sources tried in order. First one that finds a result wins.
# YouTube uses the iOS player client to bypass datacenter IP blocking.
# SoundCloud is a reliable fallback.
# ─────────────────────────────────────────────────────────────────────────────
SEARCH_SOURCES = [
    {
        "label": "SoundCloud",
        "prefix": "scsearch1",
        "extra_args": {},
    },
    {
        "label": "YouTube",
        "prefix": "ytsearch1",
        "extra_args": {"extractor_args": {"youtube": {"player_client": ["ios"]}}},
    },
]


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
        self._current_source = ""  # "YouTube" or "SoundCloud"

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

        print(f"[RADIO] Listener {client_id} connected. Total: {len(self._client_queues)}")

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
            print(f"[RADIO] Listener {client_id} left. Total: {len(self._client_queues)}")

        return response

    async def broadcast(self, chunk: bytes):
        async with self._lock:
            queues = list(self._client_queues.values())
        for q in queues:
            try:
                q.put_nowait(chunk)
            except asyncio.QueueFull:
                pass

    async def stop_current(self):
        if self._proc and self._proc.returncode is None:
            try:
                self._proc.kill()
                await self._proc.wait()
            except Exception:
                pass

        if self._ytdlp_proc and self._ytdlp_proc.returncode is None:
            try:
                self._ytdlp_proc.kill()
                await self._ytdlp_proc.wait()
            except Exception:
                pass

        self._proc = None
        self._ytdlp_proc = None
        self._playing = False
        self._current_title = ""
        self._current_source = ""

    async def play(self, song_name: str) -> tuple[bool, str]:
        """
        Searches for the song across multiple sources (YouTube first, then SoundCloud).
        Returns (success, title).
        """
        await self.stop_current()

        # ── Step 1: Resolve title + find which source has it ──────────────────
        resolved = await _resolve_song(song_name)
        if not resolved:
            print(f"[RADIO] '{song_name}' not found on any source — skipping.")
            return False, song_name

        title, search_query, source_label = resolved
        print(f"[RADIO] Found '{title}' via {source_label} — streaming...")

        self._playing = True
        self._current_title = title
        self._current_source = source_label

        try:
            # ── Step 2: yt-dlp subprocess → pipes audio to ffmpeg ──────────────
            ytdlp_cmd = _build_ytdlp_cmd(search_query, source_label)
            ytdlp_proc = await asyncio.create_subprocess_exec(
                *ytdlp_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            self._ytdlp_proc = ytdlp_proc

            # ── Step 3: ffmpeg re-encodes to MP3 and pipes to broadcaster ──────
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg",
                "-i", "pipe:0",
                "-vn",
                "-acodec", "libmp3lame",
                "-ab", "128k",
                "-ar", "44100",
                "-f", "mp3",
                "pipe:1",
                stdin=ytdlp_proc.stdout,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            self._proc = proc

            chunks_sent = 0
            while True:
                chunk = await proc.stdout.read(8192)
                if not chunk:
                    break
                await self.broadcast(chunk)
                chunks_sent += 1

            await proc.wait()

            if proc.returncode not in (0, None):
                err = await proc.stderr.read()
                print(f"[FFMPEG ERR] code={proc.returncode} {err.decode('utf-8', errors='replace')[:300]}")

            # Clean up yt-dlp
            try:
                if ytdlp_proc.returncode is None:
                    ytdlp_proc.kill()
                    await ytdlp_proc.wait()
                elif ytdlp_proc.returncode != 0:
                    err = await ytdlp_proc.stderr.read()
                    print(f"[YTDLP ERR] code={ytdlp_proc.returncode} {err.decode('utf-8', errors='replace')[:300]}")
            except Exception:
                pass

            if chunks_sent == 0:
                print(f"[RADIO] Failed: '{title}' [{source_label}] produced zero audio chunks (blocked or invalid stream).")
                return False, title

            print(f"[RADIO] Done: '{title}' [{source_label}] (ff={proc.returncode}, yt={ytdlp_proc.returncode}, chunks={chunks_sent})")
            return True, title

        except asyncio.CancelledError:
            await self.stop_current()
            raise
        except Exception as e:
            print(f"[RADIO] Playback error: {e}")
            return False, title
        finally:
            self._playing = False
            self._proc = None
            self._ytdlp_proc = None
            self._current_title = ""
            self._current_source = ""


# ─────────────────────────────────────────────────────────────────────────────
# Helper: resolve song title + pick source
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# Search Helper: Persistent yt-dlp instance to save memory & time
# ─────────────────────────────────────────────────────────────────────────────
YDL_OPTS = {
    "format": "bestaudio/best",
    "noplaylist": True,
    "quiet": True,
    "no_warnings": True,
    "extract_flat": True,
    "cachedir": False,  # Reduce disk/memory usage on Render
    "no_color": True,
}

# We initialize one global instance to avoid the overhead of loading extractors on every search
_ydl = yt_dlp.YoutubeDL(YDL_OPTS)


async def _resolve_song(song_name: str) -> Optional[tuple[str, str, str]]:
    """
    Tries each source in SEARCH_SOURCES order.
    Returns (title, search_query_for_subprocess, source_label) or None.
    """
    loop = asyncio.get_event_loop()

    for source in SEARCH_SOURCES:
        label = source["label"]
        prefix = source["prefix"]
        extra = source["extra_args"]
        query = f"{prefix}:{song_name}"

        print(f"[SEARCH] Trying {label} for: {song_name!r}")

        def _search(q=query, ex=extra):
            # Temporarily update params for this specific search if needed (like iOS client)
            # Note: _ydl.params is a dict we can update or use context managers
            old_params = _ydl.params.copy()
            if ex:
                _ydl.params.update(ex)

            try:
                info = _ydl.extract_info(q, download=False)
                if not info:
                    return None
                # Playlist-style result (entries list)
                entries = info.get("entries") or []
                if entries:
                    entry = entries[0]
                    return entry.get("title") or song_name
                # Single-entry result
                title = info.get("title")
                if title:
                    return title
            except Exception as e:
                print(f"[SEARCH] {label} error: {e}")
            finally:
                # Restore original params
                _ydl.params = old_params
            return None

        title = await loop.run_in_executor(None, _search)
        if title:
            print(f"[SEARCH] {label} found: {title!r}")
            return title, query, label
        else:
            print(f"[SEARCH] {label} — not found, trying next source...")

    return None


def _build_ytdlp_cmd(search_query: str, source_label: str) -> list[str]:
    """
    Builds the yt-dlp subprocess command for the given source.
    YouTube gets extra extractor args to use the iOS player client.
    """
    cmd = [
        "yt-dlp",
        "-o", "-",
        "-q",
        "--no-warnings",
        "-f", "bestaudio/best",
    ]

    if source_label == "YouTube":
        cmd += ["--extractor-args", "youtube:player_client=ios"]

    cmd.append(search_query)
    return cmd


broadcaster = AudioBroadcaster()
