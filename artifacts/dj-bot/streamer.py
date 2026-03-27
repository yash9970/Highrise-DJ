import asyncio
import yt_dlp
from typing import Optional


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
        self._playing = False
        self._current_title = ""

    @property
    def listener_count(self) -> int:
        return len(self._client_queues)

    @property
    def is_playing(self) -> bool:
        return self._playing

    @property
    def current_title(self) -> str:
        return self._current_title

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

        print(f"[RADIO] Listener {client_id} connected. Total listeners: {len(self._client_queues)}")

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
            print(f"[RADIO] Listener {client_id} left. Total listeners: {len(self._client_queues)}")

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
        self._proc = None
        self._playing = False
        self._current_title = ""

    async def play(self, song_name: str) -> tuple[bool, str]:
        await self.stop_current()

        print(f"[RADIO] Searching YouTube for: {song_name}")
        audio_url, title = await search_youtube(song_name)

        if not audio_url:
            print(f"[RADIO] No audio found for: {song_name}")
            return False, song_name

        print(f"[RADIO] Found: {title}")
        print(f"[RADIO] Starting stream...")

        self._playing = True
        self._current_title = title

        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg",
                "-reconnect", "1",
                "-reconnect_streamed", "1",
                "-reconnect_delay_max", "5",
                "-i", audio_url,
                "-vn",
                "-acodec", "libmp3lame",
                "-ab", "128k",
                "-ar", "44100",
                "-f", "mp3",
                "pipe:1",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            self._proc = proc

            while True:
                chunk = await proc.stdout.read(8192)
                if not chunk:
                    break
                await self.broadcast(chunk)

            await proc.wait()
            print(f"[RADIO] Finished playing: {title}")

        except asyncio.CancelledError:
            await self.stop_current()
            raise
        except Exception as e:
            print(f"[RADIO] Playback error: {e}")
            return False, title
        finally:
            self._playing = False
            self._proc = None
            self._current_title = ""

        return True, title


async def search_youtube(song_name: str) -> tuple[Optional[str], str]:
    ydl_opts = {
        "format": "bestaudio[ext=webm]/bestaudio/best",
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "extract_flat": False,
    }

    loop = asyncio.get_event_loop()

    def _search():
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(f"ytsearch1:{song_name}", download=False)
                if info and info.get("entries") and len(info["entries"]) > 0:
                    entry = info["entries"][0]
                    title = entry.get("title", song_name)
                    url = entry.get("url")
                    return url, title
        except Exception as e:
            print(f"[RADIO] yt-dlp error: {e}")
        return None, song_name

    return await loop.run_in_executor(None, _search)


broadcaster = AudioBroadcaster()
