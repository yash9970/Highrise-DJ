import os
import asyncio
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

from aiohttp import web
from highrise.__main__ import main, BotDefinition
from bot import DJBot
from streamer import broadcaster

PORT = int(os.environ.get("PORT", 8000))

# Global reference to the active bot instance so we can cancel its tasks on restart
_active_bot: DJBot | None = None


async def health(request):
    status = {
        "status": "alive",
        "playing": broadcaster.is_playing,
        "current_song": broadcaster.current_title or "none",
        "listeners": broadcaster.listener_count,
    }
    return web.json_response(status)


async def stream(request):
    return await broadcaster.stream_to_client(request)


async def run_web_server():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    app.router.add_get("/stream", stream)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

    domain = os.environ.get("RENDER_EXTERNAL_URL") or os.environ.get("REPLIT_DEV_DOMAIN") or os.environ.get("SPACE_HOST")
    if domain:
        stream_url = f"https://{domain}/stream" if not domain.startswith("http") else f"{domain}/stream"
    else:
        stream_url = f"http://localhost:{PORT}/stream"

    print(f"[HTTP] Server running on port {PORT}")
    print(f"[HTTP] Health check: {stream_url.replace('/stream', '/health')}")
    print(f"")
    print(f"[RADIO] *** STREAM URL: {stream_url} ***")
    print(f"[RADIO] Set this as your Highrise room radio URL in room settings!")
    print(f"")


async def heartbeat_loop():
    """
    Prints a heartbeat every 60 seconds so Render never sees a silent stdout
    and decides to kill the process. Also logs radio status for monitoring.
    Pings the service's own URL to keep Render awake.
    """
    import aiohttp
    
    while True:
        await asyncio.sleep(60)
        status = "playing" if broadcaster.is_playing else "idle"
        song = broadcaster.current_title or "none"
        listeners = broadcaster.listener_count
        print(f"[HEARTBEAT] status={status} | song={song!r} | listeners={listeners}")
        
        # Ping own health endpoint to prevent Render from sleeping
        domain = os.environ.get("RENDER_EXTERNAL_URL")
        if domain:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"{domain}/health") as resp:
                        pass
            except Exception as e:
                print(f"[HEARTBEAT] Ping failed: {e}")


async def run_bot():
    global _active_bot
    print("[DEBUG] run_bot() started")
    
    token = os.environ.get("HIGHRISE_TOKEN")
    room_id = os.environ.get("HIGHRISE_ROOM_ID")

    if not token:
        print("[DEBUG] Missing HIGHRISE_TOKEN")
        raise ValueError("HIGHRISE_TOKEN is not set")
    if not room_id:
        print("[DEBUG] Missing HIGHRISE_ROOM_ID")
        raise ValueError("HIGHRISE_ROOM_ID is not set")

    print(f"[DEBUG] Loaded token and room ID: {room_id}")

    # Minimum delay between ANY reconnect attempt.
    # Critical: Highrise reports "Multilogin" if we reconnect before the
    # previous WebSocket session has fully closed server-side (~5s).
    MIN_RECONNECT_DELAY = 8

    retry_delay = MIN_RECONNECT_DELAY

    while True:
        try:
            print("[BOT] Connecting to Highrise...")
            print("[DEBUG] Stopping broadcaster...")
            # Kill any running audio and search processes before creating
            # a new bot instance — prevents orphaned yt-dlp zombies.
            await broadcaster.stop_current()

            print("[DEBUG] Creating DJBot instance...")
            bot = DJBot()
            _active_bot = bot

            print("[DEBUG] Creating BotDefinition...")
            definitions = [BotDefinition(bot=bot, room_id=room_id, api_token=token)]
            
            print("[DEBUG] Awaiting main(definitions)...")
            await main(definitions)

            # Clean disconnect — always wait before reconnecting.
            # Without this sleep, we hit Highrise "Multilogin" instantly.
            print(f"[BOT] Session ended cleanly. Reconnecting in {MIN_RECONNECT_DELAY}s...")
            retry_delay = MIN_RECONNECT_DELAY
            await asyncio.sleep(MIN_RECONNECT_DELAY)

        except asyncio.CancelledError:
            print("[BOT] Bot task cancelled — shutting down.")
            raise
        except Exception as e:
            print(f"[BOT] Disconnected: {e}. Reconnecting in {retry_delay}s...")
            await asyncio.sleep(retry_delay)
            # Exponential backoff up to 60s on repeated failures
            retry_delay = min(retry_delay * 2, 60)


async def _clear_ytdlp_cache():
    """
    Clears yt-dlp's cache on startup.
    SoundCloud requires a client_id that yt-dlp fetches and caches.
    A stale cached client_id causes "not found" for every search.
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            "yt-dlp", "--rm-cache-dir",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await proc.wait()
        print("[STARTUP] yt-dlp cache cleared (fresh SoundCloud client_id will be fetched).")
    except Exception as e:
        print(f"[STARTUP] Could not clear yt-dlp cache: {e}")


async def run_all():
    await _clear_ytdlp_cache()
    await asyncio.gather(
        run_web_server(),
        run_bot(),
        heartbeat_loop(),
    )


if __name__ == "__main__":
    asyncio.run(run_all())
