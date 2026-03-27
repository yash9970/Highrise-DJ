import os
import asyncio
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

from aiohttp import web
from highrise.__main__ import main, BotDefinition
from bot import DJBot
from streamer import broadcaster

PORT = int(os.environ.get("PORT", 8000))


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

    domain = os.environ.get("RENDER_EXTERNAL_URL") or os.environ.get("REPLIT_DEV_DOMAIN")
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


async def run_bot():
    token = os.environ.get("HIGHRISE_TOKEN")
    room_id = os.environ.get("HIGHRISE_ROOM_ID")

    if not token:
        raise ValueError("HIGHRISE_TOKEN is not set")
    if not room_id:
        raise ValueError("HIGHRISE_ROOM_ID is not set")

    while True:
        try:
            print("[BOT] Connecting to Highrise...")
            definitions = [BotDefinition(bot=DJBot(), room_id=room_id, api_token=token)]
            await main(definitions)
        except Exception as e:
            print(f"[BOT] Disconnected: {e}. Reconnecting in 10 seconds...")
            await asyncio.sleep(10)


async def run_all():
    await asyncio.gather(
        run_web_server(),
        run_bot(),
    )


if __name__ == "__main__":
    asyncio.run(run_all())
