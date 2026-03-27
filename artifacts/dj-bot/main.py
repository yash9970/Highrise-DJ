import os
import asyncio
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

from aiohttp import web
from highrise.__main__ import main, BotDefinition
from bot import DJBot

PORT = int(os.environ.get("PORT", 8000))


async def health(request):
    return web.Response(text="DJ Bot is alive!", status=200)


async def run_health_server():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"[HTTP] Health server running on port {PORT} — ping this with UptimeRobot")


async def run_bot():
    token = os.environ.get("HIGHRISE_TOKEN")
    room_id = os.environ.get("HIGHRISE_ROOM_ID")

    if not token:
        raise ValueError("HIGHRISE_TOKEN environment variable is not set")
    if not room_id:
        raise ValueError("HIGHRISE_ROOM_ID environment variable is not set")

    while True:
        try:
            print("[BOT] Connecting to Highrise...")
            definitions = [BotDefinition(bot=DJBot(), room_id=room_id, api_token=token)]
            await main(definitions)
        except Exception as e:
            print(f"[BOT] Connection lost: {e}. Reconnecting in 10 seconds...")
            await asyncio.sleep(10)


async def run_all():
    await asyncio.gather(
        run_health_server(),
        run_bot(),
    )


if __name__ == "__main__":
    asyncio.run(run_all())
