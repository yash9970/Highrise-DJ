import os
import asyncio
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

from highrise.__main__ import main, BotDefinition
from bot import DJBot

if __name__ == "__main__":
    token = os.environ.get("HIGHRISE_TOKEN")
    room_id = os.environ.get("HIGHRISE_ROOM_ID")

    if not token:
        raise ValueError("HIGHRISE_TOKEN environment variable is not set")
    if not room_id:
        raise ValueError("HIGHRISE_ROOM_ID environment variable is not set")

    definitions = [BotDefinition(bot=DJBot(), room_id=room_id, api_token=token)]
    asyncio.run(main(definitions))
