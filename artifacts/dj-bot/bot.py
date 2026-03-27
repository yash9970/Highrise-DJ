import os
import asyncio
from highrise import BaseBot, SessionMetadata, User, Position, AnchorPosition
from db import init_db, add_song, get_queue, get_next_song, delete_song
from vip_checker import is_vip
from streamer import broadcaster

MASTER_USERNAME = "Zen1thos"

BOT_POSITION = Position(10.5, 0.25, 13.5, facing="FrontLeft")

DANCE_EMOTES = [
    "dance-tiktok8",
    "dance-blackpink",
    "dance-pennywise",
    "idle-dance-casual",
    "dance-tiktok2",
]

DJ_PHRASES = [
    "Let the music move you! DJ in the house!",
    "Vibes only! Keep the energy up!",
    "This next track is FIRE!",
    "Put your hands up if you're feeling it!",
    "Drop it like it's hot!",
    "The queue is open — type !dj play <song> to request!",
    "DJ never stops! Request your song with !dj play <song>",
    "Feel the rhythm, feel the bass!",
    "Tonight we dance all night long!",
    "Music is life! Keep vibing!",
]

HELP_TEXT = (
    "DJ Bot Commands (prefix: !dj):\n"
    "  !dj play <song> — Queue a song\n"
    "  !dj queue — Show the song queue\n"
    "  !dj skip — Skip current song (master only)\n"
    "  !dj clear — Clear the queue (master only)\n"
    "  !dj inventory — View bot outfit info (master only)\n"
    "  !dj help — Show this help message (master only)"
)


class DJBot(BaseBot):
    def __init__(self):
        super().__init__()
        self._song_task: asyncio.Task | None = None
        self._dance_task: asyncio.Task | None = None
        self._talk_task: asyncio.Task | None = None

    async def on_start(self, session_metadata: SessionMetadata) -> None:
        print(f"[BOT] DJ Bot started. Bot ID: {session_metadata.user_id}")
        init_db()

        await asyncio.sleep(2)

        try:
            await self.highrise.teleport(session_metadata.user_id, BOT_POSITION)
            print(f"[BOT] Teleported to position {BOT_POSITION}.")
        except Exception as e:
            print(f"[BOT] Teleport failed: {e}")

        await asyncio.sleep(2)

        self._dance_task = asyncio.create_task(self._dance_loop())
        self._talk_task = asyncio.create_task(self._talk_loop())
        self._song_task = asyncio.create_task(self._song_loop())

    async def _dance_loop(self):
        i = 0
        while True:
            try:
                emote_id = DANCE_EMOTES[i % len(DANCE_EMOTES)]
                await self.highrise.send_emote(emote_id)
                print(f"[BOT] Dancing: {emote_id}")
            except Exception as e:
                print(f"[BOT] Dance emote error: {e}")
            i += 1
            await asyncio.sleep(30)

    async def _talk_loop(self):
        await asyncio.sleep(30)
        i = 0
        while True:
            try:
                phrase = DJ_PHRASES[i % len(DJ_PHRASES)]
                await self.highrise.chat(phrase)
                print(f"[BOT] DJ talk: {phrase}")
            except Exception as e:
                print(f"[BOT] Talk error: {e}")
            i += 1
            await asyncio.sleep(30)

    async def _song_loop(self):
        while True:
            try:
                next_song = get_next_song()
                if next_song:
                    song_name = next_song["song_name"]
                    requested_by = next_song["requested_by"]
                    song_id = next_song["id"]

                    try:
                        await self.highrise.chat(f"Searching for: {song_name}... (requested by {requested_by})")
                    except Exception:
                        pass

                    success, title = await broadcaster.play(song_name)

                    if success:
                        try:
                            await self.highrise.chat(f"Finished playing: {title}")
                        except Exception:
                            pass
                    else:
                        try:
                            await self.highrise.chat(f"Could not find '{song_name}' on YouTube. Skipping.")
                        except Exception:
                            pass

                    delete_song(song_id)
                    await asyncio.sleep(1)
                else:
                    await asyncio.sleep(5)

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[BOT] Song loop error: {e}")
                await asyncio.sleep(5)

    async def on_chat(self, user: User, message: str) -> None:
        msg = message.strip()

        if not msg.lower().startswith("!dj"):
            return

        parts = msg.split(None, 2)
        if len(parts) < 2:
            return

        command = parts[1].lower() if len(parts) > 1 else ""
        args = parts[2] if len(parts) > 2 else ""

        is_master = user.username.lower() == MASTER_USERNAME.lower()
        authorized = await self._is_authorized(user, is_master)

        if command == "help":
            if is_master:
                await self._reply(HELP_TEXT)
            return

        if command == "inventory":
            if is_master:
                await self._handle_inventory(user, args)
            else:
                await self._reply(f"@{user.username} Only the master can use !dj inventory.")
            return

        if command == "play":
            if not authorized:
                await self._reply(f"@{user.username} Only VIPs, mods, designers, or the master can request songs.")
                return
            if not args:
                await self._reply(f"@{user.username} Usage: !dj play <song name>")
                return
            song_name = args.strip()
            add_song(song_name, user.username)
            queue = get_queue()
            position = len(queue)
            if broadcaster.is_playing:
                await self._reply(f"Added '{song_name}' to the queue at position {position}! (by {user.username})")
            else:
                await self._reply(f"Added '{song_name}' — playing next! (by {user.username})")
            return

        if command == "queue":
            if not authorized:
                await self._reply(f"@{user.username} Only VIPs, mods, designers, or the master can view the queue.")
                return
            queue = get_queue()
            if not queue:
                await self._reply("The queue is empty! Request a song with !dj play <song name>")
            else:
                lines = ["Song Queue:"]
                for i, song in enumerate(queue, 1):
                    marker = " (NOW PLAYING)" if i == 1 and broadcaster.is_playing else ""
                    lines.append(f"{i}. {song['song_name']} by {song['requested_by']}{marker}")
                await self._reply("\n".join(lines))
            return

        if command == "nowplaying" or command == "np":
            if broadcaster.is_playing and broadcaster.current_title:
                await self._reply(f"Now playing: {broadcaster.current_title}")
            else:
                await self._reply("Nothing is playing right now.")
            return

        if command == "skip":
            if not is_master:
                await self._reply(f"@{user.username} Only the master can skip songs.")
                return
            await broadcaster.stop_current()
            await self._reply("Skipped! Loading next song...")
            if self._song_task:
                self._song_task.cancel()
            self._song_task = asyncio.create_task(self._song_loop())
            return

        if command == "clear":
            if not is_master:
                await self._reply(f"@{user.username} Only the master can clear the queue.")
                return
            from db import clear_queue
            clear_queue()
            await broadcaster.stop_current()
            await self._reply("Queue cleared and playback stopped!")
            return

        if command == "listeners":
            if is_master:
                count = broadcaster.listener_count
                await self._reply(f"Radio listeners: {count}")
            return

    async def _is_authorized(self, user: User, is_master: bool) -> bool:
        if is_master:
            return True

        try:
            room_users = await self.highrise.get_room_users()
            for room_user, position in room_users.content:
                if room_user.id == user.id:
                    privileges = getattr(room_user, "privilege", None)
                    if privileges in ("moderator", "designer"):
                        return True
                    break
        except Exception as e:
            print(f"[BOT] Could not check room privileges: {e}")

        vip_status = await is_vip(user.username)
        return vip_status

    async def _handle_inventory(self, user: User, args: str):
        try:
            wardrobe = await self.highrise.get_my_wardrobe()
            items = getattr(wardrobe, "outfit", None) or []
            if items:
                item_names = [getattr(item, "id", str(item)) for item in items[:10]]
                await self._reply("Current outfit items:\n" + "\n".join(item_names))
            else:
                await self._reply("Wardrobe is empty or unavailable. Change the bot's outfit in-game.")
        except Exception as e:
            print(f"[BOT] Inventory error: {e}")
            await self._reply("To change the DJ bot's outfit, use the in-game wardrobe.")

    async def _reply(self, message: str):
        try:
            await self.highrise.chat(message)
        except Exception as e:
            print(f"[BOT] Failed to send message: {e}")

    async def on_user_join(self, user: User, position: Position | AnchorPosition) -> None:
        print(f"[BOT] User joined: {user.username}")

    async def on_emote(self, user: User, emote_id: str, receiver: User | None) -> None:
        pass

    async def on_tip(self, sender: User, receiver: User, tip) -> None:
        pass
