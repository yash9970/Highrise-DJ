import os
import asyncio
from highrise import BaseBot, SessionMetadata, User, Position, AnchorPosition
from db import init_db, add_song, get_queue, get_next_song, delete_song
from vip_checker import is_vip
from streamer import broadcaster

BOT_POSITION = Position(10.5, 0.25, 13.5, facing="FrontLeft")

DANCE_EMOTES = [
    "dance-tiktok8",
    "dance-blackpink",
    "dance-pennywise",
    "idle-dance-casual",
    "dance-tiktok2",
]

# ─────────────────────────────────────────────────────────────────────────────
# Trending songs to auto-play when the queue is empty.
# Add / remove entries freely — the bot cycles through them in order.
# ─────────────────────────────────────────────────────────────────────────────
TRENDING_SONGS = [
    "APT. Rose Bruno Mars",
    "Luther Kendrick Lamar SZA",
    "Die With A Smile Lady Gaga Bruno Mars",
    "Espresso Sabrina Carpenter",
    "Million Dollar Baby Tommy Richman",
    "Good Luck Babe Chappell Roan",
    "Not Like Us Kendrick Lamar",
    "Birds Of A Feather Billie Eilish",
    "Lose Control Teddy Swims",
    "Texas Hold Em Beyonce",
    "Fortnight Taylor Swift Post Malone",
    "Please Please Please Sabrina Carpenter",
    "Too Sweet Hozier",
    "HUMBLE Kendrick Lamar",
    "Blinding Lights The Weeknd",
    "Levitating Dua Lipa",
    "Stay The Kid LAROI Justin Bieber",
    "Heat Waves Glass Animals",
    "Peaches Justin Bieber",
    "INDUSTRY BABY Lil Nas X",
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

HELP_LINES = [
    "=== DJ Bot Commands ===",
    "!dj play <song> — Queue a song",
    "!dj queue — Show song queue",
    "!dj np — Now playing",
    "!dj skip — Skip song (master)",
    "!dj clear — Clear queue (master)",
    "!dj viplist — Show VIPs & mods from helper bot",
    "!dj inventory — Bot outfit (master)",
    "!dj listeners — Listener count (master)",
    "!dj help — This message (master)",
]

_active_tasks: list[asyncio.Task] = []


def _cancel_all_tasks():
    """Cancel all previously registered background tasks."""
    for t in list(_active_tasks):
        if not t.done():
            t.cancel()
    _active_tasks.clear()


class DJBot(BaseBot):
    def __init__(self):
        super().__init__()
        self._owner_id: str = ""
        self._song_task: asyncio.Task | None = None
        self._dance_task: asyncio.Task | None = None
        self._talk_task: asyncio.Task | None = None
        self._is_fallback_playing: bool = False
        self._trending_index: int = 0  # cycles through TRENDING_SONGS

    async def on_start(self, session_metadata: SessionMetadata) -> None:
        self._owner_id = session_metadata.room_info.owner_id
        print(f"[BOT] DJ Bot started. Bot ID: {session_metadata.user_id}")
        print(f"[BOT] Room owner ID: {self._owner_id} (this is the master)")

        # Cancel any tasks from a previous bot instance that may still be running
        _cancel_all_tasks()

        init_db()

        await asyncio.sleep(2)

        try:
            await self.highrise.teleport(session_metadata.user_id, BOT_POSITION)
            print(f"[BOT] Teleported to position.")
        except Exception as e:
            print(f"[BOT] Teleport failed: {e}")

        await asyncio.sleep(2)

        self._dance_task = asyncio.create_task(self._dance_loop())
        self._talk_task = asyncio.create_task(self._talk_loop())
        self._song_task = asyncio.create_task(self._song_loop())

        _active_tasks.extend([self._dance_task, self._talk_task, self._song_task])
        print(f"[BOT] Background tasks started (dance, talk, song).")

    async def _dance_loop(self):
        i = 0
        while True:
            try:
                emote_id = DANCE_EMOTES[i % len(DANCE_EMOTES)]
                await self.highrise.send_emote(emote_id)
                print(f"[BOT] Dancing: {emote_id}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[BOT] Dance emote error: {e}")
                await asyncio.sleep(5)
                continue
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
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[BOT] Talk error: {e}")
                await asyncio.sleep(5)
                continue
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
                        await self.highrise.chat(f"🎵 Searching: {song_name} (by {requested_by})...")
                    except Exception:
                        pass

                    print(f"[SONG] Playing queued song: {song_name!r} (id={song_id})")
                    success, title = await broadcaster.play(song_name)

                    if success:
                        print(f"[SONG] Finished: {title!r}")
                        try:
                            await self.highrise.chat(f"✅ Finished: {title}")
                        except Exception:
                            pass
                    else:
                        print(f"[SONG] Could not find: {song_name!r} — skipping")
                        try:
                            await self.highrise.chat(f"❌ Could not find '{song_name}' on SoundCloud. Skipping.")
                        except Exception:
                            pass

                    delete_song(song_id)
                    await asyncio.sleep(1)

                else:
                    # ── Queue is empty — auto-play next trending song ──────────
                    trending_song = TRENDING_SONGS[self._trending_index % len(TRENDING_SONGS)]
                    self._trending_index += 1

                    print(f"[SONG] Queue empty — auto-playing trending: {trending_song!r}")
                    self._is_fallback_playing = True

                    try:
                        await self.highrise.chat(
                            f"🎶 Queue is empty! Auto-playing trending: {trending_song} 🔥"
                        )
                    except Exception:
                        pass

                    try:
                        await broadcaster.play(trending_song)
                    except asyncio.CancelledError:
                        self._is_fallback_playing = False
                        raise
                    except Exception as e:
                        print(f"[SONG] Trending playback error: {e}")
                    finally:
                        self._is_fallback_playing = False

                    # Brief pause before picking the next trending song
                    await asyncio.sleep(2)

            except asyncio.CancelledError:
                print("[SONG] Song loop cancelled.")
                break
            except Exception as e:
                print(f"[BOT] Song loop error: {e}")
                await asyncio.sleep(5)

    async def on_chat(self, user: User, message: str) -> None:
        msg = message.strip()

        print(f"[CHAT] {user.username}: {msg}")

        if not msg.lower().startswith("!dj"):
            return

        parts = msg.split(None, 2)
        if len(parts) < 2:
            return

        command = parts[1].lower()
        args = parts[2].strip() if len(parts) > 2 else ""

        is_master = (user.id == self._owner_id)
        print(f"[CMD] Command='{command}' from '{user.username}' id={user.id} (is_master={is_master})")

        authorized = await self._is_authorized(user, is_master)

        if command == "help":
            if is_master:
                await self._reply_lines(HELP_LINES)
            else:
                await self._reply(f"@{user.username} Only the master can use !dj help.")
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
            add_song(args, user.username)
            if self._is_fallback_playing:
                await broadcaster.stop_current()
                await self._reply(f"Interrupting radio to play '{args}'! (by {user.username})")
            else:
                queue = get_queue()
                position = len(queue)
                if broadcaster.is_playing:
                    await self._reply(f"Added '{args}' to the queue at position {position}! (by {user.username})")
                else:
                    await self._reply(f"Added '{args}' — playing next! (by {user.username})")
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

        if command in ("nowplaying", "np"):
            if broadcaster.is_playing and broadcaster.current_title:
                await self._reply(f"Now playing: {broadcaster.current_title}")
            else:
                await self._reply("Nothing is playing right now.")
            return

        if command == "skip":
            if not await self._is_bot_mod(user, is_master):
                await self._reply(f"@{user.username} Only mods can skip songs.")
                return
            next_song = get_next_song()
            if next_song:
                delete_song(next_song["id"])
            await broadcaster.stop_current()
            await self._reply("Skipped! Loading next song...")
            if self._song_task and not self._song_task.done():
                self._song_task.cancel()
            self._song_task = asyncio.create_task(self._song_loop())
            _active_tasks.append(self._song_task)
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

        if command == "viplist":
            if not is_master:
                await self._reply(f"@{user.username} Only the master can use !dj viplist.")
                return
            await self._handle_viplist()
            return

        if command == "listeners":
            if is_master:
                count = broadcaster.listener_count
                await self._reply(f"Radio listeners: {count}")
            return

    async def _is_bot_mod(self, user: User, is_master: bool) -> bool:
        if is_master:
            return True

        try:
            room_users = await self.highrise.get_room_users()
            for room_user, position in room_users.content:
                if room_user.id == user.id:
                    privileges = getattr(room_user, "privilege", None)
                    print(f"[AUTH] {user.username} privilege: {privileges}")
                    if privileges in ("moderator", "designer"):
                        return True
                    break
        except Exception as e:
            print(f"[BOT] Could not check room privileges: {e}")

        from vip_checker import is_mod
        mod_status = await is_mod(user.username)
        print(f"[AUTH] {user.username} MOD status: {mod_status}")
        return mod_status

    async def _is_authorized(self, user: User, is_master: bool) -> bool:
        if await self._is_bot_mod(user, is_master):
            return True

        from vip_checker import is_vip
        vip_status = await is_vip(user.username)
        print(f"[AUTH] {user.username} VIP status: {vip_status}")
        return vip_status

    async def _handle_viplist(self):
        """Fetch VIPs and mods from the helper bot and show status + list."""
        from vip_checker import get_vips, get_mods, VIP_API_BASE

        try:
            vips = await get_vips()
            mods = await get_mods()

            status_line = f"Helper bot ({VIP_API_BASE}): ✅ online"

            lines = [status_line]

            if vips:
                lines.append(f"VIPs ({len(vips)}): " + ", ".join(vips))
            else:
                lines.append("VIPs: none")

            if mods:
                lines.append(f"Mods ({len(mods)}): " + ", ".join(mods))
            else:
                lines.append("Mods: none")

            await self._reply_lines(lines)

        except Exception as e:
            print(f"[BOT] viplist error: {e}")
            await self._reply(f"❌ Helper bot unreachable: {e}")

    async def _handle_inventory(self, user: User, args: str):
        try:
            result = await self.highrise.get_my_outfit()
            items = getattr(result, "outfit", None) or []
            if items:
                item_names = [getattr(item, "id", str(item)) for item in items[:15]]
                await self._reply("Bot outfit:\n" + "\n".join(item_names))
            else:
                await self._reply("No outfit items found. Change the bot's outfit in-game.")
        except Exception as e:
            print(f"[BOT] Inventory error: {e}")
            await self._reply("To change the bot's outfit, equip items in-game while logged in as the bot account.")

    async def _reply(self, message: str):
        MAX_LEN = 200
        if len(message) <= MAX_LEN:
            try:
                await self.highrise.chat(message)
            except Exception as e:
                print(f"[BOT] Failed to send message: {e}")
        else:
            lines = message.split("\n")
            await self._reply_lines(lines)

    async def _reply_lines(self, lines: list[str]):
        for line in lines:
            if not line.strip():
                continue
            try:
                await self.highrise.chat(line[:200])
                await asyncio.sleep(0.6)
            except Exception as e:
                print(f"[BOT] Failed to send line: {e}")

    async def on_user_join(self, user: User, position: Position | AnchorPosition) -> None:
        print(f"[BOT] User joined: {user.username}")

    async def on_emote(self, user: User, emote_id: str, receiver: User | None) -> None:
        pass

    async def on_tip(self, sender: User, receiver: User, tip) -> None:
        pass
