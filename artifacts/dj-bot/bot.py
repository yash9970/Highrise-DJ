import os
import asyncio
from highrise import BaseBot, SessionMetadata, User, Position, AnchorPosition
from db import init_db, add_song, get_queue, get_next_song, delete_song
from vip_checker import is_vip
from streamer import broadcaster

BOT_POSITION = Position(18.0, 0.0, 13.5, facing="FrontRight")

DANCE_EMOTES = [
    "dance-tiktok8",
    "dance-blackpink",
    "dance-pennywise",
    "idle-dance-casual",
    "dance-tiktok2",
]

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
    "!dj skip — Skip current song (mod/master)",
    "!dj clear — Clear queue (master)",
    "!dj listeners — Listener count (master)",
    "!dj wear <id> — Wear item (master)",
    "!dj unwear <id> — Remove item (master)",
    "!dj setbot — Move bot to your location (master)",
    "!dj help — This message",
]

# Module-level: survives bot reconnects so trending index doesn't reset to 0
_active_tasks: list[asyncio.Task] = []
_trending_index: int = 0


def _cancel_all_tasks():
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
        # True only while the fallback/trending song is playing
        # (not queued requests — so the play handler knows to interrupt)
        self._is_fallback_playing: bool = False

    async def on_start(self, session_metadata: SessionMetadata) -> None:
        self._owner_id = session_metadata.room_info.owner_id
        self.bot_id = session_metadata.user_id
        print(f"[BOT] DJ Bot started. Bot ID: {session_metadata.user_id}")
        print(f"[BOT] Room owner ID: {self._owner_id} (this is the master)")

        try:
            _cancel_all_tasks()

            print("[BOT] Initializing database...")
            await asyncio.to_thread(init_db)
            
            await asyncio.sleep(8)
            
            # Load custom position from Helper API
            teleport_pos = BOT_POSITION
            try:
                from vip_checker import get_dj_pos
                pos_data = await get_dj_pos()
                if pos_data:
                    teleport_pos = Position(pos_data["x"], pos_data["y"], pos_data["z"], facing=pos_data.get("facing", "FrontLeft"))
                    print(f"[BOT] Loaded DJ pos from API: {teleport_pos}")
            except Exception as e:
                print(f"[BOT] Failed to get DJ pos from API: {e}. Using default.")

            # Flawless Ghost Session Check:
            # Try to teleport. If it fails with "Not in room", check if the room is asleep (only bot inside).
            # If the room has real players inside and we still get "Not in room", it's a Ghost Session!
            try:
                await self.highrise.teleport(session_metadata.user_id, teleport_pos)
                print(f"[BOT] Teleported to position: {teleport_pos}")
            except Exception as e:
                if "not in room" in str(e).lower() or "server error" in str(e).lower():
                    resp = await self.highrise.get_room_users()
                    if hasattr(resp, "content") and len(resp.content) > 1:
                        print(f"[BOT] CRITICAL: Room is awake but I can't move! Ghost session detected!")
                        print(f"[BOT] Disconnecting and sleeping for 65 seconds to clear the old session...")
                        await asyncio.sleep(65)
                        import os
                        os._exit(1)
                    else:
                        print(f"[BOT] WARNING: Could not teleport (Empty room hibernation?). Bot will stay at door.")
                else:
                    print(f"[BOT] Initial teleport failed: {e}")

            await asyncio.sleep(2)

            self._dance_task = asyncio.create_task(self._dance_loop())
            self._talk_task  = asyncio.create_task(self._talk_loop())
            self._song_task  = asyncio.create_task(self._song_loop())
            self._auto_dance_task = asyncio.create_task(self._auto_dance_loop())
            self._ping_task  = asyncio.create_task(self._ping_loop())
            self._pos_task   = asyncio.create_task(self._position_check_loop())
            _active_tasks.extend([self._dance_task, self._talk_task, self._song_task, self._auto_dance_task, self._ping_task, self._pos_task])
            print("[BOT] Background tasks started (dance, talk, song, auto-dance, ping, pos-check).")
            
        except Exception as e:
            print(f"\n[CRITICAL ERROR] Bot crashed during startup: {e}")
            print("Did you forget to add the DATABASE_URL in Render Environment Variables?\n")
            import traceback
            traceback.print_exc()
            raise e

    # ── Background loops ──────────────────────────────────────────────────────

    async def _ping_loop(self):
        import aiohttp
        while True:
            try:
                await asyncio.sleep(5 * 60)
                async with aiohttp.ClientSession() as session:
                    await session.get("https://yash9970-highrisebotchaichai.hf.space/ping")
                    await session.get("https://yash9970-highrise-dj.hf.space/health")
                print("[BOT] Keepalive ping sent to both servers to prevent sleep")
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[BOT] Ping error: {e}")

    async def _position_check_loop(self):
        """Every 15 seconds, check if room woke up, and teleport exactly once."""
        await asyncio.sleep(10)
        self.room_is_live = False
        empty_count = 0
        while True:
            try:
                await asyncio.sleep(15)
                resp = await self.highrise.get_room_users()
                num_users = len(resp.content) if hasattr(resp, "content") else 0
                print(f"[BOT] PosCheck: DJ Bot sees {num_users} users in room.")
                
                # If there is more than 1 user, room is awake
                if num_users > 1:
                    empty_count = 0
                    if not self.room_is_live:
                        self.room_is_live = True
                        from vip_checker import get_dj_pos
                        pos_data = await get_dj_pos()
                        if pos_data:
                            teleport_pos = Position(pos_data["x"], pos_data["y"], pos_data["z"], facing=pos_data.get("facing", "FrontLeft"))
                        else:
                            teleport_pos = BOT_POSITION
                        await self.highrise.teleport(self.bot_id, teleport_pos)
                        print("[BOT] Room woke up. Spawned bot successfully.")
                else:
                    self.room_is_live = False
                    empty_count += 1
                    if empty_count >= 12:
                        print("[BOT] Room empty for 3 minutes. Restarting to clear ghost instance!")
                        import os
                        os._exit(1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[BOT] PosCheck error: {e}")

    async def _dance_loop(self):
        i = 0
        while True:
            try:
                if not self.room_is_live:
                    await asyncio.sleep(10)
                    continue
                emote = DANCE_EMOTES[i % len(DANCE_EMOTES)]
                await self.highrise.send_emote(emote)
                print(f"[BOT] Dancing: {emote}")
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
                if not self.room_is_live:
                    await asyncio.sleep(10)
                    continue
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

    async def _auto_dance_loop(self):
        """
        Periodically forces everyone inside the dance floor bounding box to dance.
        """
        import random
        from db import get_floor_bounds
        
        await asyncio.sleep(5)  # Let bot initialize
        while True:
            try:
                bounds = await asyncio.to_thread(get_floor_bounds)
                if bounds:
                    # Get all users in the room
                    resp = await self.highrise.get_room_users()
                    for room_user, pos in getattr(resp, "content", []):
                        if room_user.id == self.highrise.my_id:
                            continue  # Skip bot
                        
                        # Type-check position (could be AnchorPosition which doesn't have xyz)
                        if isinstance(pos, Position):
                            if (bounds["min_x"] <= pos.x <= bounds["max_x"] and
                                bounds["min_y"] <= pos.y <= bounds["max_y"] and
                                bounds["min_z"] <= pos.z <= bounds["max_z"]):
                                
                                # Send random dance emote to force them to dance
                                emote = random.choice(DANCE_EMOTES)
                                try:
                                    # Target _them_ with the dance so their avatar plays it
                                    await self.highrise.send_emote(emote, room_user.id)
                                except Exception:
                                    pass
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[BOT] Auto-dance loop error: {e}")
                await asyncio.sleep(5)
            
            # Run every 15 seconds to switch up their dance or catch new arrivals
            await asyncio.sleep(15)

    async def _song_loop(self):
        """
        Single song loop — never spawns a sibling task.
        Runs forever picking queued songs first, then trending fallbacks.

        KEY INVARIANT: only this one task ever calls broadcaster.play().
        The skip/interrupt commands just call broadcaster.stop_current() to
        break the current play() call — this loop then naturally moves on.
        """
        global _trending_index

        while True:
            try:
                # DB query is synchronous (psycopg2) - MUST run in thread so we don't block
                # the asyncio event loop. A blocked loop = failing health checks = container reboot
                next_song = await asyncio.to_thread(get_next_song)

                # ── A queued request ──────────────────────────────────────
                if next_song:
                    song_name    = next_song["song_name"]
                    requested_by = next_song["requested_by"]
                    song_id      = next_song["id"]

                    self._is_fallback_playing = False

                    try:
                        await self.highrise.chat(
                            f"🔍 Searching: {song_name} (requested by {requested_by})..."
                        )
                    except Exception:
                        pass

                    print(f"[SONG] Playing queued song: {song_name!r} (id={song_id})")

                    # Capture song_name in closure to avoid late-binding issues
                    _sn = song_name
                    async def _on_queued_found(title: str, source: str, _s=_sn):
                        try:
                            await self.highrise.chat(f"▶️ Now playing: {title}")
                        except Exception:
                            pass

                    success, title = await broadcaster.play(song_name, on_found=_on_queued_found)

                    # Only announce "Done" if the song actually finished naturally
                    # (not if it was skipped/interrupted externally)
                    if success and not broadcaster.was_interrupted:
                        print(f"[SONG] Finished: {title!r}")
                        try:
                            await self.highrise.chat(
                                f"✅ Done: {title} — type !dj play <song> to request next!"
                            )
                        except Exception:
                            pass
                    elif not success and not broadcaster.was_interrupted:
                        print(f"[SONG] Not found: {song_name!r} — skipping")
                        try:
                            await self.highrise.chat(f"❌ Could not find '{song_name}'. Skipping!")
                        except Exception:
                            pass

                    # Always delete from queue when done, whether skipped or finished
                    await asyncio.to_thread(delete_song, song_id)
                    await asyncio.sleep(1)

                # ── Fallback trending ─────────────────────────────────────
                else:
                    trending_song = TRENDING_SONGS[_trending_index % len(TRENDING_SONGS)]
                    _trending_index += 1

                    print(f"[SONG] Queue empty — auto-playing: {trending_song!r}")
                    self._is_fallback_playing = True

                    try:
                        # Capture for closure
                        _ts = trending_song
                        async def _on_trending_found(title: str, source: str, _t=_ts):
                            try:
                                await self.highrise.chat(
                                    f"🎶 Now playing: {title} 🔥  |  !dj play <song> to request!"
                                )
                            except Exception:
                                pass

                        success, title = await broadcaster.play(
                            trending_song, on_found=_on_trending_found
                        )

                        if not success and not broadcaster.was_interrupted:
                            print(f"[SONG] Trending failed for {trending_song!r}. Backing off 30s.")
                            await asyncio.sleep(30)
                        else:
                            await asyncio.sleep(1)

                    except asyncio.CancelledError:
                        self._is_fallback_playing = False
                        raise
                    except Exception as e:
                        print(f"[SONG] Trending error: {e}")
                        await asyncio.sleep(30)
                    finally:
                        self._is_fallback_playing = False

            except asyncio.CancelledError:
                print("[SONG] Song loop cancelled.")
                break
            except Exception as e:
                print(f"[BOT] Song loop error: {e}")
                await asyncio.sleep(5)

    # ── Chat commands ─────────────────────────────────────────────────────────

    async def on_chat(self, user: User, message: str) -> None:
        msg = message.strip()
        print(f"[CHAT] {user.username}: {msg}")

        if not msg.lower().startswith("!dj"):
            return

        parts = msg.split(None, 2)
        if len(parts) < 2:
            return

        command = parts[1].lower()
        args    = parts[2].strip() if len(parts) > 2 else ""

        is_master  = (user.id == self._owner_id)
        authorized = await self._is_authorized(user, is_master)

        print(f"[CMD] '{command}' from '{user.username}' (master={is_master})")

        # ── help ─────────────────────────────────────────────────────────────
        if command == "help":
            if is_master:
                await self._reply_lines(HELP_LINES)
            else:
                await self._reply(f"@{user.username} Only the master can use !dj help.")
            return

        # ── inventory ────────────────────────────────────────────────────────
        if command == "inventory":
            if is_master:
                await self._handle_inventory(user, args)
            else:
                await self._reply(f"@{user.username} Only the master can use !dj inventory.")
            return

        # ── play ─────────────────────────────────────────────────────────────
        if command == "play":
            if not authorized:
                await self._reply(
                    f"@{user.username} Only VIPs, mods, or the master can request songs."
                )
                return
            if not args:
                await self._reply(f"@{user.username} Usage: !dj play <song name>")
                return

            await asyncio.to_thread(add_song, args, user.username)

            if self._is_fallback_playing:
                # Interrupt trending — the song loop will pick up the queued song next
                await broadcaster.stop_current(interrupted=True)
                await self._reply(f"⏭️ Interrupting radio — '{args}' loading next! (by {user.username})")
            else:
                # A queued song is either searching or playing
                queue = await asyncio.to_thread(get_queue)
                pos = len(queue)
                if broadcaster.is_active:
                    await self._reply(
                        f"🎵 Added '{args}' to queue at position {pos}! (by {user.username})"
                    )
                else:
                    await self._reply(f"🎵 Added '{args}' — loading next! (by {user.username})")
            return

        # ── queue ────────────────────────────────────────────────────────────
        if command == "queue":
            if not authorized:
                await self._reply(
                    f"@{user.username} Only VIPs, mods, or the master can view the queue."
                )
                return
            queue = await asyncio.to_thread(get_queue)
            if not queue:
                await self._reply("Queue is empty! Use !dj play <song> to request.")
            else:
                lines = ["📋 Song Queue:"]
                for i, song in enumerate(queue, 1):
                    # Position 1 = currently playing/searching if broadcaster is active
                    if i == 1 and broadcaster.is_active:
                        status = " ▶️ PLAYING" if broadcaster.is_playing else " 🔍 SEARCHING"
                    else:
                        status = ""
                    lines.append(f"{i}. {song['song_name']} by {song['requested_by']}{status}")
                await self._reply("\n".join(lines))
            return

        # ── np (now playing) ─────────────────────────────────────────────────
        if command in ("nowplaying", "np"):
            if broadcaster.is_playing and broadcaster.current_title:
                await self._reply(f"▶️ Now playing: {broadcaster.current_title}")
            elif broadcaster.is_active:
                await self._reply("🔍 Searching for next song...")
            else:
                await self._reply("Nothing is playing right now. Use !dj play <song>!")
            return

        # ── skip ─────────────────────────────────────────────────────────────
        if command == "skip":
            if not await self._is_bot_mod(user, is_master):
                await self._reply(f"@{user.username} Only mods or the master can skip songs.")
                return

            # FIX: Do NOT spawn a new _song_loop task. Just stop the current
            # stream — the existing loop will naturally move to the next song.
            # Spawning a new task was the root cause of concurrent loops,
            # duplicate announcements, and title/audio mismatches.
            next_song = await asyncio.to_thread(get_next_song)
            if next_song:
                await asyncio.to_thread(delete_song, next_song["id"])
                await self._reply(f"⏭️ Skipped! Loading next song...")
            else:
                await self._reply(f"⏭️ Skipped fallback radio!")

            await broadcaster.stop_current(interrupted=True)
            # The existing _song_loop is blocked at await broadcaster.play().
            # stop_current() kills the subprocess, play() returns, and the loop
            # naturally picks up the next song. No new task needed.
            return

        # ── clear ────────────────────────────────────────────────────────────
        if command == "clear":
            if not is_master:
                await self._reply(f"@{user.username} Only the master can clear the queue.")
                return
            from db import clear_queue
            await asyncio.to_thread(clear_queue)
            await broadcaster.stop_current(interrupted=True)
            await self._reply("🗑️ Queue cleared and playback stopped!")
            return

        # ── viplist ──────────────────────────────────────────────────────────
        if command == "viplist":
            if not is_master:
                await self._reply(f"@{user.username} Only the master can use !dj viplist.")
                return
            await self._handle_viplist()
            return

        # ── setbot ───────────────────────────────────────────────────────────
        if command == "setbot":
            if not is_master:
                await self._reply(f"@{user.username} Only the master can move the bot.")
                return

            # Get user's current position
            resp = await self.highrise.get_room_users()
            user_pos = None
            for room_user, pos in getattr(resp, "content", []):
                if room_user.id == user.id:
                    user_pos = pos
                    break

            if not isinstance(user_pos, Position):
                await self._reply("❌ Couldn't find your coordinates (are you on an anchor?).")
                return

            try:
                # Teleport the bot to the user's position
                await self.highrise.teleport(self.highrise.my_id, user_pos)
                
                # Save to Helper API so it persists on restart
                from vip_checker import set_dj_pos
                success = await set_dj_pos(user_pos.x, user_pos.y, user_pos.z, user_pos.facing)
                
                if success:
                    await self._reply(f"✅ Bot teleported and saved to permanent database!")
                else:
                    await self._reply(f"✅ Bot teleported, but failed to save to database.")
            except Exception as e:
                await self._reply(f"❌ Failed to teleport: {e}")
            return

        # ── wear ─────────────────────────────────────────────────────────────
        if command == "wear":
            if not is_master:
                await self._reply(f"@{user.username} Only the master can use !dj wear.")
                return
            item_id = args.strip()
            if not item_id:
                await self._reply("Usage: !dj wear <item_id>")
                return
            try:
                from highrise.models import Item
                outfit_resp = await self.highrise.get_my_outfit()
                outfit = getattr(outfit_resp, "outfit", [])
                if any(i.id == item_id for i in outfit):
                    await self._reply(f"Already wearing '{item_id}'.")
                    return
                new_item = Item(type="clothing", amount=1, id=item_id)
                new_outfit = list(outfit) + [new_item]
                await self.highrise.set_outfit(new_outfit)
                await self._reply(f"✅ Equipped: {item_id}")
            except Exception as e:
                await self._reply(f"❌ Couldn't equip '{item_id}': {e}")
            return

        # ── unwear ───────────────────────────────────────────────────────────
        if command == "unwear":
            if not is_master:
                await self._reply(f"@{user.username} Only the master can use !dj unwear.")
                return
            item_id = args.strip()
            if not item_id:
                await self._reply("Usage: !dj unwear <item_id>")
                return
            try:
                outfit_resp = await self.highrise.get_my_outfit()
                outfit = getattr(outfit_resp, "outfit", [])
                new_outfit = [i for i in outfit if i.id != item_id]
                if len(new_outfit) == len(outfit):
                    await self._reply(f"'{item_id}' is not in the current outfit.")
                    return
                await self.highrise.set_outfit(new_outfit)
                await self._reply(f"✅ Removed: {item_id}")
            except Exception as e:
                await self._reply(f"❌ Couldn't remove '{item_id}': {e}")
            return

        # ── listeners ────────────────────────────────────────────────────────
        if command == "listeners":
            if is_master:
                await self._reply(f"📻 Radio listeners: {broadcaster.listener_count}")
            return

        # ── floor bounds ─────────────────────────────────────────────────────
        if command == "floor":
            if not is_master:
                await self._reply(f"@{user.username} Only the master can set the dance floor!")
                return
            
            if not args or args not in ("1", "2", "off"):
                await self._reply("Usage: !dj floor 1 | !dj floor 2 | !dj floor off\nStand in corners to set bounds.")
                return
            
            from db import set_floor_corner, clear_floor_bounds
            if args == "off":
                await asyncio.to_thread(clear_floor_bounds)
                await self._reply("🛑 Auto-dance floor disabled.")
                return
                
            # Get user's current position
            resp = await self.highrise.get_room_users()
            user_pos = None
            for room_user, pos in getattr(resp, "content", []):
                if room_user.id == user.id:
                    user_pos = pos
                    break
            
            if not isinstance(user_pos, Position):
                await self._reply("❌ Couldn't find your coordinates (are you on an anchor?).")
                return
                
            corner = int(args)
            await asyncio.to_thread(set_floor_corner, corner, user_pos.x, user_pos.y, user_pos.z)
            await self._reply(f"✅ Corner {corner} saved at ({user_pos.x:.1f}, {user_pos.y:.1f}, {user_pos.z:.1f})! "
                              f"{'Set the other corner to activate!' if corner == 1 else 'Floor active!'}")
            return

    # ── Auth helpers ──────────────────────────────────────────────────────────

    async def _is_bot_mod(self, user: User, is_master: bool) -> bool:
        if is_master:
            return True
        try:
            room_users = await self.highrise.get_room_users()
            for room_user, _ in room_users.content:
                if room_user.id == user.id:
                    priv = getattr(room_user, "privilege", None)
                    if priv in ("moderator", "designer"):
                        return True
                    break
        except Exception as e:
            print(f"[BOT] Could not check room privileges: {e}")

        from vip_checker import is_mod
        return await is_mod(user.username)

    async def _is_authorized(self, user: User, is_master: bool) -> bool:
        if await self._is_bot_mod(user, is_master):
            return True
        from vip_checker import is_vip
        return await is_vip(user.username)

    # ── Feature handlers ──────────────────────────────────────────────────────

    async def _handle_viplist(self):
        from vip_checker import get_vips, get_mods, VIP_API_BASE
        try:
            vips = await get_vips()
            mods = await get_mods()
            lines = [
                f"Helper bot ({VIP_API_BASE}): ✅ online",
                f"VIPs ({len(vips)}): " + (", ".join(vips) if vips else "none"),
                f"Mods ({len(mods)}): " + (", ".join(mods) if mods else "none"),
            ]
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
                await self._reply("No outfit items found.")
        except Exception as e:
            print(f"[BOT] Inventory error: {e}")
            await self._reply("Equip items in-game while logged in as the bot account.")

    # ── Messaging helpers ─────────────────────────────────────────────────────

    async def _reply(self, message: str):
        if len(message) <= 200:
            try:
                await self.highrise.chat(message)
            except Exception as e:
                print(f"[BOT] Failed to send message: {e}")
        else:
            await self._reply_lines(message.split("\n"))

    async def _reply_lines(self, lines: list[str]):
        for line in lines:
            if not line.strip():
                continue
            try:
                await self.highrise.chat(line[:200])
                await asyncio.sleep(0.6)
            except Exception as e:
                print(f"[BOT] Failed to send line: {e}")

    # ── Highrise event handlers ───────────────────────────────────────────────

    async def on_user_join(self, user: User, position: Position | AnchorPosition) -> None:
        print(f"[BOT] User joined: {user.username}")
        # When a user joins, the room wakes up. Teleport to the DJ position just in case we were stuck at the door!
        # Wait 2 seconds for the room's physics mesh to fully load before teleporting
        try:
            await asyncio.sleep(2.0)
            from vip_checker import get_dj_pos
            pos_data = await get_dj_pos()
            if pos_data:
                teleport_pos = Position(pos_data["x"], pos_data["y"], pos_data["z"], facing=pos_data.get("facing", "FrontLeft"))
            else:
                teleport_pos = BOT_POSITION
            await self.highrise.teleport(self.bot_id, teleport_pos)
        except Exception:
            pass

    async def on_emote(self, user: User, emote_id: str, receiver: User | None) -> None:
        pass

    async def on_tip(self, sender: User, receiver: User, tip) -> None:
        pass
