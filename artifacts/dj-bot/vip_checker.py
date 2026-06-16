import aiohttp
import asyncio

VIP_API_BASE = "https://yash9970-highrisebotchaichai.hf.space"


async def get_vips() -> list[str]:
    try:
        import time
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{VIP_API_BASE}/vips?t={int(time.time())}", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, list):
                        return [str(v).lower() for v in data]
                    if isinstance(data, dict):
                        vips = data.get("vips", data.get("data", []))
                        return [str(v).lower() for v in vips]
    except Exception as e:
        print(f"[VIP] Failed to fetch VIPs: {e}")
    return []


async def is_vip(username: str) -> bool:
    vips = await get_vips()
    return username.lower() in vips


async def get_mods() -> list[str]:
    try:
        import time
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{VIP_API_BASE}/mods?t={int(time.time())}", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, list):
                        return [str(v).lower() for v in data]
                    if isinstance(data, dict):
                        return [str(v).lower() for v in data.get("mods", [])]
    except Exception as e:
        print(f"[VIP] Failed to fetch Mods: {e}")
    return []


async def is_mod(username: str) -> bool:
    mods = await get_mods()
    return username.lower() in mods


async def get_dj_pos() -> dict | None:
    try:
        import time
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{VIP_API_BASE}/djpos?t={int(time.time())}", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    return await resp.json()
    except Exception as e:
        print(f"[API] Failed to fetch DJ pos: {e}")
    return None


async def set_dj_pos(x: float, y: float, z: float, facing: str) -> bool:
    try:
        async with aiohttp.ClientSession() as session:
            payload = {"x": x, "y": y, "z": z, "facing": facing}
            async with session.post(f"{VIP_API_BASE}/djpos", json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                return resp.status == 200
    except Exception as e:
        print(f"[API] Failed to set DJ pos: {e}")
    return False
