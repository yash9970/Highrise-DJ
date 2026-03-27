import aiohttp
import asyncio

VIP_API_BASE = "https://highrise-helper.onrender.com"


async def get_vips() -> list[str]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{VIP_API_BASE}/vips", timeout=aiohttp.ClientTimeout(total=10)) as resp:
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
