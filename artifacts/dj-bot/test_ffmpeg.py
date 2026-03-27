import asyncio
import yt_dlp

async def main():
    song_name = "shape of you"
    ydl_opts = {
        "format": "bestaudio/best",
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "extract_flat": False,
    }

    url = None
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(f"scsearch1:{song_name}", download=False)
        if info and info.get("entries") and len(info["entries"]) > 0:
            entry = info["entries"][0]
            url = entry.get("url")
            print(f"Got URL: {url}")

    if not url:
        print("Failed to get URL.")
        return

    print(f"Final URL grabbed from yt-dlp: {url}")
    print(f"Headers: {entry.get('http_headers')}")
    
asyncio.run(main())
