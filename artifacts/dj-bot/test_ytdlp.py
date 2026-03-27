import yt_dlp
import json

ydl_opts = {
    "format": "bestaudio[ext=webm]/bestaudio/best",
    "noplaylist": True,
    "quiet": True,
    "no_warnings": True,
    "extract_flat": False,
}

song_name = "shape of you"
with yt_dlp.YoutubeDL(ydl_opts) as ydl:
    info = ydl.extract_info(f"ytsearch1:{song_name}", download=False)
    if info and info.get("entries") and len(info["entries"]) > 0:
        entry = info["entries"][0]
        title = entry.get("title", song_name)
        url = entry.get("url")
        print(f"TITLE: {title}")
        print(f"URL: {url is not None}")
        
    else:
        print("NO ENTRIES")
