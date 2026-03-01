from __future__ import annotations

import asyncio
import logging
import os
import shlex
import shutil
from dataclasses import dataclass
from asyncio import subprocess as asp

import discord

FFMPEG_BEFORE_OPTS = "-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5"
FFMPEG_OPTIONS = "-vn"

logger = logging.getLogger(__name__)


async def yt_dlp_get_url(query: str) -> (str, str, str):
    """Return (direct_audio_url, title) for a YouTube link or search query using yt_dlp Python API.

    This runs the blocking `yt_dlp.YoutubeDL.extract_info` inside a thread to avoid
    requiring the external `yt-dlp` executable on PATH.
    """
    import yt_dlp

    # prepare the query: use ytsearch1: for plain search terms
    url = query if (query.startswith("http://") or query.startswith("https://")) else f"ytsearch1:{query}"

    def extract():
        import shutil as _sh
        has_deno = bool(_sh.which("deno"))
        ydl_opts = {"format": "bestaudio/best", "quiet": True, "no_warnings": True}
        if has_deno:
            ydl_opts["jsruntimes"] = "deno"
        import sys, io
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            old_out, old_err = sys.stdout, sys.stderr
            sys.stdout, sys.stderr = io.StringIO(), io.StringIO()
            try:
                info = ydl.extract_info(url, download=False)
            finally:
                sys.stdout, sys.stderr = old_out, old_err
            return info

    info = await asyncio.to_thread(extract)

    # info may be a search result with 'entries'
    if not info:
        raise RuntimeError("yt_dlp returned no info")

    if "entries" in info and info["entries"]:
        info = info["entries"][0]

    # title
    title = info.get("title") or info.get("id")

    # original webpage url if available
    webpage_url = info.get("webpage_url") or info.get("original_url")

    # try direct URL from info or formats
    stream_url = info.get("url")
    if not stream_url and "formats" in info:
        # pick best audio-only format if possible
        best = None
        for f in reversed(info.get("formats", [])):
            if f.get("vcodec") in (None, "none"):
                best = f
                break
        if not best:
            best = info["formats"][-1]
        stream_url = best.get("url")

    if not stream_url:
        raise RuntimeError("Could not determine a direct stream URL from yt_dlp info")

    return stream_url, title, webpage_url


@dataclass
class Track:
    title: str
    source_url: str
    webpage_url: str | None = None


class MusicPlayer:
    def __init__(self, bot: discord.Bot, guild: discord.Guild, start_task: bool = True):
        self.bot = bot
        self.guild = guild
        self.queue = asyncio.Queue()
        self.next_event = asyncio.Event()
        self.current: Track | None = None
        self.voice_client: discord.VoiceClient | None = None
        self.task = None
        if start_task:
            self.task = bot.loop.create_task(self.player_loop())
        # locate ffmpeg
        self.ffmpeg = shutil.which("ffmpeg")
        if not self.ffmpeg:
            logging.getLogger(__name__).warning("ffmpeg not found on PATH; playback will fail until ffmpeg is installed")
        os.makedirs("tmp_audio", exist_ok=True)

    async def player_loop(self):
        while True:
            self.next_event.clear()
            track: Track = await self.queue.get()
            self.current = track
            logger.info(f"Now playing: {track.title} in guild {self.guild.id}")
            if not self.voice_client or not self.voice_client.is_connected():
                # try to reconnect or skip
                logger.warning("Voice client not connected; clearing current and skipping")
                continue

            # Try streaming first
            # decide whether to force download: prefer download when deno is missing
            force_download = os.environ.get("DMBOT_FORCE_DOWNLOAD") == "1" or not shutil.which("deno")
            logger.info("Playback decision: ffmpeg=%s deno=%s force_download=%s", self.ffmpeg, bool(shutil.which("deno")), force_download)
            print(f"[player] ffmpeg={self.ffmpeg} deno={bool(shutil.which('deno'))} force_download={force_download}")
            if not force_download:
                try:
                    if not self.ffmpeg:
                        raise RuntimeError("ffmpeg executable not found")
                    source = discord.FFmpegPCMAudio(
                        track.source_url,
                        executable=self.ffmpeg,
                        before_options=FFMPEG_BEFORE_OPTS,
                        options=FFMPEG_OPTIONS,
                    )

                    def _after(err):
                        if err:
                            logger.error("Player error: %s", err)
                        self.bot.loop.call_soon_threadsafe(self.next_event.set)

                    self.voice_client.play(source, after=_after)
                    await self.next_event.wait()
                    self.current = None
                    continue
                except Exception:
                    logger.exception("Streaming playback failed, attempting download fallback for %s", track.title)
                    print(f"[player] Streaming failed for {track.title}, falling back to download")

            # Fallback / forced: download the audio to a local file and play it
            try:
                import yt_dlp

                def download():
                    ydl_opts = {
                        "format": "bestaudio/best",
                        "outtmpl": os.path.join("tmp_audio", "%(id)s.%(ext)s"),
                        "quiet": True,
                        "no_warnings": True,
                        "noplaylist": True,
                    }
                    # only set jsruntimes if deno is present to avoid noisy warnings
                    if shutil.which("deno"):
                        ydl_opts["jsruntimes"] = "deno"

                    import sys, io
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        # prefer using the original webpage URL when available
                        url_to_download = track.webpage_url or track.source_url
                        old_out, old_err = sys.stdout, sys.stderr
                        sys.stdout, sys.stderr = io.StringIO(), io.StringIO()
                        try:
                            logger.info("Starting yt-dlp download for: %s", url_to_download)
                            info = ydl.extract_info(url_to_download, download=True)
                            logger.info("yt-dlp returned info: id=%s title=%s", info.get('id') if isinstance(info, dict) else None, info.get('title') if isinstance(info, dict) else None)
                        finally:
                            sys.stdout, sys.stderr = old_out, old_err

                        if not info:
                            raise RuntimeError("yt_dlp.extract_info returned no info for download")

                        # If a playlist/dict with entries was returned, pick the first entry
                        entry = info
                        if isinstance(info, dict) and "entries" in info and info["entries"]:
                            entry = info["entries"][0]

                        filename = ydl.prepare_filename(entry)
                        logger.info("Prepared filename: %s", filename)
                        if not filename:
                            raise RuntimeError("Could not determine downloaded filename")
                        return filename

                try:
                    filename = await asyncio.wait_for(asyncio.to_thread(download), timeout=120)
                    logger.info("Downloaded fallback file: %s", filename)
                except asyncio.TimeoutError:
                    logger.error("yt-dlp download timed out for %s", track.source_url)
                    raise
                except Exception:
                    logger.exception("yt-dlp download failed for %s", track.source_url)
                    raise

                if not filename or not os.path.exists(filename):
                    raise RuntimeError("Downloaded file not found: %s" % filename)

                # For local files, do not pass reconnect input options (they're only for network streams)
                source = discord.FFmpegPCMAudio(filename, executable=self.ffmpeg, before_options="", options=FFMPEG_OPTIONS)

                def _after_local(err):
                    if err:
                        logger.error("Local playback error: %s", err)
                    # cleanup local file after play finishes
                    try:
                        os.remove(filename)
                    except Exception:
                        pass
                    self.bot.loop.call_soon_threadsafe(self.next_event.set)

                self.voice_client.play(source, after=_after_local)
                await self.next_event.wait()
                self.current = None
                continue
            except Exception:
                logger.exception("Download-and-play fallback failed for %s", track.title)
                # ensure we don't block the loop
                self.bot.loop.call_soon_threadsafe(self.next_event.set)
            await self.next_event.wait()
            self.current = None

    async def enqueue(self, track: Track):
        await self.queue.put(track)

    def is_playing(self) -> bool:
        return self.current is not None or (self.voice_client and self.voice_client.is_playing())

    async def stop(self):
        if self.voice_client and self.voice_client.is_connected():
            await self.voice_client.disconnect()
        # drain queue
        while not self.queue.empty():
            try:
                self.queue.get_nowait()
            except Exception:
                break
