from __future__ import annotations

import asyncio
import logging
import os
import shlex
import shutil
import random
from collections import deque
import re
from dataclasses import dataclass
from asyncio import subprocess as asp
from typing import Optional
import json
import time
from typing import List, Tuple

import discord
import yt_dlp  # type: ignore
from worker import download_to_file

FFMPEG_BEFORE_OPTS = "-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5"
FFMPEG_OPTIONS = "-vn"

# persistent autoplay history
HISTORY_FILE = os.environ.get("DMBOT_AUTOPLAY_HISTORY_FILE", "autoplay_history.json")
HISTORY_MAXLEN = int(os.environ.get("DMBOT_AUTOPLAY_HISTORY_MAXLEN", "512"))
FAILED_QUERY_TTL = int(os.environ.get("DMBOT_AUTOPLAY_FAILED_TTL", "300"))  # seconds to avoid re-querying recently-failed search strings
FAILED_QUERIES_FILE = os.environ.get("DMBOT_AUTOPLAY_FAILED_FILE", "autoplay_failed.json")

logger = logging.getLogger(__name__)


async def yt_dlp_get_url(query: str, max_results: int = 1, exclude_webpage: str | None = None) -> tuple[str, str, str, bool]:
    """Return (direct_audio_url, title) for a YouTube link or search query using yt_dlp Python API.

    This runs the blocking `yt_dlp.YoutubeDL.extract_info` inside a thread to avoid
    requiring the external `yt-dlp` executable on PATH.
    """

    # prepare the query: use ytsearchN: for plain search terms so we can request multiple results
    if query.startswith("http://") or query.startswith("https://"):
        url = query
    else:
        n = max_results or 1
        try:
            n_int = int(n)
        except Exception:
            n_int = 1
        if n_int > 1:
            url = f"ytsearch{n_int}:{query}"
        else:
            url = f"ytsearch1:{query}"

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
        # if caller asked for multiple results, pick a random entry (optionally excluding a webpage)
        entries = [e for e in info["entries"] if e]
        if max_results is None or max_results <= 1:
            info = entries[0]
        else:
            candidates = entries
            if exclude_webpage:
                candidates = [e for e in entries if (e.get("webpage_url") or e.get("original_url")) != exclude_webpage]
                if not candidates:
                    # nothing left after exclusion; fall back to original entries
                    candidates = entries
            # limit to the first `max_results` candidates for performance, then pick randomly
            try:
                info = random.choice(candidates[:max_results])
            except Exception:
                info = entries[0]

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
        # No direct stream URL available; return webpage_url for download fallback instead
        return None, title, webpage_url, bool(info.get("is_live") or info.get("live_status"))

    # detect live streams if yt-dlp exposes it
    is_live = bool(info.get("is_live") or info.get("live_status"))

    return stream_url, title, webpage_url, is_live


async def yt_dlp_get_candidates(query: str, max_results: int = 10, exclude_webpage: str | None = None) -> List[Tuple[Optional[str], Optional[str], Optional[str], bool]]:
    """Return a list of candidate tuples (stream_url, title, webpage_url, is_live).

    This aggregates multiple yt-dlp entries and returns them for local shuffling/filtering.
    """
    # prepare the query like yt_dlp_get_url does
    if query.startswith("http://") or query.startswith("https://"):
        url = query
    else:
        n = max_results or 1
        try:
            n_int = int(n)
        except Exception:
            n_int = 1
        if n_int > 1:
            url = f"ytsearch{n_int}:{query}"
        else:
            url = f"ytsearch1:{query}"

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
    if not info:
        return []

    entries = []
    if "entries" in info and info["entries"]:
        entries = [e for e in info["entries"] if e]
    else:
        entries = [info]

    candidates = []
    for e in entries[: max(1, max_results)]:
        title = e.get("title") or e.get("id")
        webpage_url = e.get("webpage_url") or e.get("original_url")
        stream_url = e.get("url")
        if not stream_url and "formats" in e:
            best = None
            for f in reversed(e.get("formats", [])):
                if f.get("vcodec") in (None, "none"):
                    best = f
                    break
            if not best and e.get("formats"):
                best = e["formats"][-1]
            if best:
                stream_url = best.get("url")
        is_live = bool(e.get("is_live") or e.get("live_status"))
        candidates.append((stream_url, title, webpage_url, is_live))

    return candidates


@dataclass
class Track:
    title: str
    source_url: str
    webpage_url: str | None = None
    filename: str | None = None
    prefetched: bool = False
    is_live: bool = False


class MusicPlayer:
    def __init__(self, bot: discord.Bot, guild: discord.Guild, start_task: bool = True):
        self.bot = bot
        self.guild = guild
        self.queue = asyncio.Queue()
        self.next_event = asyncio.Event()
        self.current: Track | None = None
        self.last_played: Track | None = None
        # persistent per-guild history of recently-played webpage_urls (or source urls) to avoid repeats
        self._history_file = HISTORY_FILE
        self._history_maxlen = HISTORY_MAXLEN
        self._persisted_history: dict[str, list[str]] = {}
        # track recently-failed search queries to avoid spamming yt-dlp
        self._failed_queries: dict[str, float] = {}
        self._failed_queries_file = FAILED_QUERIES_FILE
        # autoplay genre/category if set by command (e.g. 'hip hop')
        self.autoplay_genre: str | None = None
        try:
            if os.path.exists(self._history_file):
                with open(self._history_file, "r", encoding="utf-8") as _hf:
                    data = json.load(_hf)
                    if isinstance(data, dict):
                        # migrate keys to strings
                        self._persisted_history = {str(k): list(v) for k, v in data.items() if isinstance(v, list)}
        except Exception:
            logger.exception("Failed to load autoplay history; continuing with empty history")
        # load failed queries and prune expired
        try:
            if os.path.exists(self._failed_queries_file):
                with open(self._failed_queries_file, "r", encoding="utf-8") as _ff:
                    fdata = json.load(_ff)
                    if isinstance(fdata, dict):
                        now = time.time()
                        for k, v in fdata.items():
                            try:
                                ts = float(v)
                            except Exception:
                                continue
                            if now - ts < FAILED_QUERY_TTL:
                                self._failed_queries[k] = ts
        except Exception:
            logger.exception("Failed to load failed-query cache; continuing")
        guild_key = str(self.guild.id)
        initial_hist = self._persisted_history.get(guild_key, [])[-self._history_maxlen:]
        self.play_history: deque[str] = deque(initial_hist, maxlen=self._history_maxlen)
        self.autoplay: bool = False
        # buffer of prefetched autoplay tracks (not in main queue)
        self.autoplay_buffer: deque[Track] = deque()
        self._autoplay_fill_task: asyncio.Task | None = None
        self.last_voice_channel_id: int | None = None
        self._prefetch_tasks: dict[str, asyncio.Task] = {}
        self.voice_client: discord.VoiceClient | None = None
        self.task = None
        if start_task:
            self.task = bot.loop.create_task(self.player_loop())
        # locate ffmpeg
        self.ffmpeg = shutil.which("ffmpeg")
        if not self.ffmpeg:
            logging.getLogger(__name__).warning("ffmpeg not found on PATH; playback will fail until ffmpeg is installed")
        os.makedirs("tmp_audio", exist_ok=True)

    def _record_play(self, track: Track):
        """Record the last played track and remember a key to avoid immediate repeats."""
        self.last_played = track
        try:
            key = track.webpage_url or track.source_url or track.title
            if key:
                self.play_history.append(key)
                # persist per-guild history (append and trim)
                gk = str(self.guild.id)
                lst = self._persisted_history.get(gk, [])
                lst.append(key)
                if len(lst) > self._history_maxlen:
                    lst = lst[-self._history_maxlen:]
                self._persisted_history[gk] = lst
                # background save to avoid blocking
                try:
                    self.bot.loop.create_task(asyncio.to_thread(self._save_history_sync))
                except Exception:
                    # best-effort synchronous fallback
                    try:
                        self._save_history_sync()
                    except Exception:
                        pass
        except Exception:
            pass

    def _save_history_sync(self):
        """Write the persisted history atomically to disk (synchronous)."""
        try:
            tmp = f"{self._history_file}.tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._persisted_history, f)
            os.replace(tmp, self._history_file)
        except Exception:
            logger.exception("Failed to save autoplay history to %s", self._history_file)

    def _save_failed_queries_sync(self):
        """Write the failed-queries cache atomically to disk (synchronous)."""
        try:
            tmp = f"{self._failed_queries_file}.tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._failed_queries, f)
            os.replace(tmp, self._failed_queries_file)
        except Exception:
            logger.exception("Failed to save failed-queries to %s", self._failed_queries_file)

    async def player_loop(self):
        while True:
            # autoplay: if enabled and nothing queued, play from autoplay buffer (prefetched)
            if self.autoplay and self.current is None and self.queue.empty():
                if self.voice_client and self.voice_client.is_connected():
                    try:
                        # ensure buffer exists
                        if not self.autoplay_buffer:
                            # fill buffer but don't block playback for too long
                            try:
                                await self.fill_autoplay_buffer(5)
                            except Exception:
                                logger.exception("Autoplay fill failed")
                        if self.autoplay_buffer:
                            next_track = self.autoplay_buffer.popleft()
                            await self.enqueue(next_track)
                    except Exception:
                        logger.exception("Autoplay enqueue failed")

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
            # but never force-download live streams (they should be streamed)
            force_download = (os.environ.get("DMBOT_FORCE_DOWNLOAD") == "1" or not shutil.which("deno")) and not getattr(track, "is_live", False)
            logger.info("Playback decision: ffmpeg=%s deno=%s force_download=%s", self.ffmpeg, bool(shutil.which("deno")), force_download)
            print(f"[player] ffmpeg={self.ffmpeg} deno={bool(shutil.which('deno'))} force_download={force_download}")
            # If a prefetched local file exists, play it directly
            if track.filename and os.path.exists(track.filename):
                try:
                    source = discord.FFmpegPCMAudio(track.filename, executable=self.ffmpeg, before_options="", options=FFMPEG_OPTIONS)

                    def _after_local_prefetched(err):
                        if err:
                            logger.error("Local playback error: %s", err)
                        # do not remove prefetched file
                        self.bot.loop.call_soon_threadsafe(self.next_event.set)

                    self.voice_client.play(source, after=_after_local_prefetched)
                    await self.next_event.wait()
                    self._record_play(track)
                    try:
                        # refill both queue prefetch and autoplay buffer in background
                        self.bot.loop.create_task(self.ensure_prefetch_ahead(6))
                        self.bot.loop.create_task(self.fill_autoplay_buffer(5))
                    except Exception:
                        logger.exception("refill tasks failed after play")
                    self.current = None
                    continue
                except Exception:
                    logger.exception("Playing prefetched file failed, falling back")

            if not force_download and getattr(track, 'source_url', None):
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
                    self._record_play(track)
                    try:
                        self.bot.loop.create_task(self.ensure_prefetch_ahead(6))
                        self.bot.loop.create_task(self.fill_autoplay_buffer(5))
                    except Exception:
                        logger.exception("refill tasks failed after play")
                    self.current = None
                    continue
                except Exception:
                    logger.exception("Streaming playback failed for %s", track.title)
                    # If this was a live track, avoid attempting to download the stream
                    if getattr(track, "is_live", False):
                        logger.info("Streaming failed for live track '%s'; skipping download fallback", track.title)
                        # ensure loop doesn't block on next_event
                        self.bot.loop.call_soon_threadsafe(self.next_event.set)
                        self.current = None
                        continue
                    # otherwise fall back to download
                    print(f"[player] Streaming failed for {track.title}, falling back to download")
            else:
                # no direct stream URL available or force_download requested; proceed to download fallback
                logger.debug("Streaming skipped (no direct URL) for '%s', proceeding to download/fallback", track.title)

            # Fallback / forced: download the audio to a local file and play it
            try:
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
                    # If a prefetch for this URL is in progress, wait for it instead
                    url_to_download = track.webpage_url or track.source_url
                    if url_to_download and url_to_download in self._prefetch_tasks:
                        pre_t = self._prefetch_tasks[url_to_download]
                        try:
                            await asyncio.wait_for(pre_t, timeout=30)
                        except Exception:
                            # prefetch failed or timed out; we'll proceed to download
                            logger.info("Prefetch task did not produce a file or timed out; proceeding to download")
                        # if prefetch set the filename on the track, use it
                        if track.filename and os.path.exists(track.filename):
                            filename = track.filename
                            logger.info("Using prefetched file from task: %s", filename)
                        else:
                            filename = await asyncio.wait_for(asyncio.to_thread(download), timeout=120)
                            logger.info("Downloaded fallback file: %s", filename)
                    else:
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
                track.filename = filename
                track.prefetched = track.prefetched or False
                source = discord.FFmpegPCMAudio(filename, executable=self.ffmpeg, before_options="", options=FFMPEG_OPTIONS)

                def _after_local(err):
                    if err:
                        logger.error("Local playback error: %s", err)
                    # cleanup local file after play finishes only if it wasn't a prefetched file
                    try:
                        if not getattr(track, "prefetched", False):
                            os.remove(filename)
                    except Exception:
                        pass
                    self.bot.loop.call_soon_threadsafe(self.next_event.set)

                self.voice_client.play(source, after=_after_local)
                await self.next_event.wait()
                self._record_play(track)
                try:
                    self.bot.loop.create_task(self.ensure_prefetch_ahead(6))
                    self.bot.loop.create_task(self.fill_autoplay_buffer(5))
                except Exception:
                    logger.exception("refill tasks failed after play")
                self.current = None
                continue
            except Exception:
                logger.exception("Download-and-play fallback failed for %s", track.title)
                # ensure we don't block the loop
                self.bot.loop.call_soon_threadsafe(self.next_event.set)
            await self.next_event.wait()
            self._record_play(track)
            try:
                self.bot.loop.create_task(self.ensure_prefetch_ahead(6))
                self.bot.loop.create_task(self.fill_autoplay_buffer(5))
            except Exception:
                logger.exception("refill tasks failed after play")
            self.current = None

    async def enqueue(self, track: Track):
        await self.queue.put(track)
        # ensure a few upcoming tracks are prefetched so playback is smooth
        try:
            await self.ensure_prefetch_ahead(6)
        except Exception:
            logger.exception("ensure_prefetch_ahead failed in enqueue")
        # start background prefetch for this track if possible
        # start a prefetch for this specific track as well
        try:
            await self.start_prefetch_for_track(track)
        except Exception:
            logger.exception("start_prefetch_for_track failed in enqueue")

    async def start_prefetch_for_track(self, track: Track):
        """Start a background prefetch for a single track (idempotent).

        Safe to call multiple times; will not duplicate work for same URL.
        """
        # choose a download query that yt_dlp can use reliably:
        # prefer webpage_url when available, otherwise fall back to a search by title
        download_query = None
        if track.webpage_url:
            download_query = track.webpage_url
        elif track.source_url and ("youtube.com" in track.source_url or "youtu.be" in track.source_url):
            download_query = track.source_url
        elif track.title:
            download_query = f"ytsearch1:{track.title}"

        if not download_query:
            return
        # do not prefetch live streams (they cannot be downloaded reliably)
        if getattr(track, "is_live", False):
            return
        key = download_query
        # avoid duplicate prefetches for same key
        if key in self._prefetch_tasks and not self._prefetch_tasks[key].done():
            return key

        async def _do_prefetch(u: str, t: Track):
            logger.info("Prefetch task started for %s (key=%s)", getattr(t, 'title', '<unknown>'), u)
            try:
                filename = await asyncio.to_thread(download_to_file, u, "tmp_audio")
                if filename and os.path.exists(filename):
                    t.filename = filename
                    t.prefetched = True
                    logger.info("Prefetched %s -> %s", u, filename)
                else:
                    logger.info("Prefetch did not produce a file for %s", u)
            except Exception:
                logger.exception("Prefetch failed for %s", u)
            finally:
                logger.info("Prefetch task finished for %s (key=%s)", getattr(t, 'title', '<unknown>'), u)

        task = self.bot.loop.create_task(_do_prefetch(key, track))
        self._prefetch_tasks[key] = task
        return key

    async def ensure_prefetch_ahead(self, ahead: int = 6):
        """Ensure the next `ahead` tracks in the queue are being prefetched.

        This scans the internal queue and triggers `start_prefetch_for_track`
        for the first `ahead` items.
        """
        try:
            # access underlying deque of the asyncio.Queue
            items = list(self.queue._queue)
            logger.debug("ensure_prefetch_ahead: planning to prefetch %d items (queue size=%d)", min(ahead, len(items)), len(items))
        except Exception:
            return
        count = 0
        for item in items:
            if not isinstance(item, Track):
                continue
            try:
                logger.debug("ensure_prefetch_ahead: starting prefetch for queue item %s", getattr(item, 'title', '<unknown>'))
                await self.start_prefetch_for_track(item)
            except Exception:
                logger.exception("ensure_prefetch_ahead: failed prefetch for one item")
            count += 1
            if count >= ahead:
                break

    async def fill_autoplay_buffer(self, target: int = 5):
        """Fill the autoplay buffer with prefetched tracks up to `target`.

        This will attempt to pick similar tracks (using `pick_autoplay_track`)
        and prefetch them. Only tracks that successfully prefetch a local file
        are appended to the autoplay buffer so playback can use local files.
        """
        # prevent concurrent fill tasks
        if self._autoplay_fill_task and not self._autoplay_fill_task.done():
            return

        async def _fill():
            # seed for the next pick should be the last actual played track
            seed = self.last_played
            tried_keys: set[str] = set()
            attempts = 0
            max_attempts = max(50, target * 10)
            while len(self.autoplay_buffer) < target and attempts < max_attempts:
                try:
                    cand = await self.pick_autoplay_track(seed)
                except Exception:
                    logger.exception("fill_autoplay_buffer: pick_autoplay_track failed")
                    break
                if not cand:
                    break
                url = cand.webpage_url or cand.source_url or cand.title
                if not url:
                    break
                key = url
                # avoid retrying the same failing candidate repeatedly
                if key in tried_keys:
                    attempts += 1
                    await asyncio.sleep(0.05)
                    continue
                try:
                    key = await self.start_prefetch_for_track(cand)
                    task = self._prefetch_tasks.get(key) if key else None
                    if task:
                        try:
                            await asyncio.wait_for(task, timeout=60)
                        except Exception:
                            logger.info("Autoplay prefetch timed out or failed for %s", key)
                    # only append if file exists
                    if cand.filename and os.path.exists(cand.filename):
                        cand.prefetched = True
                        self.autoplay_buffer.append(cand)
                        # advance seed so next suggestion is related
                        seed = cand
                    else:
                        # record failed candidate and try another
                        tried_keys.add(key)
                        seed = seed or cand
                        attempts += 1
                        await asyncio.sleep(0.1)
                        continue
                except Exception:
                    logger.exception("Autoplay prefetch error for %s", url)
                    # mark this candidate as tried and continue
                    tried_keys.add(key)
                    attempts += 1
                    await asyncio.sleep(0.1)
                    continue
            if attempts >= max_attempts:
                logger.info("Autoplay fill aborted after %d attempts (target=%d)", attempts, target)
        self._autoplay_fill_task = self.bot.loop.create_task(_fill())
        try:
            await self._autoplay_fill_task
        except Exception:
            # swallow exceptions, they are already logged
            pass

    async def ensure_autoplay_ready(self, min_prefetched: int = 1, timeout: float = 10.0) -> bool:
        """Ensure at least `min_prefetched` prefetched autoplay tracks are available within `timeout` seconds.

        Returns True if ready, False on timeout.
        """
        if len(self.autoplay_buffer) >= min_prefetched:
            return True
        # trigger a fill if not already running
        try:
            if not self._autoplay_fill_task or self._autoplay_fill_task.done():
                self._autoplay_fill_task = self.bot.loop.create_task(self.fill_autoplay_buffer(max(min_prefetched, 5)))
        except Exception:
            logger.exception("ensure_autoplay_ready: failed to start fill task")

        start = time.time()
        while time.time() - start < timeout:
            if len(self.autoplay_buffer) >= min_prefetched:
                return True
            await asyncio.sleep(0.2)
        return False

    async def pick_autoplay_track(self, last_track: Track | None) -> Track | None:
        """Try to pick a similar track based on the last played track's title.

        Returns a new Track or None if no candidate found.
        """
        try:
            def _norm(s: str | None) -> str:
                if not s:
                    return ""
                s = s.strip().lower()
                # attempt to extract youtube id if present for more robust comparison
                m = re.search(r"(?:v=|youtu\.be/|/watch\?v=)([A-Za-z0-9_-]{6,})", s)
                if m:
                    return m.group(1)
                return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", s))

            # Try multiple attempts to pick a candidate different from recent history
            if last_track and last_track.title:
                attempts = 12
                # try a few variants to be resilient to yt_dlp extraction failures
                queries = [last_track.title]
                # strip common prefixes like 'Requested:' which can confuse searches
                cleaned = re.sub(r"^requested:\s*", "", last_track.title, flags=re.I).strip()
                if cleaned and cleaned != last_track.title:
                    queries.append(cleaned)
                # also try shorter tokenized version
                short = " ".join(cleaned.split()[:6]) if cleaned else None
                if short and short != cleaned:
                    queries.append(short)

                for _ in range(attempts):
                    q = random.choice(queries)
                    now = time.time()
                    # skip recently-failed queries to avoid repeat spam
                    f_at = self._failed_queries.get(q)
                    if f_at and (now - f_at) < FAILED_QUERY_TTL:
                        logger.debug("Autoplay: skipping recently-failed query '%s'", q)
                        await asyncio.sleep(0.05)
                        continue
                    try:
                        candidates = await yt_dlp_get_candidates(q, max_results=30, exclude_webpage=(last_track.webpage_url or None))
                    except Exception as e:
                        # record failed query and skip for a while
                        try:
                            self._failed_queries[q] = now
                            self.bot.loop.create_task(asyncio.to_thread(self._save_failed_queries_sync))
                        except Exception:
                            pass
                        logger.info("Autoplay: yt_dlp search failed for query '%s': %s", q, e)
                        await asyncio.sleep(0.1)
                        continue
                    if not candidates:
                        try:
                            self._failed_queries[q] = now
                            self.bot.loop.create_task(asyncio.to_thread(self._save_failed_queries_sync))
                        except Exception:
                            pass
                        await asyncio.sleep(0.05)
                        continue

                    random.shuffle(candidates)
                    picked = None
                    for stream_url, title, webpage_url, is_live in candidates:
                        if not stream_url and not webpage_url:
                            continue

                        cand_key = (webpage_url or stream_url or title) or ""
                        norm_cand = _norm(cand_key)
                        # compare against recent history and last_played
                        dup = False
                        for h in list(self.play_history):
                            if _norm(h) and _norm(h) == norm_cand:
                                dup = True
                                break
                        if not dup:
                            for h in self._persisted_history.get(str(self.guild.id), []):
                                if _norm(h) and _norm(h) == norm_cand:
                                    dup = True
                                    break
                        if last_track:
                            if _norm(last_track.webpage_url or last_track.source_url or last_track.title) == norm_cand:
                                dup = True
                        if dup:
                            continue
                        picked = (stream_url, title, webpage_url, is_live)
                        break

                    if picked:
                        stream_url, title, webpage_url, is_live = picked
                        chosen_source = stream_url if (stream_url and stream_url != webpage_url) else None
                        return Track(title=title or last_track.title, source_url=chosen_source, webpage_url=webpage_url, is_live=is_live)
                    # no acceptable candidates for this query -> mark as failed
                    try:
                        self._failed_queries[q] = now
                        self.bot.loop.create_task(asyncio.to_thread(self._save_failed_queries_sync))
                    except Exception:
                        pass
                    await asyncio.sleep(0.05)

            # Fallbacks: try a variety of genre-based and "mix/playlist" queries
            genres = ["rock", "chill", "pop", "electronic", "hip hop", "lofi", "classical", "indie", "dance", "r&b", "soul", "metal", "reggae"]

            def _word_overlap(a: str | None, b: str | None) -> float:
                if not a or not b:
                    return 0.0
                wa = set(_norm(a).split())
                wb = set(_norm(b).split())
                if not wa or not wb:
                    return 0.0
                inter = wa.intersection(wb)
                return len(inter) / max(len(wa), len(wb))

            # Build a wider list of queries with variations to increase chance of diverse picks
            if self.autoplay_genre:
                g = self.autoplay_genre
                try_queries = [g, f"{g} mix", f"{g} playlist", f"best {g} songs", f"{g} hits"]
            else:
                try_queries = ["popular music"]
                for g in genres:
                    try_queries.extend([f"{g} mix", f"{g} playlist", f"best {g} songs", f"{g} hits"])
                random.shuffle(try_queries)

            # threshold for considering two titles "too similar" (lower to be more aggressive)
            similarity_threshold = 0.45

            # get normalized history titles to compare against (include persisted guild history)
            history_titles = list(self.play_history)
            history_titles.extend(self._persisted_history.get(str(self.guild.id), []))

            for q in try_queries:
                now = time.time()
                f_at = self._failed_queries.get(q)
                if f_at and (now - f_at) < FAILED_QUERY_TTL:
                    logger.debug("Autoplay: skipping recently-failed fallback query '%s'", q)
                    continue
                try:
                    candidates = await yt_dlp_get_candidates(q, max_results=30, exclude_webpage=(last_track.webpage_url if last_track else None))
                except Exception:
                    try:
                        self._failed_queries[q] = now
                        self.bot.loop.create_task(asyncio.to_thread(self._save_failed_queries_sync))
                    except Exception:
                        pass
                    continue
                if not candidates:
                    try:
                        self._failed_queries[q] = now
                        self.bot.loop.create_task(asyncio.to_thread(self._save_failed_queries_sync))
                    except Exception:
                        pass
                    continue

                random.shuffle(candidates)
                picked = None
                for stream_url, title, webpage_url, is_live in candidates:
                    if not stream_url and not webpage_url:
                        continue
                    cand_title = title or webpage_url or stream_url or q
                    cand_norm = _norm(cand_title)
                    if not cand_norm:
                        continue

                    # Reject if exact normalized match in history
                    if any(_norm(h) == cand_norm for h in history_titles):
                        continue

                    # Reject if candidate is too similar to any recent title (covers remixes/covers)
                    too_similar = False
                    for h in history_titles:
                        if _word_overlap(cand_title, h) > similarity_threshold:
                            too_similar = True
                            break
                    if too_similar:
                        continue

                    # Also avoid very high overlap with the immediate last track
                    if last_track and last_track.title:
                        if _word_overlap(cand_title, last_track.title) > similarity_threshold:
                            continue

                    picked = (stream_url, title, webpage_url, is_live)
                    break

                if picked:
                    stream_url, title, webpage_url, is_live = picked
                    logger.info("Autoplay fallback: selected '%s' from query '%s'", title, q)
                    chosen_source = stream_url if (stream_url and stream_url != webpage_url) else None
                    return Track(title=title or "Autoplay", source_url=chosen_source, webpage_url=webpage_url, is_live=is_live)
                try:
                    self._failed_queries[q] = now
                    self.bot.loop.create_task(asyncio.to_thread(self._save_failed_queries_sync))
                except Exception:
                    pass

            logger.info("Autoplay: no suitable fallback candidate found after genre queries")
            return None
        except Exception:
            logger.exception("pick_autoplay_track error")
            return None

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
