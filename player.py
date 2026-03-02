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


class _YTDLLogger:
    def debug(self, msg):
        return

    def info(self, msg):
        return

    def warning(self, msg):
        return

    def error(self, msg):
        return

FFMPEG_BEFORE_OPTS = "-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5"
FFMPEG_OPTIONS = "-vn"

# download timeout and retry configuration (seconds)
DOWNLOAD_TIMEOUT = int(os.environ.get("DMBOT_DOWNLOAD_TIMEOUT", "120"))
DOWNLOAD_RETRIES = int(os.environ.get("DMBOT_DOWNLOAD_RETRIES", "2"))

# persistent autoplay history
HISTORY_FILE = os.environ.get("DMBOT_AUTOPLAY_HISTORY_FILE", "autoplay_history.json")
HISTORY_MAXLEN = int(os.environ.get("DMBOT_AUTOPLAY_HISTORY_MAXLEN", "512"))
FAILED_QUERY_TTL = int(os.environ.get("DMBOT_AUTOPLAY_FAILED_TTL", "300"))  # seconds to avoid re-querying recently-failed search strings
FAILED_QUERIES_FILE = os.environ.get("DMBOT_AUTOPLAY_FAILED_FILE", "autoplay_failed.json")

logger = logging.getLogger(__name__)


async def yt_dlp_get_url(query: str, max_results: int = 1, exclude_webpage: str | None = None) -> tuple[str, str, str, bool, float | None]:
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
        # Allow providing cookies to handle age-restricted / signed-in-only videos.
        # Set DMBOT_YT_COOKIES to a cookies.txt file exported from your browser,
        # or DMBOT_YT_COOKIES_FROM_BROWSER to a browser name (e.g. 'chrome') to use
        # yt-dlp's cookies-from-browser extractor.
        ydl_opts = {"format": "bestaudio/best", "quiet": True, "no_warnings": True}
        # prefer explicit cookiefile path if provided
        cookiefile = os.environ.get("DMBOT_YT_COOKIES")
        cookies_from_browser = os.environ.get("DMBOT_YT_COOKIES_FROM_BROWSER")
        if cookiefile:
            ydl_opts["cookiefile"] = cookiefile
        elif cookies_from_browser:
            # yt-dlp option key is 'cookiesfrombrowser'
            ydl_opts["cookiesfrombrowser"] = cookies_from_browser
        if has_deno:
            ydl_opts["jsruntimes"] = "deno"
        import sys, io
        # Some environments may provide a YoutubeDL implementation that does not accept
        # the `logger` keyword (e.g., older youtube_dl forks). Try to pass the logger
        # but fall back to constructing without it on TypeError.
        try:
            ydl_ctx = yt_dlp.YoutubeDL(ydl_opts, logger=_YTDLLogger())
        except TypeError:
            ydl_ctx = yt_dlp.YoutubeDL(ydl_opts)
        with ydl_ctx as ydl:
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

    # duration in seconds if yt_dlp provided it
    duration = None
    try:
        duration = float(info.get("duration")) if isinstance(info, dict) and info.get("duration") else None
    except Exception:
        duration = None

    return stream_url, title, webpage_url, is_live, duration


async def yt_dlp_get_candidates(query: str, max_results: int = 10, exclude_webpage: str | None = None) -> List[Tuple[Optional[str], Optional[str], Optional[str], bool, float | None]]:
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
        # See notes in yt_dlp_get_url about cookies env vars
        ydl_opts = {"format": "bestaudio/best", "quiet": True, "no_warnings": True}
        cookiefile = os.environ.get("DMBOT_YT_COOKIES")
        cookies_from_browser = os.environ.get("DMBOT_YT_COOKIES_FROM_BROWSER")
        if cookiefile:
            ydl_opts["cookiefile"] = cookiefile
        elif cookies_from_browser:
            ydl_opts["cookiesfrombrowser"] = cookies_from_browser
        if has_deno:
            ydl_opts["jsruntimes"] = "deno"
        import sys, io
        try:
            ydl_ctx = yt_dlp.YoutubeDL(ydl_opts, logger=_YTDLLogger())
        except TypeError:
            ydl_ctx = yt_dlp.YoutubeDL(ydl_opts)
        with ydl_ctx as ydl:
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
        duration = None
        try:
            duration = float(e.get("duration")) if isinstance(e, dict) and e.get("duration") else None
        except Exception:
            duration = None
        candidates.append((stream_url, title, webpage_url, is_live, duration))

    return candidates


@dataclass
class Track:
    title: str
    source_url: str
    webpage_url: str | None = None
    filename: str | None = None
    prefetched: bool = False
    is_live: bool = False
    duration: float | None = None


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
        # newer UI sets `autoplay_mode` (e.g. 'hiphop' or 'custom:<query>')
        # keep for backward compatibility with older code that used `autoplay_genre`.
        self.autoplay_mode: str | None = None
        # whether the current autoplay selection originated from the 247 Autoplay UI
        self.autoplay_from_247: bool = False
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

    async def _autoplay_advance(self):
        """Try to advance autoplay immediately after a stream ends.

        This will attempt to fill the autoplay buffer and enqueue the next
        available candidate (preferring buffered items), or fall back to a
        direct pick.
        """
        if not getattr(self, 'autoplay', False):
            return
        # if there are queued items or currently playing, do nothing
        try:
            if not self.queue.empty() or self.is_playing():
                return
        except Exception:
            pass

        try:
            # try to fill buffer quickly
            try:
                await self.fill_autoplay_buffer(5)
            except Exception:
                logger.exception("Autoplay advance: fill_autoplay_buffer failed")

            if self.autoplay_buffer:
                nt = self.autoplay_buffer.popleft()
                try:
                    await self.enqueue(nt)
                    return
                except Exception:
                    logger.exception("Autoplay advance: failed to enqueue buffered track")

            # final fallback: pick directly
            try:
                nt = await self.pick_autoplay_track(self.last_played)
                if nt:
                    await self.enqueue(nt)
            except Exception:
                logger.exception("Autoplay advance: pick_autoplay_track failed")
        except Exception:
            logger.exception("Autoplay advance unexpected error")

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
        logger.info("player_loop started for guild %s", self.guild.id)
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

                    # start a progress monitor task to log elapsed time and percent
                    async def _play_progress_monitor(t: Track, start_ts: float):
                        try:
                            interval = 5 if (t.duration and t.duration > 0) else 30
                            while not self.next_event.is_set():
                                elapsed = time.time() - start_ts
                                if t.duration and t.duration > 0:
                                    pct = min(100.0, (elapsed / t.duration) * 100.0)
                                    logger.debug("[guild %s] Playing '%s' elapsed=%.1fs / %.1fs (%.1f%%)", self.guild.id, t.title, elapsed, t.duration, pct)
                                else:
                                    logger.debug("[guild %s] Playing '%s' elapsed=%.1fs", self.guild.id, t.title, elapsed)
                                await asyncio.sleep(interval)
                        except asyncio.CancelledError:
                            return

                    play_start = time.time()
                    monitor_task = self.bot.loop.create_task(_play_progress_monitor(track, play_start))

                    def _after_local_prefetched(err):
                        if err:
                            logger.error("Local playback error: %s", err)
                        # do not remove prefetched file
                        self.bot.loop.call_soon_threadsafe(self.next_event.set)
                        try:
                            # attempt to advance autoplay immediately
                            self.bot.loop.create_task(self._autoplay_advance())
                        except Exception:
                            pass

                    self.voice_client.play(source, after=_after_local_prefetched)
                    await self.next_event.wait()
                    # ensure monitor is stopped
                    try:
                        if monitor_task and not monitor_task.done():
                            monitor_task.cancel()
                    except Exception:
                        pass
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
                        try:
                            self.bot.loop.create_task(self._autoplay_advance())
                        except Exception:
                            pass

                    # start progress monitor for streamed playback
                    async def _play_progress_monitor_stream(t: Track, start_ts: float):
                        try:
                            interval = 5 if (t.duration and t.duration > 0) else 30
                            while not self.next_event.is_set():
                                elapsed = time.time() - start_ts
                                if t.duration and t.duration > 0:
                                    pct = min(100.0, (elapsed / t.duration) * 100.0)
                                    logger.debug("[guild %s] Streaming '%s' elapsed=%.1fs / %.1fs (%.1f%%)", self.guild.id, t.title, elapsed, t.duration, pct)
                                else:
                                    logger.debug("[guild %s] Streaming '%s' elapsed=%.1fs", self.guild.id, t.title, elapsed)
                                await asyncio.sleep(interval)
                        except asyncio.CancelledError:
                            return

                    play_start = time.time()
                    monitor_task = self.bot.loop.create_task(_play_progress_monitor_stream(track, play_start))

                    self.voice_client.play(source, after=_after)
                    await self.next_event.wait()
                    try:
                        if monitor_task and not monitor_task.done():
                            monitor_task.cancel()
                    except Exception:
                        pass
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
                    try:
                        ydl_ctx = yt_dlp.YoutubeDL(ydl_opts, logger=_YTDLLogger())
                    except TypeError:
                        ydl_ctx = yt_dlp.YoutubeDL(ydl_opts)
                    with ydl_ctx as ydl:
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
                        # attach duration to track if available
                        try:
                            dur = entry.get('duration') if isinstance(entry, dict) else None
                            if dur:
                                track.duration = float(dur)
                        except Exception:
                            pass
                        logger.info("Prepared filename: %s", filename)
                        if not filename:
                            raise RuntimeError("Could not determine downloaded filename")
                        return filename

                # If a prefetch for this URL is in progress, wait for it instead
                url_to_download = track.webpage_url or track.source_url

                async def _attempt_download_with_retries():
                    # if there was a prefetch task, wait briefly for it
                    if url_to_download and url_to_download in self._prefetch_tasks:
                        pre_t = self._prefetch_tasks[url_to_download]
                        try:
                            await asyncio.wait_for(pre_t, timeout=30)
                        except Exception:
                            logger.info("Prefetch task did not produce a file or timed out; proceeding to download")
                        if track.filename and os.path.exists(track.filename):
                            logger.info("Using prefetched file from task: %s", track.filename)
                            return track.filename

                    last_exc = None
                    # perform initial attempt + configured retries
                    attempts = max(1, DOWNLOAD_RETRIES) + 1
                    for attempt in range(1, attempts + 1):
                        # progressive timeout: base * 2^(attempt-1) for subsequent attempts
                        timeout_secs = DOWNLOAD_TIMEOUT * (2 ** (attempt - 1)) if attempt > 1 else DOWNLOAD_TIMEOUT
                        try:
                            filename = await asyncio.wait_for(asyncio.to_thread(download), timeout=timeout_secs)
                            logger.info("Downloaded fallback file: %s (attempt %d, timeout=%ds)", filename, attempt, timeout_secs)
                            return filename
                        except asyncio.TimeoutError as e:
                            last_exc = e
                            logger.warning("yt-dlp download attempt %d timed out after %d seconds for %s", attempt, timeout_secs, track.source_url)
                        except Exception as e:
                            last_exc = e
                            logger.exception("yt-dlp download attempt %d failed for %s", attempt, track.source_url)
                        # small backoff before retrying
                        await asyncio.sleep(min(5 * attempt, 30))

                    # all attempts failed
                    if last_exc:
                        raise last_exc
                    raise RuntimeError("yt-dlp download failed without exception")

                dl_start = time.time()
                filename = await _attempt_download_with_retries()
                dl_elapsed = time.time() - dl_start
                logger.info("Download completed for '%s' in %.1fs -> %s", track.title, dl_elapsed, filename)

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
                    try:
                        self.bot.loop.create_task(self._autoplay_advance())
                    except Exception:
                        pass

                # start monitor for local playback
                async def _play_progress_monitor_file(t: Track, start_ts: float):
                    try:
                        interval = 5 if (t.duration and t.duration > 0) else 30
                        while not self.next_event.is_set():
                            elapsed = time.time() - start_ts
                            if t.duration and t.duration > 0:
                                pct = min(100.0, (elapsed / t.duration) * 100.0)
                                logger.debug("[guild %s] Playing file '%s' elapsed=%.1fs / %.1fs (%.1f%%)", self.guild.id, t.title, elapsed, t.duration, pct)
                            else:
                                logger.debug("[guild %s] Playing file '%s' elapsed=%.1fs", self.guild.id, t.title, elapsed)
                            await asyncio.sleep(interval)
                    except asyncio.CancelledError:
                        return

                play_start = time.time()
                monitor_task = self.bot.loop.create_task(_play_progress_monitor_file(track, play_start))

                self.voice_client.play(source, after=_after_local)
                await self.next_event.wait()
                try:
                    if monitor_task and not monitor_task.done():
                        monitor_task.cancel()
                except Exception:
                    pass
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
        logger.info("Enqueued track for guild %s: %s (prefetched=%s, live=%s)", self.guild.id, track.title, track.prefetched, track.is_live)
        # prefetch disabled: enqueue will not trigger background downloads

    async def start_prefetch_for_track(self, track: Track):
        """Start a background prefetch for a single track (idempotent).

        Safe to call multiple times; will not duplicate work for same URL.
        """
        # Prefetching disabled globally for autoplay/24/7: no-op
        return None

    async def ensure_prefetch_ahead(self, ahead: int = 6):
        """Ensure the next `ahead` tracks in the queue are being prefetched.

        This scans the internal queue and triggers `start_prefetch_for_track`
        for the first `ahead` items.
        """
        # Prefetching disabled globally: no-op
        return

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
            seed = self.last_played
            attempts = 0
            max_attempts = max(50, target * 10)
            while len(self.autoplay_buffer) < target and attempts < max_attempts:
                try:
                    cand = await self.pick_autoplay_track(seed)
                except Exception:
                    logger.exception("fill_autoplay_buffer: pick_autoplay_track failed")
                    break
                if not cand:
                    attempts += 1
                    await asyncio.sleep(0.1)
                    continue
                # accept candidate if it appears usable (live or has a URL)
                if not (getattr(cand, 'source_url', None) or getattr(cand, 'webpage_url', None) or getattr(cand, 'is_live', False)):
                    attempts += 1
                    await asyncio.sleep(0.1)
                    continue
                cand.prefetched = False
                self.autoplay_buffer.append(cand)
                seed = cand
            if attempts >= max_attempts:
                logger.debug("Autoplay fill aborted after %d attempts (target=%d)", attempts, target)

        self._autoplay_fill_task = self.bot.loop.create_task(_fill())
        try:
            await self._autoplay_fill_task
        except Exception:
            pass

    async def ensure_autoplay_ready(self, min_prefetched: int = 1, timeout: float = 10.0) -> bool:
        """Ensure at least `min_prefetched` prefetched autoplay tracks are available within `timeout` seconds.

        Returns True if ready, False on timeout.
        """
        if len(self.autoplay_buffer) >= min_prefetched:
            return True
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

    async def pick_autoplay_track(self, last_track: Track | None, max_results: int = 30) -> Track | None:
        """Try to pick a similar track based on the last played track's title.

        Returns a new Track or None if no candidate found.
        """
        try:
            # If the UI set a custom autoplay query, prefer it over using the last
            # played track's title as the seed. This avoids using the possibly
            # non-music title of the initial custom-resolved track as the basis
            # for subsequent suggestions.
            if isinstance(self.autoplay_mode, str) and self.autoplay_mode.startswith("custom:"):
                logger.info("Autoplay: using custom mode query instead of last_track seed")
                last_track = None

            def _norm(s: str | None) -> str:
                if not s:
                    return ""
                s = s.strip().lower()
                # attempt to extract youtube id if present for more robust comparison
                m = re.search(r"(?:v=|youtu\.be/|/watch\?v=)([A-Za-z0-9_-]{6,})", s)
                if m:
                    return m.group(1)
                return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", s))

            def _title_has_live_token(title: str | None) -> bool:
                if not title:
                    return False
                t = title.lower()
                if re.search(r"\blive\b", t):
                    return True
                if "24/7" in t or "247" in t:
                    return True
                # explicit phrases
                if "live radio" in t or "live:" in t or "live:" in t:
                    return True
                return False

            # If a specific autoplay genre is set (non-custom), prefer live 24/7 streams.
            # Reduce supported live categories to the curated set requested by user
            LIVE_MODES = {
                "rockclassics",
                "rock",
                "praise",
                "worship",
                "bestradio",
                "jazz",
                "worldnews",
                "podcasts",
                # user-requested additions
                "christmas",
                "chill",
                "relaxing",
                "dnb",
                "rainy",
                "deephouse",
                # top curated popular 24/7 searches
                "top247",
            }

            live_category = None
            # If the selection came from the 247 Autoplay menu, always treat it as a live category
            if getattr(self, 'autoplay_from_247', False) and self.autoplay_genre:
                live_category = str(self.autoplay_genre).strip()
            else:
                if self.autoplay_mode and isinstance(self.autoplay_mode, str) and not self.autoplay_mode.startswith("custom:"):
                    cand = str(self.autoplay_mode).strip()
                    if cand in LIVE_MODES:
                        live_category = cand
                elif self.autoplay_genre:
                    cand = str(self.autoplay_genre).strip()
                    if cand in LIVE_MODES:
                        live_category = cand

            if live_category:
                # Build specific queries that target 24/7/live stations for the category
                qbase = live_category
                # Special curated top-24/7 queries when requested
                if qbase == "top247":
                    queries = [
                        "24/7 lofi hip hop radio",
                        "lofi hip hop 24/7",
                        "24/7 lo-fi beats to relax/study to",
                        "lofi girl 24/7",
                        "24/7 chillhop radio",
                        "24/7 deep house live",
                        "deep house 24/7 live",
                        "24/7 jazz radio",
                        "24/7 classical music live",
                        "rain sounds 24/7",
                        "news 24/7 live",
                        "24/7 podcasts live",
                        "24/7 radio live",
                    ]
                else:
                    queries = [
                        f"247 {qbase} live",
                        f"247 {qbase} 24/7 live",
                        f"{qbase} 24/7 live",
                        f"{qbase} hits live",
                        f"spotify {qbase} live",
                        f"spotify hits {qbase} live",
                        f"{qbase} radio live",
                    ]
                for q in queries:
                    now = time.time()
                    f_at = self._failed_queries.get(q)
                    if f_at and (now - f_at) < FAILED_QUERY_TTL:
                        continue
                    try:
                            candidates = await yt_dlp_get_candidates(q, max_results=max_results)
                            # Diagnostic: log candidate details to help debug non-live selections
                            try:
                                for idx, (stream_url, title, webpage_url, is_live, duration) in enumerate(candidates):
                                    logger.debug("Autoplay live-query candidate[%d] q=%s title=%s is_live=%s duration=%s stream_url=%s webpage_url=%s", idx, q, title, is_live, duration, bool(stream_url), webpage_url)
                            except Exception:
                                pass
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

                    # prefer live candidates explicitly; skip ones we recently played
                    for stream_url, title, webpage_url, is_live, duration in candidates:
                        # Require a direct stream URL for live autoplay candidates.
                        # Webpage-only entries (no direct stream_url) often point to VODs
                        # or pages that will be downloaded as non-live recordings —
                        # skip those to ensure we only enqueue actual live streams.
                        if not stream_url:
                            logger.debug("Autoplay live candidate skipped (no direct stream URL): %s", title)
                            continue
                        # Require that the title explicitly indicates a live/24/7 stream
                        t = (title or "").lower()
                        if not (re.search(r"\blive\b", t) or "247" in t or "24/7" in t):
                            logger.debug("Autoplay live candidate skipped (title missing live/247): %s", title)
                            continue
                        if not is_live:
                            continue
                        # Heuristic: if yt-dlp reports a duration, it's likely not a true live stream
                        if duration and duration > 0:
                            logger.debug("Autoplay live candidate skipped (has duration=%.1f): %s", duration, title)
                            continue
                        # Avoid immediate repeats: skip candidates present in recent history
                        cand_key = (webpage_url or stream_url or title) or ""
                        norm_cand = _norm(cand_key)
                        if norm_cand:
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
                            if not dup and self.last_played:
                                if _norm(self.last_played.webpage_url or self.last_played.source_url or self.last_played.title) == norm_cand:
                                    dup = True
                            if dup:
                                continue
                        # enforce title contains a required live token before accepting
                        if not _title_has_live_token(title):
                            logger.debug("Autoplay live candidate skipped (title missing required token): %s", title)
                            continue
                        # allow using the direct stream URL even if it equals the webpage URL
                        chosen_source = stream_url if stream_url else None
                        return Track(title=title or qbase, source_url=chosen_source, webpage_url=webpage_url, is_live=True, duration=duration)
                # no live candidate found for configured queries
                return None

            # Determine whether the current autoplay selection requires live/24/7 titles
            require_live_title = False
            try:
                if getattr(self, 'autoplay_from_247', False):
                    require_live_title = True
                if isinstance(self.autoplay_mode, str) and self.autoplay_mode in LIVE_MODES:
                    require_live_title = True
                if getattr(self, 'autoplay_genre', None) and self.autoplay_genre in LIVE_MODES:
                    require_live_title = True
            except Exception:
                pass

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
                        candidates = await yt_dlp_get_candidates(q, max_results=max_results, exclude_webpage=(last_track.webpage_url or None))
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
                    # prefer candidates that have a direct stream URL, are not live,
                    # and have a reasonable duration to avoid 10+ hour mixes
                    MAX_DURATION = 7200.0  # 2 hours

                    def _is_dup(norm_cand: str) -> bool:
                        if not norm_cand:
                            return True
                        for h in list(self.play_history):
                            if _norm(h) and _norm(h) == norm_cand:
                                return True
                        for h in self._persisted_history.get(str(self.guild.id), []):
                            if _norm(h) and _norm(h) == norm_cand:
                                return True
                        if last_track:
                            if _norm(last_track.webpage_url or last_track.source_url or last_track.title) == norm_cand:
                                return True
                        return False

                    # first pass: find best-playable candidate
                    for stream_url, title, webpage_url, is_live, duration in candidates:
                        if not stream_url:
                            continue
                        cand_key = (webpage_url or stream_url or title) or ""
                        norm_cand = _norm(cand_key)
                        if _is_dup(norm_cand):
                            continue
                        if is_live:
                            logger.debug("Skipping live candidate for autoplay: %s", title)
                            continue
                        if duration and duration > 0 and duration <= MAX_DURATION:
                            picked = (stream_url, title, webpage_url, is_live, duration)
                            break

                    # second pass: relax duration requirement but still prefer direct streams
                    if not picked:
                        for stream_url, title, webpage_url, is_live, duration in candidates:
                            if not stream_url:
                                continue
                            cand_key = (webpage_url or stream_url or title) or ""
                            norm_cand = _norm(cand_key)
                            if _is_dup(norm_cand):
                                continue
                            if is_live:
                                continue
                            picked = (stream_url, title, webpage_url, is_live, duration)
                            break

                    # final fallback: accept candidates without direct stream (may require download)
                    if not picked:
                        for stream_url, title, webpage_url, is_live, duration in candidates:
                            if not stream_url and not webpage_url:
                                continue
                            cand_key = (webpage_url or stream_url or title) or ""
                            norm_cand = _norm(cand_key)
                            if _is_dup(norm_cand):
                                continue
                            picked = (stream_url, title, webpage_url, is_live, duration)
                            break

                    if picked:
                        stream_url, title, webpage_url, is_live, duration = picked
                        # enforce live-title requirement if configured
                        if require_live_title and not _title_has_live_token(title):
                            logger.debug("Autoplay candidate skipped due to missing live token (require_live_title=true): %s", title)
                            try:
                                self._failed_queries[q] = now
                                self.bot.loop.create_task(asyncio.to_thread(self._save_failed_queries_sync))
                            except Exception:
                                pass
                            await asyncio.sleep(0.05)
                            continue
                        chosen_source = stream_url if (stream_url and stream_url != webpage_url) else None
                        return Track(title=title or last_track.title, source_url=chosen_source, webpage_url=webpage_url, is_live=is_live, duration=duration)
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
            # prefer UI-provided `autoplay_mode` when available; it may contain
            # either a genre like 'hiphop' or a custom query prefixed with 'custom:'.
            if self.autoplay_mode:
                if isinstance(self.autoplay_mode, str) and self.autoplay_mode.startswith("custom:"):
                    q = self.autoplay_mode.split(":", 1)[1]
                    try_queries = [q]
                else:
                    g = self.autoplay_mode
                    try_queries = [g, f"{g} mix", f"{g} playlist", f"best {g} songs", f"{g} hits"]
            elif self.autoplay_genre:
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
                    candidates = await yt_dlp_get_candidates(q, max_results=max_results, exclude_webpage=(last_track.webpage_url if last_track else None))
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
                # prefer playable candidates with direct streams and reasonable duration
                MAX_DURATION = 7200.0

                def _is_dup_fallback(norm_cand: str) -> bool:
                    if not norm_cand:
                        return True
                    if any(_norm(h) == norm_cand for h in history_titles):
                        return True
                    return False

                for stream_url, title, webpage_url, is_live, duration in candidates:
                    if not stream_url and not webpage_url:
                        continue
                    cand_title = title or webpage_url or stream_url or q
                    cand_norm = _norm(cand_title)
                    if not cand_norm:
                        continue
                    if _is_dup_fallback(cand_norm):
                        continue
                    if is_live:
                        continue
                    if duration and duration > 0 and duration <= MAX_DURATION:
                        picked = (stream_url, title, webpage_url, is_live, duration)
                        break

                if not picked:
                    for stream_url, title, webpage_url, is_live, duration in candidates:
                        if not stream_url and not webpage_url:
                            continue
                        cand_title = title or webpage_url or stream_url or q
                        cand_norm = _norm(cand_title)
                        if not cand_norm:
                            continue
                        if _is_dup_fallback(cand_norm):
                            continue
                        if is_live:
                            continue
                        picked = (stream_url, title, webpage_url, is_live, duration)
                        break

                if picked:
                    stream_url, title, webpage_url, is_live, duration = picked
                    # enforce live-title requirement for fallback selections as well
                    if require_live_title and not _title_has_live_token(title):
                        logger.debug("Autoplay fallback candidate skipped (missing live token): %s", title)
                        try:
                            self._failed_queries[q] = now
                            self.bot.loop.create_task(asyncio.to_thread(self._save_failed_queries_sync))
                        except Exception:
                            pass
                        continue
                    logger.info("Autoplay fallback: selected '%s' from query '%s'", title, q)
                    chosen_source = stream_url if (stream_url and stream_url != webpage_url) else None
                    return Track(title=title or "Autoplay", source_url=chosen_source, webpage_url=webpage_url, is_live=is_live, duration=duration)
                try:
                    self._failed_queries[q] = now
                    self.bot.loop.create_task(asyncio.to_thread(self._save_failed_queries_sync))
                except Exception:
                    pass

            logger.debug("Autoplay: no suitable fallback candidate found after genre queries")
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
