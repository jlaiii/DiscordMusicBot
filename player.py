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

# (Removed) — no persistent history or failed-query cache

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
        # history/prefetch removed — keep minimal state used elsewhere
        self.play_history: deque[str] = deque()
        self.last_played: Track | None = None
        # background support removed
        self.last_voice_channel_id: int | None = None
        self._prefetch_tasks: dict[str, asyncio.Task] = {}
        self.voice_client: discord.VoiceClient | None = None
        self.task = None
        if start_task:
            self.task = bot.loop.create_task(self.player_loop())
        # loop flag: when True, replay the currently finished track
        self.loop: bool = False
        # locate ffmpeg
        self.ffmpeg = shutil.which("ffmpeg")
        if not self.ffmpeg:
            logging.getLogger(__name__).warning("ffmpeg not found on PATH; playback will fail until ffmpeg is installed")
        # Test mode: when set via env var, skip creating FFmpegPCMAudio and just call voice_client.play(None,...)
        self._test_mode = os.environ.get("DMBOT_TEST_FAKE_PLAY") == "1"
        # per-guild download lock to avoid concurrent downloads causing heavy contention
        self._download_lock: asyncio.Lock = asyncio.Lock()
        os.makedirs("tmp_audio", exist_ok=True)

    def _record_play(self, track: Track):
        """Record the last played track and remember a key to avoid immediate repeats."""
        self.last_played = track
        try:
            key = track.webpage_url or track.source_url or track.title
            if key:
                self.play_history.append(key)
        except Exception:
            pass

    def _handle_loop(self, track: Track) -> bool:
        """If loop is enabled, re-insert `track` to the front of the queue for immediate replay.

        Returns True when the track was requeued for looping.
        """
        if not getattr(self, "loop", False):
            return False
        try:
            # use the underlying deque to append to the left so the track is next
            self.queue._queue.appendleft(track)
            logger.info("Loop enabled; replaying track: %s", track.title)
            return True
        except Exception:
            # best-effort fallback: enqueue normally
            try:
                self.bot.loop.create_task(self.queue.put(track))
            except Exception:
                pass
            return True

    def _save_history_sync(self):
        """Write the persisted history atomically to disk (synchronous)."""
        try:
            tmp = f"{self._history_file}.tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._persisted_history, f)
            os.replace(tmp, self._history_file)
        except Exception:
            # history persistence removed
            pass

    def _save_failed_queries_sync(self):
        """Write the failed-queries cache atomically to disk (synchronous)."""
        # disabled: failed-query cache removed
        return

    def _set_next_event(self, source: str | None = None, err: Exception | None = None):
        """Set the next_event safely from any thread, logging context for debugging."""
        try:
            logger.debug("Setting next_event (source=%s err=%s) for guild=%s", source, getattr(err, 'args', None), getattr(self.guild, 'id', None))
        except Exception:
            pass
        try:
            self.bot.loop.call_soon_threadsafe(self.next_event.set)
        except Exception:
            try:
                logger.exception("Failed to set next_event (source=%s)", source)
            except Exception:
                pass

    async def _playback_watchdog(self, timeout_secs: float, reason: str):
        try:
            await asyncio.sleep(timeout_secs)
            if not self.next_event.is_set():
                logger.warning("Playback watchdog triggered (%s) for guild %s, forcing next_event", reason, getattr(self.guild, 'id', None))
                try:
                    if self.voice_client and getattr(self.voice_client, 'is_playing', lambda: False)():
                        try:
                            self.voice_client.stop()
                        except Exception:
                            pass
                except Exception:
                    pass
                try:
                    self._set_next_event(source=f"watchdog_{reason}")
                except Exception:
                    pass
        except asyncio.CancelledError:
            return

    async def player_loop(self):
        logger.info("player_loop started for guild %s", self.guild.id)
        while True:
            # Automatic enqueue from background buffer removed

            try:
                logger.debug("Waiting for next track; queue size=%d (guild=%s)", self.queue.qsize(), getattr(self.guild, 'id', None))
            except Exception:
                pass
            self.next_event.clear()
            track: Track = await self.queue.get()
            try:
                logger.debug("Dequeued track: %s (queue size now=%d) for guild=%s", getattr(track, 'title', None), self.queue.qsize(), getattr(self.guild, 'id', None))
            except Exception:
                pass
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
                    if self._test_mode:
                        source = None
                    else:
                        if not self.ffmpeg:
                            logger.error("ffmpeg not available; cannot play local file: %s", track.filename)
                            self._set_next_event(source="ffmpeg_missing_local")
                            self.current = None
                            continue
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
                            logger.error("Local playback error (prefetched): %s", err)
                        # do not remove prefetched file
                        try:
                            self._set_next_event(source="after_local_prefetched", err=err)
                        except Exception:
                            try:
                                logger.exception("Failed in _after_local_prefetched")
                            except Exception:
                                pass

                    self.voice_client.play(source, after=_after_local_prefetched)
                    # start a watchdog to detect stalled playback (after callback not called)
                    watchdog_task = None
                    try:
                        timeout_secs = (track.duration * 2) if (track.duration and track.duration > 0) else 300
                        watchdog_task = self.bot.loop.create_task(self._playback_watchdog(timeout_secs, "prefetched"))
                    except Exception:
                        watchdog_task = None
                    await self.next_event.wait()
                    # ensure monitor is stopped
                    try:
                        if monitor_task and not monitor_task.done():
                            monitor_task.cancel()
                    except Exception:
                        pass
                    try:
                        if watchdog_task and not watchdog_task.done():
                            watchdog_task.cancel()
                    except Exception:
                        pass
                    self._record_play(track)
                    # if loop is enabled, requeue the track for immediate replay
                    try:
                        if self._handle_loop(track):
                            self.current = None
                            continue
                    except Exception:
                        pass
                    try:
                        # refill both queue prefetch and background buffer in background
                        self.bot.loop.create_task(self.ensure_prefetch_ahead(6))
                        # no buffer fill scheduled
                    except Exception:
                        logger.exception("refill tasks failed after play")
                    self.current = None
                    continue
                except Exception:
                    logger.exception("Playing prefetched file failed, falling back")

            if not force_download and getattr(track, 'source_url', None):
                try:
                    if self._test_mode:
                        source = None
                    else:
                        if not self.ffmpeg:
                            logger.error("ffmpeg not available; cannot stream URL: %s", track.source_url)
                            self._set_next_event(source="ffmpeg_missing_stream")
                            self.current = None
                            continue
                        source = discord.FFmpegPCMAudio(
                            track.source_url,
                            executable=self.ffmpeg,
                            before_options=FFMPEG_BEFORE_OPTS,
                            options=FFMPEG_OPTIONS,
                        )

                    def _after(err):
                        if err:
                            logger.error("Player error (stream): %s", err)
                        try:
                            self._set_next_event(source="after_stream", err=err)
                        except Exception:
                            try:
                                logger.exception("Failed in _after (stream)")
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
                    # start watchdog
                    watchdog_task = None
                    try:
                        timeout_secs = (track.duration * 2) if (track.duration and track.duration > 0) else 300
                        watchdog_task = self.bot.loop.create_task(self._playback_watchdog(timeout_secs, "stream"))
                    except Exception:
                        watchdog_task = None
                    await self.next_event.wait()
                    try:
                        if monitor_task and not monitor_task.done():
                            monitor_task.cancel()
                    except Exception:
                        pass
                    try:
                        if watchdog_task and not watchdog_task.done():
                            watchdog_task.cancel()
                    except Exception:
                        pass
                    self._record_play(track)
                    try:
                        if self._handle_loop(track):
                            self.current = None
                            continue
                    except Exception:
                        pass
                    try:
                        try:
                            self.bot.loop.create_task(self.ensure_prefetch_ahead(6))
                        except Exception:
                            pass
                        # no buffer fill scheduled
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
                        try:
                            self._set_next_event(source="stream_failed_live")
                        except Exception:
                            pass
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
                            # If failure is due to missing ffmpeg for m3u8/HLS, abort retries early
                            try:
                                msg = str(e).lower()
                            except Exception:
                                msg = ''
                            if 'ffmpeg' in msg or 'm3u8' in msg:
                                logger.error("yt-dlp download attempt %d failed due to ffmpeg/m3u8 issue for %s: %s", attempt, track.source_url, e)
                                raise e
                            logger.exception("yt-dlp download attempt %d failed for %s", attempt, track.source_url)
                        # small backoff before retrying
                        await asyncio.sleep(min(5 * attempt, 30))

                    # all attempts failed
                    if last_exc:
                        raise last_exc
                    raise RuntimeError("yt-dlp download failed without exception")

                dl_start = time.time()
                # ensure only one download runs per-guild at a time to reduce contention
                async with self._download_lock:
                    filename = await _attempt_download_with_retries()
                dl_elapsed = time.time() - dl_start
                logger.info("Download completed for '%s' in %.1fs -> %s", track.title, dl_elapsed, filename)

                if not filename or not os.path.exists(filename):
                    raise RuntimeError("Downloaded file not found: %s" % filename)

                # For local files, do not pass reconnect input options (they're only for network streams)
                track.filename = filename
                track.prefetched = track.prefetched or False
                if not self.ffmpeg:
                    logger.error("ffmpeg not available; cannot play downloaded file: %s", filename)
                    self._set_next_event(source="ffmpeg_missing_downloaded")
                    self.current = None
                    continue
                source = discord.FFmpegPCMAudio(filename, executable=self.ffmpeg, before_options="", options=FFMPEG_OPTIONS)

                def _after_local(err):
                    if err:
                        logger.error("Local playback error: %s", err)
                    # cleanup local file after play finishes only if it wasn't a prefetched file
                    try:
                        if not getattr(track, "prefetched", False):
                            os.remove(filename)
                    except Exception:
                        logger.exception("Failed removing downloaded file: %s", filename)
                    try:
                        self._set_next_event(source="after_local", err=err)
                    except Exception:
                        try:
                            logger.exception("Failed in _after_local")
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
                # start watchdog for downloaded file play
                watchdog_task = None
                try:
                    timeout_secs = (track.duration * 2) if (track.duration and track.duration > 0) else 300
                    watchdog_task = self.bot.loop.create_task(self._playback_watchdog(timeout_secs, "downloaded"))
                except Exception:
                    watchdog_task = None
                await self.next_event.wait()
                try:
                    if monitor_task and not monitor_task.done():
                        monitor_task.cancel()
                except Exception:
                    pass
                try:
                    if watchdog_task and not watchdog_task.done():
                        watchdog_task.cancel()
                except Exception:
                    pass
                self._record_play(track)
                try:
                    if self._handle_loop(track):
                        self.current = None
                        continue
                except Exception:
                    pass
                try:
                    self.bot.loop.create_task(self.ensure_prefetch_ahead(6))
                except Exception:
                    logger.exception("refill tasks failed after play")
                self.current = None
                continue
            except Exception:
                logger.exception("Download-and-play fallback failed for %s", track.title)
                # ensure we don't block the loop
                try:
                    self._set_next_event(source="download_and_play_fallback_failed")
                except Exception:
                    pass
            await self.next_event.wait()
            self._record_play(track)
            try:
                self.bot.loop.create_task(self.ensure_prefetch_ahead(6))
                # no buffer fill scheduled
            except Exception:
                logger.exception("refill tasks failed after play")
            self.current = None

    async def enqueue(self, track: Track):
        # Enqueue a track for playback
        try:
            await self.queue.put(track)
        except Exception:
            logger.exception("Failed to put track onto queue for guild %s: %s", getattr(self.guild, 'id', None), getattr(track, 'title', None))
            raise
        try:
            logger.info("Enqueued track for guild %s: %s (prefetched=%s, live=%s) (queue_size=%d)", self.guild.id, track.title, track.prefetched, track.is_live, self.queue.qsize())
        except Exception:
            logger.info("Enqueued track for guild %s: %s", getattr(self.guild, 'id', None), getattr(track, 'title', None))
        return True
        # prefetch disabled: enqueue will not trigger background downloads

    async def start_prefetch_for_track(self, track: Track):
        """Start a background prefetch for a single track (idempotent).

        Safe to call multiple times; will not duplicate work for same URL.
        """
        # Prefetching disabled globally: no-op
        return None

    async def ensure_prefetch_ahead(self, ahead: int = 6):
        """Ensure the next `ahead` tracks in the queue are being prefetched.

        This scans the internal queue and triggers `start_prefetch_for_track`
        for the first `ahead` items.
        """
        # Prefetching disabled globally: no-op
        return

    # Background support removed
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
