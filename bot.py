from __future__ import annotations

import asyncio
import logging
import os
import re
import shutil
from multiprocessing import Manager, Process

import discord
from typing import Optional
from discord.ext import commands

from player import MusicPlayer, Track, yt_dlp_get_url, yt_dlp_get_candidates
from worker import run_worker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dmbot")
# reduce noisy ffmpeg termination INFO logs from discord internals
logging.getLogger("discord.player").setLevel(logging.WARNING)

PREFIX = "!"


class ControllerBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        # Disable the default help command so we can register a custom one
        super().__init__(command_prefix=PREFIX, intents=intents, help_command=None)
        self.players: dict[int, MusicPlayer] = {}
        self.manager = Manager()
        self.task_queue = self.manager.Queue()
        self.worker_proc: Process | None = None

    async def setup_hook(self):
        # start a single worker process for downloads
        if not self.worker_proc:
            self.worker_proc = Process(target=run_worker, args=(self.task_queue,), daemon=True)
            self.worker_proc.start()
            logger.info("Started download worker process")

    def get_player(self, guild: discord.Guild) -> MusicPlayer:
        if guild.id not in self.players:
            self.players[guild.id] = MusicPlayer(self, guild)
        return self.players[guild.id]


def create_bot() -> ControllerBot:
    bot = ControllerBot()

    async def play(ctx: commands.Context, *, query: str):
        if not ctx.author.voice or not ctx.author.voice.channel:
            await ctx.send("You must be connected to a voice channel to play audio.")
            return
        channel = ctx.author.voice.channel
        player = bot.get_player(ctx.guild)

        if not player.voice_client or not player.voice_client.is_connected():
            player.voice_client = await channel.connect()

        # If forced-download mode is active (or deno missing), avoid extracting a stream URL here
        force_download = os.environ.get("DMBOT_FORCE_DOWNLOAD") == "1" or not shutil.which("deno")
        title = None
        stream_url = None
        webpage_url = None

        if force_download:
            # For forced-download mode: if the query looks like a URL, pass it
            # through; otherwise use a yt_dlp search prefix so the downloader
            # can resolve the correct video during extraction.
            title = f"Requested: {query}"
            if query.startswith("http://") or query.startswith("https://"):
                # probe the URL to detect live streams and a resolved stream URL if possible
                try:
                    stream_url, probed_title, webpage_url, is_live, duration = await yt_dlp_get_url(query)
                    if probed_title:
                        title = f"Requested: {probed_title}"
                except Exception:
                    # probing failed; fall back to passing the URL through
                    stream_url = query
                    webpage_url = query
            else:
                # let yt_dlp handle searching when we later download
                stream_url = None
                webpage_url = f"ytsearch1:{query}"
        else:
            try:
                stream_url, title, webpage_url, is_live, duration = await yt_dlp_get_url(query)
            except Exception as e:
                await ctx.send(f"Failed to get audio: {e}")
                return

        # debug: show which stream url was selected (best-effort)
        if stream_url:
            try:
                await ctx.send(f"Resolved stream: {stream_url}")
            except Exception:
                pass

        # include is_live flag when known
        try:
            is_live_flag = bool(is_live)
        except Exception:
            is_live_flag = False
        track = Track(title=title or query, source_url=stream_url, webpage_url=webpage_url, is_live=is_live_flag, duration=locals().get('duration', None))
        if player.is_playing():
            await player.enqueue(track)
            logger.info("Queued track for guild %s: %s", ctx.guild.id if ctx.guild else None, track.title)
            await ctx.send(f"Queued: {track.title}")
        else:
            await player.enqueue(track)
            logger.info("Enqueued and starting playback for guild %s: %s", ctx.guild.id if ctx.guild else None, track.title)
            await ctx.send(f"Playing: {track.title}")

    async def skip(ctx: commands.Context):
        player = bot.get_player(ctx.guild)
        if player.voice_client and player.voice_client.is_playing():
            logger.info("Skip requested by %s in guild %s", getattr(ctx.author, 'name', None), ctx.guild.id if ctx.guild else None)
            player.voice_client.stop()
            await ctx.send("Skipped.")
            # try to advance autoplay immediately if enabled
            if getattr(player, "autoplay", False):
                async def _advance():
                    try:
                        # attempt to reconnect if disconnected and author in voice
                        if (not player.voice_client or not player.voice_client.is_connected()) and ctx.author.voice and ctx.author.voice.channel:
                            try:
                                player.voice_client = await ctx.author.voice.channel.connect()
                                player.last_voice_channel_id = ctx.author.voice.channel.id
                            except Exception:
                                pass
                        # try using buffer or pick and enqueue
                        await player.fill_autoplay_buffer(5)
                        if player.autoplay_buffer:
                            nt = player.autoplay_buffer.popleft()
                            await player.enqueue(nt)
                        else:
                            nt = await player.pick_autoplay_track(player.last_played)
                            if nt:
                                await player.enqueue(nt)
                    except Exception:
                        logger.exception("Autoplay advance after skip failed")

                bot.loop.create_task(_advance())
        else:
            # nothing is playing; if autoplay is on attempt to start next
            if getattr(player, "autoplay", False):
                await ctx.send("Nothing is playing — attempting to resume autoplay.")
                async def _start():
                    try:
                        if (not player.voice_client or not player.voice_client.is_connected()) and ctx.author.voice and ctx.author.voice.channel:
                            try:
                                player.voice_client = await ctx.author.voice.channel.connect()
                                player.last_voice_channel_id = ctx.author.voice.channel.id
                            except Exception:
                                pass
                        await player.fill_autoplay_buffer(5)
                        if player.autoplay_buffer:
                            nt = player.autoplay_buffer.popleft()
                            await player.enqueue(nt)
                        else:
                            nt = await player.pick_autoplay_track(player.last_played)
                            if nt:
                                await player.enqueue(nt)
                    except Exception:
                        logger.exception("Autoplay start from skip/no-play failed")

                bot.loop.create_task(_start())
            else:
                await ctx.send("Nothing is playing.")

    async def _queue(ctx: commands.Context):
        player = bot.get_player(ctx.guild)
        items = list(player.queue._queue)
        if not items:
            await ctx.send("Queue is empty.")
            return
        lines = [f"{i+1}. {t.title}" for i, t in enumerate(items)]
        await ctx.send("\n".join(lines))

    async def stop_cmd(ctx: commands.Context):
        player = bot.get_player(ctx.guild)
        logger.info("Stop requested by %s in guild %s", getattr(ctx.author, 'name', None), ctx.guild.id if ctx.guild else None)
        await player.stop()
        await ctx.send("Stopped and cleared queue.")

    async def pause(ctx: commands.Context):
        player = bot.get_player(ctx.guild)
        if player.voice_client and player.voice_client.is_playing():
            player.voice_client.pause()
            await ctx.send("Paused.")
        else:
            await ctx.send("Nothing is playing.")

    async def resume(ctx: commands.Context):
        player = bot.get_player(ctx.guild)
        if player.voice_client and player.voice_client.is_paused():
            player.voice_client.resume()
            await ctx.send("Resumed.")
        else:
            await ctx.send("Nothing is paused.")

    async def volume(ctx: commands.Context, vol: int):
        await ctx.send("Volume control is not implemented in this simple demo.")

    async def help_cmd(ctx: commands.Context):
        """Send a HELP.txt file describing bot commands."""
        help_path = os.path.join(os.path.dirname(__file__), "HELP.txt")
        if os.path.exists(help_path):
            try:
                await ctx.send(file=discord.File(help_path))
                return
            except Exception:
                pass
        # fallback: send short inline help
        help_text = (
            "Commands: !play <query>, !skip, !autoplay [genre], !pause, !resume, !stop, !queue, !help"
        )
        await ctx.send(help_text)

    async def autoplay_cmd(ctx: commands.Context, *, genre: Optional[str] = None):
        player = bot.get_player(ctx.guild)
        if genre:
            # set specific genre and enable autoplay
            player.autoplay_genre = genre.strip().lower()
            player.autoplay = True
            await ctx.send(f"Autoplay ON (genre: {player.autoplay_genre})")
        else:
            # toggle
            player.autoplay = not getattr(player, "autoplay", False)
            status = "ON" if player.autoplay else "OFF"
            await ctx.send(f"Autoplay {status}")

        # If turned on and bot not connected but the author is in voice, join and start playing
        if player.autoplay:
            if (not player.voice_client or not player.voice_client.is_connected()) and ctx.author.voice and ctx.author.voice.channel:
                try:
                    channel = ctx.author.voice.channel
                    player.voice_client = await channel.connect()
                    player.last_voice_channel_id = channel.id
                except Exception as e:
                    await ctx.send(f"Failed to join voice channel: {e}")
                    return

            # if nothing is playing, enqueue an autoplay seed
            if not player.is_playing():
                try:
                    # start background buffer fill without waiting
                    try:
                        player.bot.loop.create_task(player.fill_autoplay_buffer(5))
                    except Exception:
                        logger.exception("Failed to start background autoplay buffer fill")

                    # first try a fast immediate search (fewer candidates) to reduce latency
                    try:
                        next_track = await asyncio.wait_for(player.pick_autoplay_track(player.last_played, max_results=6), timeout=8.0)
                        if next_track:
                            logger.info("Fast autoplay selected: %s", next_track.title)
                            # try to resolve a direct stream URL quickly to avoid download fallback delays
                            try:
                                if not next_track.source_url and (next_track.webpage_url or next_track.title):
                                    logger.info("Resolving stream URL for autoplay pick: %s", next_track.webpage_url or next_track.title)
                                    try:
                                        stream_url, title_r, webpage_r, is_live_r, duration_r = await asyncio.wait_for(yt_dlp_get_url(next_track.webpage_url or next_track.title, max_results=1), timeout=6.0)
                                        next_track.source_url = stream_url or next_track.source_url
                                        if duration_r:
                                            next_track.duration = duration_r
                                        if title_r:
                                            next_track.title = title_r
                                        # If no direct stream url found, try extracting individual candidates from the webpage (mix/playlist)
                                        if not next_track.source_url and next_track.webpage_url:
                                            try:
                                                logger.info("No direct stream URL; fetching candidates from %s", next_track.webpage_url)
                                                cands = await asyncio.wait_for(yt_dlp_get_candidates(next_track.webpage_url, max_results=6), timeout=8.0)
                                                for s_url, s_title, s_webpage, s_is_live in cands:
                                                    if s_url or s_webpage:
                                                        logger.info("Autoplay candidate chosen from webpage: %s", s_title or s_webpage)
                                                        next_track.source_url = s_url or next_track.source_url
                                                        next_track.webpage_url = s_webpage or next_track.webpage_url
                                                        if s_title:
                                                            next_track.title = s_title
                                                        next_track.is_live = s_is_live
                                                        break
                                            except asyncio.TimeoutError:
                                                logger.info("Candidate fetch timed out for %s", next_track.webpage_url)
                                            except Exception:
                                                logger.exception("Failed to fetch candidates for %s", next_track.webpage_url)
                                    except asyncio.TimeoutError:
                                        logger.info("Quick resolve timed out for autoplay pick: %s", next_track.title)
                                    except Exception:
                                        logger.exception("Quick resolve failed for autoplay pick: %s", next_track.title)
                            except Exception:
                                logger.exception("Unexpected error resolving autoplay pick stream URL")

                            try:
                                logger.info("Attempting to enqueue autoplay pick for guild %s: %s", ctx.guild.id if ctx.guild else None, next_track.title)
                                await player.enqueue(next_track)
                                await ctx.send(f"Autoplay started: {next_track.title}")
                            except Exception:
                                logger.exception("Failed to enqueue autoplay pick: %s", next_track.title)
                                await ctx.send("Autoplay failed to enqueue the selected track.")
                            return
                    except asyncio.TimeoutError:
                        logger.info("Fast autoplay pick timed out; attempting quick direct search before buffer/prefetch")
                        # try a very quick direct search by genre/title to get something playing fast
                        try:
                            quick_q = player.autoplay_genre or "popular music"
                            logger.info("Quick direct autoplay search using query: %s", quick_q)
                            try:
                                stream_url, title_r, webpage_r, is_live_r, duration_r = await asyncio.wait_for(yt_dlp_get_url(quick_q, max_results=1), timeout=6.0)
                            except asyncio.TimeoutError:
                                logger.info("Quick direct search timed out for query: %s", quick_q)
                                raise
                            if stream_url or webpage_r:
                                quick_track = Track(title=title_r or quick_q, source_url=stream_url, webpage_url=webpage_r, is_live=bool(is_live_r), duration=duration_r)
                                try:
                                    logger.info("Quick direct search found autoplay track: %s", quick_track.title)
                                    await player.enqueue(quick_track)
                                    await ctx.send(f"Autoplay started: {quick_track.title}")
                                    return
                                except Exception:
                                    logger.exception("Failed to enqueue quick autoplay track: %s", quick_track.title)
                        except Exception:
                            logger.info("Quick direct search did not produce a usable track; falling back to buffer/prefetch")
                    except Exception:
                        logger.exception("Fast autoplay pick failed; falling back to buffer/prefetch")

                    # attempt to ensure at least one prefetched autoplay track (short timeout)
                    ready = await player.ensure_autoplay_ready(min_prefetched=1, timeout=6.0)
                    if ready and player.autoplay_buffer:
                        next_track = player.autoplay_buffer.popleft()
                        try:
                            logger.info("Attempting to enqueue buffered autoplay track for guild %s: %s", ctx.guild.id if ctx.guild else None, next_track.title)
                            await player.enqueue(next_track)
                            await ctx.send(f"Autoplay started (buffered): {next_track.title}")
                        except Exception:
                            logger.exception("Failed to enqueue buffered autoplay track: %s", next_track.title)
                            await ctx.send("Autoplay failed to enqueue buffered track.")
                        return

                    # final fallback: try a full pick (longer but may still yield a result)
                    try:
                        next_track = await asyncio.wait_for(player.pick_autoplay_track(player.last_played), timeout=12.0)
                        if next_track:
                            try:
                                logger.info("Attempting to enqueue fallback autoplay pick for guild %s: %s", ctx.guild.id if ctx.guild else None, next_track.title)
                                await player.enqueue(next_track)
                                await ctx.send(f"Autoplay started: {next_track.title}")
                            except Exception:
                                logger.exception("Failed to enqueue fallback autoplay pick: %s", next_track.title)
                                await ctx.send("Autoplay failed to enqueue the selected track.")
                            return
                        else:
                            await ctx.send("Autoplay failed to find a track.")
                            return
                    except asyncio.TimeoutError:
                        await ctx.send("Autoplay timed out trying to find a track.")
                        return
                    except Exception:
                        logger.exception("Autoplay failed to find a track.")
                        await ctx.send("Autoplay failed to find a track.")
                        return
                except Exception:
                    await ctx.send("Autoplay failed to find a track.")

    # register commands
    bot.add_command(commands.Command(play, name="play"))
    bot.add_command(commands.Command(skip, name="next"))
    bot.add_command(commands.Command(skip, name="skip"))
    bot.add_command(commands.Command(help_cmd, name="help"))
    bot.add_command(commands.Command(_queue, name="queue"))
    bot.add_command(commands.Command(stop_cmd, name="stop"))
    bot.add_command(commands.Command(pause, name="pause"))
    bot.add_command(commands.Command(resume, name="resume"))
    bot.add_command(commands.Command(volume, name="volume"))
    bot.add_command(commands.Command(autoplay_cmd, name="autoplay"))
    async def status_cmd(ctx: commands.Context):
        player = bot.get_player(ctx.guild)
        vc = player.voice_client
        status = []
        status.append(f"Voice connected: {bool(vc and vc.is_connected())}")
        status.append(f"Is playing: {player.is_playing()}")
        status.append(f"Queue size: {player.queue.qsize()}")
        status.append(f"Autoplay buffer size: {len(player.autoplay_buffer)}")
        status.append(f"Autoplay enabled: {getattr(player, 'autoplay', False)}")
        status.append(f"Last played: {player.last_played.title if player.last_played else None}")
        await ctx.send("\n".join(status))
    bot.add_command(commands.Command(status_cmd, name="status"))

    return bot


def main():
    token = os.environ.get("DISCORD_TOKEN")
    if not token:
        # fallback to token file in project root (convenience for local runs)
        token_path = os.path.join(os.getcwd(), "token")
        if os.path.exists(token_path):
            with open(token_path, "r") as f:
                token = f.read().strip()
    if not token:
        raise RuntimeError("DISCORD_TOKEN environment variable must be set or token file available")
    bot = create_bot()
    bot.run(token)


if __name__ == "__main__":
    main()
