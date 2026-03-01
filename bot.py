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

from player import MusicPlayer, Track, yt_dlp_get_url
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
        super().__init__(command_prefix=PREFIX, intents=intents)
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
                    stream_url, probed_title, webpage_url, is_live = await yt_dlp_get_url(query)
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
                stream_url, title, webpage_url, is_live = await yt_dlp_get_url(query)
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
        track = Track(title=title or query, source_url=stream_url, webpage_url=webpage_url, is_live=is_live_flag)
        if player.is_playing():
            await player.enqueue(track)
            await ctx.send(f"Queued: {track.title}")
        else:
            await player.enqueue(track)
            await ctx.send(f"Playing: {track.title}")

    async def skip(ctx: commands.Context):
        player = bot.get_player(ctx.guild)
        if player.voice_client and player.voice_client.is_playing():
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
                    # attempt to ensure at least one prefetched autoplay track (wait briefly)
                    ready = await player.ensure_autoplay_ready(min_prefetched=1, timeout=10.0)
                    if ready and player.autoplay_buffer:
                        next_track = player.autoplay_buffer.popleft()
                        await player.enqueue(next_track)
                        await ctx.send(f"Autoplay started (buffered): {next_track.title}")
                    else:
                        # fallback to immediate pick if buffer not ready
                        next_track = await player.pick_autoplay_track(player.last_played)
                        if next_track:
                            await player.enqueue(next_track)
                            await ctx.send(f"Autoplay started: {next_track.title}")
                        else:
                            await ctx.send("Autoplay failed to find a track.")
                except Exception:
                    await ctx.send("Autoplay failed to find a track.")

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
