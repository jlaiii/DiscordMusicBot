from __future__ import annotations

import asyncio
import logging
import os
import re
import shutil
from multiprocessing import Manager, Process

import discord
from discord.ext import commands

from player import MusicPlayer, Track, yt_dlp_get_url
from worker import run_worker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dmbot")

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
                stream_url = query
                webpage_url = query
            else:
                # let yt_dlp handle searching when we later download
                stream_url = None
                webpage_url = f"ytsearch1:{query}"
        else:
            try:
                stream_url, title, webpage_url = await yt_dlp_get_url(query)
            except Exception as e:
                await ctx.send(f"Failed to get audio: {e}")
                return

        # debug: show which stream url was selected (best-effort)
        if stream_url:
            try:
                await ctx.send(f"Resolved stream: {stream_url}")
            except Exception:
                pass

        track = Track(title=title or query, source_url=stream_url, webpage_url=webpage_url)
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

    # register commands
    bot.add_command(commands.Command(play, name="play"))
    bot.add_command(commands.Command(skip, name="next"))
    bot.add_command(commands.Command(skip, name="skip"))
    bot.add_command(commands.Command(_queue, name="queue"))
    bot.add_command(commands.Command(stop_cmd, name="stop"))
    bot.add_command(commands.Command(pause, name="pause"))
    bot.add_command(commands.Command(resume, name="resume"))
    bot.add_command(commands.Command(volume, name="volume"))

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
