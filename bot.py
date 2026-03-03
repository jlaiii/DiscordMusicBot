from __future__ import annotations

import asyncio
import logging
import os
import re
import shutil
from multiprocessing import Manager, Process
import urllib.parse

import discord
from typing import Optional
from discord.ext import commands

from player import MusicPlayer, Track, yt_dlp_get_url, yt_dlp_get_candidates
from worker import run_worker
import playlists

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
        # initialize playlists DB
        try:
            await playlists.init_db()
            logger.info("Playlists DB initialized")
        except Exception:
            logger.exception("Failed to initialize playlists DB")
        # register persistent UI main menu view so controls survive restarts
        try:
            from ui import MainMenuView
            # add a persistent main menu view (timeout=None)
            try:
                self.add_view(MainMenuView(self, timeout=None))
                logger.info("Registered persistent MainMenuView")
            except Exception:
                logger.exception("Failed to register MainMenuView persistently")
        except Exception:
            # UI may not be available yet; ignore
            pass
        

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
                def _summarize_url(u: str) -> str:
                    try:
                        p = urllib.parse.urlparse(u)
                        host = p.netloc or ''
                        tail = os.path.basename(p.path) or ''
                        token = f"{host}/{tail}" if tail else host
                        if len(token) > 60:
                            token = token[:57] + "..."
                        return token
                    except Exception:
                        return u if len(u) <= 80 else u[:77] + "..."

                summary = _summarize_url(stream_url)
                if title:
                    await ctx.send(f"Resolved stream for '{title}': {summary}")
                else:
                    await ctx.send(f"Resolved stream: {summary}")
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
            return

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
            "Commands: !play <query>, !skip, !pause, !resume, !stop, !queue, !help"
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
    async def status_cmd(ctx: commands.Context):
        player = bot.get_player(ctx.guild)
        vc = player.voice_client
        status = []
        status.append(f"Voice connected: {bool(vc and vc.is_connected())}")
        status.append(f"Is playing: {player.is_playing()}")
        status.append(f"Queue size: {player.queue.qsize()}")
        status.append(f"Last played: {player.last_played.title if player.last_played else None}")
        await ctx.send("\n".join(status))
    async def nowplaying_cmd(ctx: commands.Context):
        player = bot.get_player(ctx.guild)
        track = player.current or player.last_played
        if not track:
            await ctx.send("Nothing is playing.")
            return

        def _fmt_duration(d: float | None) -> str:
            try:
                if not d or d <= 0:
                    return "Unknown"
                s = int(d)
                h, m = divmod(s, 3600)
                m, s = divmod(m, 60)
                if h:
                    return f"{h:d}:{m:02d}:{s:02d}"
                return f"{m:d}:{s:02d}"
            except Exception:
                return "Unknown"

        parts = []
        parts.append(f"Title: {track.title}")
        parts.append(f"Duration: {_fmt_duration(track.duration)}")
        parts.append(f"Live: {bool(getattr(track, 'is_live', False))}")
        if getattr(track, 'webpage_url', None):
            parts.append(f"URL: {track.webpage_url}")
        elif getattr(track, 'source_url', None):
            parts.append(f"Source: {track.source_url}")

        await ctx.send("\n".join(parts))
    bot.add_command(commands.Command(status_cmd, name="status"))
    bot.add_command(commands.Command(nowplaying_cmd, name="nowplaying"))

    async def loop_cmd(ctx: commands.Context):
        """Toggle looping of the current song for this guild's player."""
        player = bot.get_player(ctx.guild)
        player.loop = not getattr(player, 'loop', False)
        await ctx.send(f"Loop {'enabled' if player.loop else 'disabled'} for current track.")
    bot.add_command(commands.Command(loop_cmd, name="loop"))

    async def playlist_cmd(ctx: commands.Context, *args):
        """Dispatch playlist subcommands:
        create, add, remove, view, list, edit, delete, play
        """
        if not args:
            # open playlist browser GUI
            try:
                from ui import PlaylistBrowserView
                view = PlaylistBrowserView(bot, owner_id=str(ctx.author.id))
                await view._load(ctx.guild)
                embed = view._build_embed()
                view.build_select()
                msg = await ctx.send(embed=embed, view=view)
                try:
                    view.published_message = msg
                except Exception:
                    pass
                return
            except Exception as e:
                await ctx.send(f"Failed to open playlist browser: {e}")
                return
        sub = args[0].lower()
        try:
            if sub == "create":
                if len(args) < 2:
                    await ctx.send("Usage: !playlist create <name>")
                    return
                name = " ".join(args[1:]).strip()
                pid = await playlists.create_playlist(str(ctx.author.id), name)
                await ctx.send(f"Created playlist '{name}'.")
                return

            if sub == "add":
                if len(args) < 2:
                    await ctx.send("Usage: !playlist add <playlist name>")
                    return
                pname = " ".join(args[1:]).strip()
                player = bot.get_player(ctx.guild)
                track = player.current or player.last_played
                if not track:
                    await ctx.send("No current track to add. Play a song first.")
                    return
                ok = await playlists.add_item(str(ctx.author.id), pname, getattr(track, 'title', 'Unknown'), getattr(track, 'webpage_url', None), getattr(track, 'source_url', None), getattr(track, 'duration', None), bool(getattr(track, 'is_live', False)))
                if ok:
                    await ctx.send(f"Added '{track.title}' to playlist '{pname}'.")
                else:
                    await ctx.send(f"Failed to add to playlist '{pname}'. Does it exist and belong to you?")
                return

            if sub == "remove":
                if len(args) < 3:
                    await ctx.send("Usage: !playlist remove <playlist name> <song index>")
                    return
                pname = " ".join(args[1:-1]).strip()
                try:
                    idx = int(args[-1])
                except Exception:
                    await ctx.send("Index must be a number")
                    return
                ok = await playlists.remove_item(str(ctx.author.id), pname, idx)
                if ok:
                    await ctx.send(f"Removed item {idx} from '{pname}'.")
                else:
                    await ctx.send(f"Failed to remove item {idx} from '{pname}'.")
                return

            if sub == "view":
                if len(args) < 2:
                    await ctx.send("Usage: !playlist view <playlist name>")
                    return
                pname = " ".join(args[1:]).strip()
                meta = await playlists.view_playlist(str(ctx.author.id), pname)
                if not meta:
                    await ctx.send(f"Playlist '{pname}' not found or not visible.")
                    return
                lines = [f"Playlist: {meta['name']} (owner: {meta['owner_id']}, visibility: {meta['visibility']})"]
                if not meta.get('items'):
                    lines.append("(empty)")
                else:
                    def _fmt(d):
                        try:
                            if not d:
                                return "Unknown"
                            s = int(d)
                            h, m = divmod(s, 3600)
                            m, s = divmod(m, 60)
                            if h:
                                return f"{h:d}:{m:02d}:{s:02d}"
                            return f"{m:d}:{s:02d}"
                        except Exception:
                            return "Unknown"
                    for it in meta.get('items', []):
                        lines.append(f"{it['position']}. {it['title']} ({_fmt(it.get('duration'))})")
                await ctx.send("\n".join(lines))
                return

            if sub == "list":
                rows = await playlists.list_playlists_for_user(str(ctx.author.id))
                if not rows:
                    await ctx.send("No playlists found.")
                    return
                lines = [f"{r['name']} (owner: {r['owner_id']}, visibility: {r['visibility']})" for r in rows]
                await ctx.send("\n".join(lines))
                return

            if sub == "edit":
                if len(args) < 4:
                    await ctx.send("Usage: !playlist edit <playlist name> name <new name>  OR  !playlist edit <playlist name> visibility <public|private>")
                    return
                pname = args[1]
                op = args[2].lower()
                if op == "name":
                    newname = " ".join(args[3:]).strip()
                    ok = await playlists.edit_playlist(str(ctx.author.id), pname, new_name=newname)
                    if ok:
                        await ctx.send(f"Renamed playlist '{pname}' -> '{newname}'")
                    else:
                        await ctx.send("Edit failed. Are you the owner and does the playlist exist?")
                    return
                if op == "visibility":
                    val = args[3].lower()
                    if val not in ("public", "private"):
                        await ctx.send("visibility must be 'public' or 'private'")
                        return
                    ok = await playlists.edit_playlist(str(ctx.author.id), pname, visibility=val)
                    if ok:
                        await ctx.send(f"Updated visibility for '{pname}' -> {val}")
                    else:
                        await ctx.send("Edit failed. Are you the owner and does the playlist exist?")
                    return

            if sub == "delete":
                if len(args) < 2:
                    await ctx.send("Usage: !playlist delete <playlist name>")
                    return
                pname = " ".join(args[1:]).strip()
                ok = await playlists.delete_playlist(str(ctx.author.id), pname)
                if ok:
                    await ctx.send(f"Deleted playlist '{pname}'.")
                else:
                    await ctx.send("Delete failed. Are you the owner and does the playlist exist?")
                return

            if sub == "play":
                if len(args) < 2:
                    await ctx.send("Usage: !playlist play <playlist name>")
                    return
                pname = " ".join(args[1:]).strip()
                meta = await playlists.view_playlist(str(ctx.author.id), pname)
                if not meta:
                    await ctx.send(f"Playlist '{pname}' not found or not visible.")
                    return
                player = bot.get_player(ctx.guild)
                # ensure connected
                if (not player.voice_client or not player.voice_client.is_connected()) and ctx.author.voice and ctx.author.voice.channel:
                    try:
                        player.voice_client = await ctx.author.voice.channel.connect()
                    except Exception:
                        pass
                count = 0
                for it in meta.get('items', []):
                    tr = Track(title=it.get('title') or 'Unknown', source_url=it.get('source_url'), webpage_url=it.get('webpage_url'), duration=it.get('duration'), is_live=bool(it.get('is_live', False)))
                    await player.enqueue(tr)
                    count += 1
                await ctx.send(f"Enqueued {count} items from playlist '{pname}'.")
                return

            await ctx.send(f"Unknown subcommand: {sub}")
        except Exception as e:
            logger.exception("Playlist command failed")
            await ctx.send(f"Playlist command error: {e}")

    bot.add_command(commands.Command(playlist_cmd, name="playlist"))

    # UI menu command: send the MainMenuView (persistent view registered below)
    try:
        from ui import MainMenuView

        async def menu_cmd(ctx: commands.Context):
            view = MainMenuView(bot, timeout=None)
            emb = discord.Embed(title="Music Controls", description="Open the player controls below.")
            await ctx.send(embed=emb, view=view)

        bot.add_command(commands.Command(menu_cmd, name="menu"))
        bot.add_command(commands.Command(menu_cmd, name="player"))

        # register persistent view so component callbacks remain valid across restarts
        try:
            bot.add_view(MainMenuView(bot, timeout=None))
            try:
                from ui import PlaylistBrowserView
                # register a default browser view to preserve component callbacks across restarts
                bot.add_view(PlaylistBrowserView(bot, owner_id="0", timeout=None))
            except Exception:
                pass
        except Exception:
            # ignore failures to register persistent view (older runtimes)
            pass
    except Exception as e:
        # If UI failed to import, log the error and register a fallback command so users get feedback
        logger.exception("Failed to import UI module; UI commands unavailable")

        async def fallback_menu(ctx: commands.Context):
            await ctx.send("UI commands are currently unavailable. Check the bot logs for import errors.")

        bot.add_command(commands.Command(fallback_menu, name="menu"))
        bot.add_command(commands.Command(fallback_menu, name="player"))

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
