from __future__ import annotations

import discord
import asyncio
from discord import ui
from typing import Optional

from player import Track
import playlists




async def safe_interaction_send(interaction: discord.Interaction, content: str = None, delete_after: int | None = 20, **kwargs):
    """Try to respond to an interaction safely: response -> followup -> channel message.

    For non-ephemeral messages, schedule deletion after `delete_after` seconds (default 20).
    If `delete_after` is None, do not auto-delete.
    """
    if content is None:
        return

    async def _schedule_delete(msg: discord.Message, delay: int):
        try:
            await asyncio.sleep(delay)
            try:
                await msg.delete()
            except Exception:
                pass
        except asyncio.CancelledError:
            return

    # Try primary response
    try:
        await interaction.response.send_message(content, **kwargs)
        # if message is non-ephemeral and we should delete it, schedule deletion
        if not kwargs.get('ephemeral', False) and delete_after:
            try:
                msg = await interaction.original_response()
                interaction.client.loop.create_task(_schedule_delete(msg, delete_after))
            except Exception:
                pass
        return
    except Exception:
        pass

    # Try followup
    try:
        msg = await interaction.followup.send(content, **kwargs)
        if not kwargs.get('ephemeral', False) and delete_after and isinstance(msg, discord.Message):
            try:
                interaction.client.loop.create_task(_schedule_delete(msg, delete_after))
            except Exception:
                pass
        return
    except Exception:
        pass

    # Final fallback: channel message
    try:
        ch = getattr(interaction, 'channel', None)
        if ch:
            msg = await ch.send(content)
            if delete_after and isinstance(msg, discord.Message):
                try:
                    interaction.client.loop.create_task(_schedule_delete(msg, delete_after))
                except Exception:
                    pass
    except Exception:
        pass


class MainMenuView(ui.View):
    def __init__(self, bot, timeout: Optional[float] = None):
        super().__init__(timeout=timeout)
        self.bot = bot

    @ui.button(label="Play/Pause", style=discord.ButtonStyle.primary, custom_id="dmbot:playpause")
    async def playpause(self, interaction: discord.Interaction, button: ui.Button):
        player = self.bot.get_player(interaction.guild)
        # toggle pause/resume
        if player.voice_client and player.voice_client.is_playing():
            player.voice_client.pause()
            await interaction.response.send_message("Paused.", ephemeral=True)
        elif player.voice_client and player.voice_client.is_paused():
            player.voice_client.resume()
            await interaction.response.send_message("Resumed.", ephemeral=True)
        else:
            await interaction.response.send_message("Nothing is playing.", ephemeral=True)

    @ui.button(label="Skip", style=discord.ButtonStyle.secondary, custom_id="dmbot:skip")
    async def skip(self, interaction: discord.Interaction, button: ui.Button):
        player = self.bot.get_player(interaction.guild)
        if player.voice_client and player.voice_client.is_playing():
            player.voice_client.stop()
            await interaction.response.send_message("Skipped.", ephemeral=True)
        else:
            await interaction.response.send_message("Nothing is playing.", ephemeral=True)

    @ui.button(label="Loop", style=discord.ButtonStyle.secondary, custom_id="dmbot:loop")
    async def loop_btn(self, interaction: discord.Interaction, button: ui.Button):
        player = self.bot.get_player(interaction.guild)
        player.loop = not getattr(player, 'loop', False)
        await interaction.response.send_message(f"Loop {'enabled' if player.loop else 'disabled'}.", ephemeral=True)

    @ui.button(label="Stop", style=discord.ButtonStyle.danger, custom_id="dmbot:stop")
    async def stop(self, interaction: discord.Interaction, button: ui.Button):
        player = self.bot.get_player(interaction.guild)
        await player.stop()
        await interaction.response.send_message("Stopped and cleared queue.", ephemeral=True)

    @ui.button(label="Save Current", style=discord.ButtonStyle.success, custom_id="dmbot:save_current")
    async def save_current(self, interaction: discord.Interaction, button: ui.Button):
        # Open playlist browser so the user can pick which playlist to save the current track into
        view = PlaylistBrowserView(self.bot, owner_id=str(interaction.user.id), save_target=True)
        await view._load()
        embed = view._build_embed()
        view.build_select()
        # remember originating interaction so the view can clear itself on timeout
        view._originating_interaction = interaction
        await interaction.response.send_message("Choose a playlist to save the current track:", ephemeral=True, embed=embed, view=view)

    

    @ui.button(label="Playlists", style=discord.ButtonStyle.primary, custom_id="dmbot:playlists")
    async def playlists_btn(self, interaction: discord.Interaction, button: ui.Button):
        # open playlist browser view (ephemeral)
        view = PlaylistBrowserView(self.bot, owner_id=str(interaction.user.id))
        await view._load()
        embed = view._build_embed()
        # attach the dynamic select to let user choose a playlist
        view.build_select()
        view._originating_interaction = interaction
        await interaction.response.send_message(embed=embed, ephemeral=True, view=view)

    @ui.button(label="Search/Play", style=discord.ButtonStyle.primary, custom_id="dmbot:search_play")
    async def search_play(self, interaction: discord.Interaction, button: ui.Button):
        # open modal to accept a search/query and play it
        await interaction.response.send_modal(SearchModal(self.bot))

class PlaylistBrowserView(ui.View):
    def __init__(self, bot, owner_id: str, page: int = 0, timeout: Optional[float] = None, save_target: bool = False):
        super().__init__(timeout=timeout)
        self.bot = bot
        self.owner_id = owner_id
        self.page = page
        self.per_page = 10
        self.playlists_cache = []
        self.select: Optional[ui.Select] = None
        # if True, selecting a playlist will save the current track to it
        self.save_target = bool(save_target)
        self._originating_interaction: Optional[discord.Interaction] = None

    async def _load(self, guild: Optional[discord.Guild] = None):
        try:
            self.playlists_cache = await playlists.list_playlists_for_user(self.owner_id)
        except Exception:
            self.playlists_cache = []
        # resolve owner display names (best-effort, use cache then fetch)
        owners = {p.get('owner_id') for p in self.playlists_cache}
        owner_names: dict[str, str] = {}
        for oid in owners:
            if not oid:
                continue
            try:
                uid = int(oid)
            except Exception:
                owner_names[oid] = oid
                continue
            # try guild member first
            display = None
            try:
                if guild:
                    mem = guild.get_member(uid)
                    if mem:
                        display = getattr(mem, 'display_name', None) or getattr(mem, 'name', None)
                if not display:
                    usr = self.bot.get_user(uid)
                    if usr:
                        display = f"{getattr(usr, 'name', uid)}#{getattr(usr, 'discriminator', '')}" if getattr(usr, 'discriminator', None) else getattr(usr, 'name', str(uid))
                if not display:
                    # last resort: fetch user
                    try:
                        usr = await self.bot.fetch_user(uid)
                        display = f"{getattr(usr, 'name', uid)}#{getattr(usr, 'discriminator', '')}" if getattr(usr, 'discriminator', None) else getattr(usr, 'name', str(uid))
                    except Exception:
                        display = str(uid)
            except Exception:
                display = str(uid)
            owner_names[oid] = display or str(uid)
        # attach owner_name to each playlist entry
        for p in self.playlists_cache:
            p['owner_name'] = owner_names.get(p.get('owner_id'), p.get('owner_id'))

    def _build_embed(self):
        embed = discord.Embed(title="Playlists", description=f"Page {self.page+1}")
        start = self.page * self.per_page
        end = start + self.per_page
        items = self.playlists_cache[start:end]
        if not items:
            embed.description = "No playlists found."
            return embed
        for p in items:
            owner_display = p.get('owner_name') or p.get('owner_id')
            name = p.get('name')
            vis = p.get('visibility')
            embed.add_field(name=f"{name}", value=f"Owner: {owner_display} • {vis}", inline=False)
        return embed

    def build_select(self):
        # remove existing dynamic select if present
        if self.select:
            try:
                self.remove_item(self.select)
            except Exception:
                pass
        start = self.page * self.per_page
        end = start + self.per_page
        items = self.playlists_cache[start:end]
        options = []
        for p in items:
            label = p.get('name') or 'Untitled'
            owner_display = p.get('owner_name') or p.get('owner_id')
            desc = f"Owner: {owner_display} • {p.get('visibility')}"
            options.append(discord.SelectOption(label=label, description=desc, value=str(p.get('id'))))
        if not options:
            return
        class _PlSelect(ui.Select):
            def __init__(self, opts):
                super().__init__(placeholder="Select a playlist…", min_values=1, max_values=1, options=opts)

            async def callback(self, interaction: discord.Interaction):
                view = getattr(self, 'view', None)
                if view is None:
                    await interaction.response.send_message("Internal error: view not available.", ephemeral=True)
                    return
                pid = int(self.values[0])
                # find playlist dict
                picked = None
                for pl in view.playlists_cache:
                    if pl.get('id') == pid:
                        picked = pl
                        break
                if not picked:
                    await interaction.response.send_message("Playlist not found.", ephemeral=True)
                    return
                # build a small embed summarizing the playlist
                try:
                    meta = await playlists.view_playlist(picked.get('owner_id'), picked.get('name'))
                except Exception:
                    meta = picked
                emb = discord.Embed(title=f"{picked.get('name')}", description=f"Owner: {picked.get('owner_name') or picked.get('owner_id')} • {picked.get('visibility')}")
                if meta and meta.get('items'):
                    # show up to 6 items
                    lines = []
                    for it in meta.get('items', [])[:6]:
                        lines.append(f"{it.get('position')}. {it.get('title')}")
                    emb.add_field(name="Items", value="\n".join(lines), inline=False)
                # if this browser was opened as a save-target picker, save current track instead
                if getattr(view, 'save_target', False):
                    try:
                        # try to get current track from guild player
                        player = view.bot.get_player(interaction.guild)
                        track = getattr(player, 'current', None) or getattr(player, 'last_played', None)
                        if not track:
                            await interaction.response.send_message("No current track to save.", ephemeral=True)
                            return
                        # if picked playlist belongs to the user, add directly; otherwise save to user's 'Saved' playlist
                        if str(picked.get('owner_id')) == str(interaction.user.id):
                            owner = str(interaction.user.id)
                            pname = picked.get('name')
                        else:
                            owner = str(interaction.user.id)
                            pname = "Saved"
                            try:
                                await playlists.create_playlist(owner, pname)
                            except Exception:
                                pass
                        ok = await playlists.add_item(owner, pname, getattr(track, 'title', 'Unknown'), getattr(track, 'webpage_url', None), getattr(track, 'source_url', None), getattr(track, 'duration', None), bool(getattr(track, 'is_live', False)))
                        if ok:
                            await interaction.response.send_message(f"Saved '{getattr(track, 'title', 'Unknown')}' to playlist '{pname}'.", ephemeral=True)
                        else:
                            await interaction.response.send_message("Failed to save to selected playlist.", ephemeral=True)
                    except Exception as e:
                        await interaction.response.send_message(f"Failed to save: {e}", ephemeral=True)
                    return

                # open action select view — defer then follow up to avoid double-response issues
                try:
                    await interaction.response.defer(ephemeral=True)
                    action_view = PlaylistActionSelectView(view.bot, picked)
                    # track originating interaction so the action view can clear itself
                    action_view._originating_interaction = interaction
                    await interaction.followup.send(embed=emb, ephemeral=True, view=action_view)
                except Exception as e:
                    try:
                        await interaction.followup.send(f"Failed to open actions: {e}", ephemeral=True)
                    except Exception:
                        pass

        self.select = _PlSelect(options)
        self.add_item(self.select)

    @ui.button(label="Prev", style=discord.ButtonStyle.secondary, custom_id="dmbot:pl_prev")
    async def prev(self, interaction: discord.Interaction, button: ui.Button):
        await self._load(interaction.guild)
        if self.page > 0:
            self.page -= 1
        embed = self._build_embed()
        self.build_select()
        await interaction.response.edit_message(content=None, embed=embed, view=self)

    @ui.button(label="Next", style=discord.ButtonStyle.secondary, custom_id="dmbot:pl_next")
    async def next(self, interaction: discord.Interaction, button: ui.Button):
        await self._load(interaction.guild)
        max_pages = max(0, (len(self.playlists_cache) - 1) // self.per_page)
        if self.page < max_pages:
            self.page += 1
        embed = self._build_embed()
        self.build_select()
        await interaction.response.edit_message(content=None, embed=embed, view=self)

    @ui.button(label="Back", style=discord.ButtonStyle.primary, custom_id="dmbot:pl_back")
    async def back(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.edit_message(content="Closed playlists.", embed=None, view=None)

    @ui.button(label="Create", style=discord.ButtonStyle.success, custom_id="dmbot:pl_create")
    async def create(self, interaction: discord.Interaction, button: ui.Button):
        try:
            await interaction.response.send_modal(CreatePlaylistModal())
        except Exception as e:
            await interaction.response.send_message(f"Failed to open create modal: {e}", ephemeral=True)

    @ui.button(label="Share All Playlists", style=discord.ButtonStyle.primary, custom_id="dmbot:pl_toggle_share")
    async def toggle_share_menu(self, interaction: discord.Interaction, button: ui.Button):
        # Only the browser owner may publish the playlist browser to the channel
        if str(interaction.user.id) != str(self.owner_id):
            await interaction.response.send_message("Only the browser owner can publish this playlist list.", ephemeral=True)
            return
        # Build a public embed listing available playlists and post to channel with import controls
        try:
            await interaction.response.defer()
        except Exception:
            pass
        try:
            # Ensure latest cache
            await self._load(interaction.guild)
            emb = discord.Embed(title=f"Shared playlists from {interaction.user.display_name}", description="Browse and import playlists below")
            for p in self.playlists_cache[:20]:
                emb.add_field(name=p.get('name') or 'Untitled', value=f"Owner: {p.get('owner_name') or p.get('owner_id')} • {p.get('visibility')}", inline=False)
            public_view = PublicShareAllView(self.playlists_cache)
            ch = interaction.channel
            if ch:
                await ch.send(embed=emb, view=public_view)
                try:
                    await interaction.followup.send("Published playlist browser to channel.", ephemeral=True)
                except Exception:
                    pass
            else:
                try:
                    await interaction.followup.send("Could not find channel to publish in.", ephemeral=True)
                except Exception:
                    pass
        except Exception as e:
            try:
                await interaction.response.send_message(f"Failed to publish playlist browser: {e}", ephemeral=True)
            except Exception:
                pass

        @ui.button(label="Toggle Visibility", style=discord.ButtonStyle.secondary, custom_id="dmbot:pl_toggle_visibility")
        async def toggle_visibility(self, interaction: discord.Interaction, button: ui.Button):
            # Only the browser owner may toggle visibility of their playlists
            if str(interaction.user.id) != str(self.owner_id):
                await interaction.response.send_message("Only the browser owner can toggle visibility for their playlists.", ephemeral=True)
                return
            try:
                await interaction.response.defer(ephemeral=True)
            except Exception:
                pass
            try:
                # refresh cache
                await self._load(interaction.guild)
                # determine if we should make public (if any of the owner's playlists are private)
                owner_playlists = [p for p in self.playlists_cache if str(p.get('owner_id')) == str(self.owner_id)]
                if not owner_playlists:
                    await interaction.followup.send("You have no playlists to toggle.", ephemeral=True)
                    return
                make_public = any(p.get('visibility') != 'public' for p in owner_playlists)
                new_vis = 'public' if make_public else 'private'
                changed = 0
                for p in owner_playlists:
                    try:
                        ok = await playlists.edit_playlist(str(self.owner_id), p.get('name'), visibility=new_vis)
                        if ok:
                            p['visibility'] = new_vis
                            changed += 1
                    except Exception:
                        continue
                # reload and update view
                await self._load(interaction.guild)
                emb = self._build_embed()
                self.build_select()
                try:
                    await interaction.followup.send(f"Updated {changed} playlists to '{new_vis}'.", ephemeral=True)
                except Exception:
                    pass
                try:
                    await interaction.edit_original_response(embed=emb, view=self)
                except Exception:
                    try:
                        await interaction.response.edit_message(embed=emb, view=self)
                    except Exception:
                        pass
            except Exception as e:
                try:
                    await interaction.followup.send(f"Failed to toggle visibility: {e}", ephemeral=True)
                except Exception:
                    pass

    async def on_timeout(self):
        # Intentionally no-op: keep the playlist browser open until the user closes it.
        return

    async def on_error(self, error: Exception, item, interaction: discord.Interaction):
        try:
            await interaction.response.send_message(f"Error: {error}", ephemeral=True)
        except Exception:
            pass


class PlaylistActionsView(ui.View):
    def __init__(self, bot, playlist: dict, timeout: Optional[float] = None):
        super().__init__(timeout=timeout)
        self.bot = bot
        self.playlist = playlist
        self._originating_interaction: Optional[discord.Interaction] = None

    @ui.button(label="View", style=discord.ButtonStyle.primary, custom_id="dmbot:pl_action_view")
    async def view_items(self, interaction: discord.Interaction, button: ui.Button):
        try:
            # fetch the exact playlist by its owner so we show the selected playlist
            meta = await playlists.view_playlist(self.playlist.get('owner_id'), self.playlist.get('name'))
        except Exception as e:
            await interaction.response.send_message(f"Failed to read playlist: {e}", ephemeral=True)
            return
        if not meta:
            await interaction.response.send_message("Playlist not found or not visible.", ephemeral=True)
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
        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    @ui.button(label="Get (Import)", style=discord.ButtonStyle.secondary, custom_id="dmbot:pl_action_get")
    async def get_import(self, interaction: discord.Interaction, button: ui.Button):
        try:
            meta = await playlists.view_playlist(self.playlist.get('owner_id'), self.playlist.get('name'))
        except Exception as e:
            await interaction.response.send_message(f"Failed to fetch playlist: {e}", ephemeral=True)
            return
        if not meta:
            await interaction.response.send_message("Failed to fetch playlist.", ephemeral=True)
            return
        newname = f"Imported {meta.get('name')}"
        owner = str(interaction.user.id)
        try:
            await playlists.create_playlist(owner, newname)
        except Exception:
            # ignore if exists
            pass
        player = self.bot.get_player(interaction.guild)
        # ensure connected if user in voice and bot not connected
        if (not player.voice_client or not player.voice_client.is_connected()) and interaction.user.voice and interaction.user.voice.channel:
            try:
                player.voice_client = await interaction.user.voice.channel.connect()
            except Exception:
                pass
        count = 0
        for it in meta.get('items', []):
            tr = Track(title=it.get('title') or 'Unknown', source_url=it.get('source_url'), webpage_url=it.get('webpage_url'), duration=it.get('duration'), is_live=bool(it.get('is_live', False)))
            try:
                await player.enqueue(tr)
                count += 1
            except Exception:
                continue
        await interaction.response.send_message(f"Enqueued {count} items from playlist '{meta.get('name')}'.", ephemeral=True)

    @ui.button(label="Add Current To This", style=discord.ButtonStyle.success, custom_id="dmbot:pl_action_add_to_this")
    async def add_current_to_this(self, interaction: discord.Interaction, button: ui.Button):
        # only owner may add directly to this playlist
        if str(interaction.user.id) != str(self.playlist.get('owner_id')):
            await interaction.response.send_message("Only the owner can add directly to this playlist. You can import it instead.", ephemeral=True)
            return
        try:
            player = self.bot.get_player(interaction.guild)
        except Exception as e:
            await interaction.response.send_message(f"Failed to access player: {e}", ephemeral=True)
            return
        track = player.current or player.last_played
        if not track:
            await interaction.response.send_message("No current track to add.", ephemeral=True)
            return
        ok = await playlists.add_item(str(interaction.user.id), self.playlist.get('name'), getattr(track, 'title', 'Unknown'), getattr(track, 'webpage_url', None), getattr(track, 'source_url', None), getattr(track, 'duration', None), bool(getattr(track, 'is_live', False)))
        if ok:
            await interaction.response.send_message(f"Added '{track.title}' to '{self.playlist.get('name')}'.", ephemeral=True)
        else:
            await interaction.response.send_message("Failed to add to playlist.", ephemeral=True)

    @ui.button(label="Remove Item", style=discord.ButtonStyle.secondary, custom_id="dmbot:pl_action_remove_item")
    async def remove_item(self, interaction: discord.Interaction, button: ui.Button):
        # only owner may remove
        if str(interaction.user.id) != str(self.playlist.get('owner_id')):
            await interaction.response.send_message("Only the owner can remove items.", ephemeral=True)
            return
        try:
            await interaction.response.send_modal(RemoveItemModal(self.playlist.get('name')))
        except Exception as e:
            await interaction.response.send_message(f"Failed to open remove modal: {e}", ephemeral=True)

    @ui.button(label="Add Current (to mine)", style=discord.ButtonStyle.success, custom_id="dmbot:pl_action_addcur")
    async def add_current(self, interaction: discord.Interaction, button: ui.Button):
        try:
            player = self.bot.get_player(interaction.guild)
        except Exception as e:
            await interaction.response.send_message(f"Failed to access player: {e}", ephemeral=True)
            return
        track = player.current or player.last_played
        if not track:
            await interaction.response.send_message("No current track to add.", ephemeral=True)
            return
        owner = str(interaction.user.id)
        pname = "Saved"
        try:
            await playlists.create_playlist(owner, pname)
        except Exception:
            pass
        ok = await playlists.add_item(owner, pname, getattr(track, 'title', 'Unknown'), getattr(track, 'webpage_url', None), getattr(track, 'source_url', None), getattr(track, 'duration', None), bool(getattr(track, 'is_live', False)))
        if ok:
            await interaction.response.send_message(f"Saved '{track.title}' to your playlist '{pname}'.", ephemeral=True)
        else:
            await interaction.response.send_message(f"Failed to save to playlist '{pname}'.", ephemeral=True)

    async def on_timeout(self):
        # Intentionally no-op: keep this actions view open until the user closes it.
        return

    @ui.button(label="Edit", style=discord.ButtonStyle.secondary, custom_id="dmbot:pl_action_edit")
    async def edit(self, interaction: discord.Interaction, button: ui.Button):
        # only owner may edit
        if str(interaction.user.id) != str(self.playlist.get('owner_id')):
            await interaction.response.send_message("Only the owner can edit this playlist.", ephemeral=True)
            return
        try:
            await interaction.response.send_modal(EditPlaylistModal(self.playlist.get('name')))
        except Exception as e:
            await interaction.response.send_message(f"Failed to open edit modal: {e}", ephemeral=True)

    @ui.button(label="Delete", style=discord.ButtonStyle.danger, custom_id="dmbot:pl_action_delete")
    async def delete(self, interaction: discord.Interaction, button: ui.Button):
        if str(interaction.user.id) != str(self.playlist.get('owner_id')):
            await interaction.response.send_message("Only the owner can delete this playlist.", ephemeral=True)
            return
        # simple confirmation
        confirm = ConfirmDeleteView(self.playlist.get('name'))
        await interaction.response.send_message(f"Confirm delete playlist '{self.playlist.get('name')}'?", ephemeral=True, view=confirm)

    @ui.button(label="Share/Unshare", style=discord.ButtonStyle.primary, custom_id="dmbot:pl_action_share")
    async def share_toggle(self, interaction: discord.Interaction, button: ui.Button):
        if str(interaction.user.id) != str(self.playlist.get('owner_id')):
            await interaction.response.send_message("Only the owner can change visibility.", ephemeral=True)
            return
        cur_vis = self.playlist.get('visibility')
        new_vis = 'private' if cur_vis == 'public' else 'public'
        try:
            ok = await playlists.edit_playlist(str(interaction.user.id), self.playlist.get('name'), visibility=new_vis)
        except Exception as e:
            await interaction.response.send_message(f"Failed to update visibility: {e}", ephemeral=True)
            return
        if ok:
            self.playlist['visibility'] = new_vis
            await safe_interaction_send(interaction, f"Updated visibility -> {new_vis}", ephemeral=True)
        else:
            await safe_interaction_send(interaction, "Failed to update visibility.", ephemeral=True)



class PublicShareView(ui.View):
    """A public view posted to a channel allowing anyone to import a shared playlist."""
    def __init__(self, meta: dict):
        super().__init__(timeout=None)
        self.meta = meta

    @ui.button(label="Import", style=discord.ButtonStyle.success, custom_id="dmbot:public_import")
    async def import_btn(self, interaction: discord.Interaction, button: ui.Button):
        try:
            meta = self.meta
            owner = str(interaction.user.id)
            newname = f"Imported {meta.get('name')}"
            try:
                await playlists.create_playlist(owner, newname)
            except Exception:
                pass
            count = 0
            for it in meta.get('items', []):
                try:
                    await playlists.add_item(owner, newname, it.get('title') or 'Unknown', it.get('webpage_url'), it.get('source_url'), it.get('duration'), bool(it.get('is_live', False)))
                    count += 1
                except Exception:
                    continue
            await interaction.response.send_message(f"Imported {count} items into playlist '{newname}'.", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"Import failed: {e}", ephemeral=True)

    @ui.button(label="Close", style=discord.ButtonStyle.secondary, custom_id="dmbot:public_close")
    async def close_btn(self, interaction: discord.Interaction, button: ui.Button):
        try:
            await interaction.message.delete()
            await interaction.response.send_message("Shared message removed.", ephemeral=True)
        except Exception:
            await interaction.response.send_message("Could not remove message.", ephemeral=True)


class PublicShareAllView(ui.View):
    """Public view with a select of multiple playlists to allow importing one."""
    def __init__(self, playlists_list: list[dict]):
        super().__init__(timeout=None)
        self.playlists_map = {str(p.get('id')): p for p in playlists_list}

        class _ShareSel(ui.Select):
            def __init__(self, opts):
                super().__init__(placeholder='Select playlist to import…', min_values=1, max_values=1, options=opts)

            async def callback(self, interaction: discord.Interaction):
                # no immediate action; import triggered by button
                try:
                    await interaction.response.defer(ephemeral=True)
                    await interaction.followup.send(f"Selected playlist to import: {self.values[0]}", ephemeral=True)
                except Exception:
                    pass

        opts = [discord.SelectOption(label=p.get('name') or 'Untitled', value=str(p.get('id'))) for p in playlists_list]
        self.add_item(_ShareSel(opts))

    @ui.button(label="Import Selected", style=discord.ButtonStyle.success, custom_id="dmbot:public_import_selected")
    async def import_selected(self, interaction: discord.Interaction, button: ui.Button):
        # find select value
        sel = None
        for item in self.children:
            if isinstance(item, ui.Select) and item.values:
                sel = item.values[0]
                break
        if not sel:
            await interaction.response.send_message("No playlist selected.", ephemeral=True)
            return
        meta = self.playlists_map.get(sel)
        if not meta:
            await interaction.response.send_message("Playlist not found.", ephemeral=True)
            return
        owner = str(interaction.user.id)
        newname = f"Imported {meta.get('name')}"
        try:
            await playlists.create_playlist(owner, newname)
        except Exception:
            pass
        count = 0
        for it in meta.get('items', []):
            try:
                await playlists.add_item(owner, newname, it.get('title') or 'Unknown', it.get('webpage_url'), it.get('source_url'), it.get('duration'), bool(it.get('is_live', False)))
                count += 1
            except Exception:
                continue
        await interaction.response.send_message(f"Imported {count} items into playlist '{newname}'.", ephemeral=True)


class PlaylistActionSelectView(ui.View):
    """A view containing a Select with actions for a chosen playlist."""
    def __init__(self, bot, playlist: dict, parent_browser: Optional[object] = None, timeout: Optional[float] = None):
        super().__init__(timeout=timeout)
        self.bot = bot
        self.playlist = playlist
        self.parent_browser = parent_browser
        self._originating_interaction: Optional[discord.Interaction] = None
        # build the centralized action options for the import/select UI
        options = [
            discord.SelectOption(label="View items", value="view", description="Show playlist items"),
            discord.SelectOption(label="Play playlist", value="play", description="Enqueue this playlist in the guild"),
            discord.SelectOption(label="Import to my account", value="import", description="Create a copy in your playlists"),
            discord.SelectOption(label="Add current to this playlist", value="add_to_this", description="Add the currently playing track (owner only)"),
            discord.SelectOption(label="Add current to my Saved", value="add_to_mine", description="Save current track to your Saved playlist"),
            discord.SelectOption(label="Edit playlist", value="edit", description="Rename or change visibility (owner only)"),
            discord.SelectOption(label="Remove item by index", value="remove", description="Remove an item from this playlist (owner only)"),
            discord.SelectOption(label="Delete playlist", value="delete", description="Delete this playlist (owner only)"),
            discord.SelectOption(label="Publish to channel (anyone can import)", value="publish", description="Post playlist publicly so others can import it"),
        ]
        # (additional import categories removed)

        class _ActionSelect(ui.Select):
            def __init__(self):
                super().__init__(placeholder="Choose action…", min_values=1, max_values=1, options=options)

            async def callback(self, interaction: discord.Interaction):
                view = getattr(self, 'view', None)
                if view is None:
                    await interaction.response.send_message("Internal error: view not available.", ephemeral=True)
                    return
                act = self.values[0]
                p = view.playlist
                bot = view.bot
                # map actions
                if act == "view":
                    try:
                        meta = await playlists.view_playlist(p.get('owner_id'), p.get('name'))
                        if not meta:
                            await interaction.response.send_message("Playlist not found or not visible.", ephemeral=True)
                            return
                        lines = [f"Playlist: {meta['name']} (owner: {meta['owner_id']}, visibility: {meta['visibility']})"]
                        if not meta.get('items'):
                            lines.append("(empty)")
                        else:
                            for it in meta.get('items', []):
                                lines.append(f"{it['position']}. {it['title']}")
                        await interaction.response.send_message("\n".join(lines), ephemeral=True)
                    except Exception as e:
                        await interaction.response.send_message(f"Failed to view: {e}", ephemeral=True)
                elif act == "play":
                    # enqueue in guild player
                    try:
                        meta = await playlists.view_playlist(str(interaction.user.id), p.get('name'))
                        if not meta:
                            meta = await playlists.view_playlist(p.get('owner_id'), p.get('name'))
                    except Exception as e:
                        await interaction.response.send_message(f"Failed to load playlist: {e}", ephemeral=True)
                        return
                    if not meta or not meta.get('items'):
                        await interaction.response.send_message("Playlist empty or unavailable.", ephemeral=True)
                        return
                    player = bot.get_player(interaction.guild)
                    if (not player.voice_client or not player.voice_client.is_connected()) and interaction.user.voice and interaction.user.voice.channel:
                        try:
                            player.voice_client = await interaction.user.voice.channel.connect()
                        except Exception:
                            pass
                    count = 0
                    for it in meta.get('items', []):
                        tr = Track(title=it.get('title') or 'Unknown', source_url=it.get('source_url'), webpage_url=it.get('webpage_url'), duration=it.get('duration'), is_live=bool(it.get('is_live', False)))
                        try:
                            await player.enqueue(tr)
                            count += 1
                        except Exception:
                            continue
                    await interaction.response.send_message(f"Enqueued {count} items from playlist '{meta.get('name')}'.", ephemeral=True)
                elif act == "import":
                    try:
                        meta = await playlists.view_playlist(p.get('owner_id'), p.get('name'))
                    except Exception as e:
                        await interaction.response.send_message(f"Failed to fetch playlist: {e}", ephemeral=True)
                        return
                    if not meta:
                        await interaction.response.send_message("Failed to fetch playlist.", ephemeral=True)
                        return
                    owner = str(interaction.user.id)
                    newname = f"Imported {meta.get('name')}"
                    try:
                        await playlists.create_playlist(owner, newname)
                    except Exception:
                        pass
                    count = 0
                    for it in meta.get('items', []):
                        try:
                            await playlists.add_item(owner, newname, it.get('title') or 'Unknown', it.get('webpage_url'), it.get('source_url'), it.get('duration'), bool(it.get('is_live', False)))
                            count += 1
                        except Exception:
                            continue
                    await interaction.response.send_message(f"Imported {count} items into playlist '{newname}'.", ephemeral=True)
                elif act == "add_to_this":
                    if str(interaction.user.id) != str(p.get('owner_id')):
                        await interaction.response.send_message("Only the owner can add directly to this playlist.", ephemeral=True)
                        return
                    try:
                        player = bot.get_player(interaction.guild)
                    except Exception as e:
                        await interaction.response.send_message(f"Failed to access player: {e}", ephemeral=True)
                        return
                    track = player.current or player.last_played
                    if not track:
                        await interaction.response.send_message("No current track to add.", ephemeral=True)
                        return
                    ok = await playlists.add_item(str(interaction.user.id), p.get('name'), getattr(track, 'title', 'Unknown'), getattr(track, 'webpage_url', None), getattr(track, 'source_url', None), getattr(track, 'duration', None), bool(getattr(track, 'is_live', False)))
                    if ok:
                        await interaction.response.send_message(f"Added '{track.title}' to '{p.get('name')}'.", ephemeral=True)
                    else:
                        await interaction.response.send_message("Failed to add to playlist.", ephemeral=True)
                elif act == "add_to_mine":
                    try:
                        player = bot.get_player(interaction.guild)
                    except Exception as e:
                        await interaction.response.send_message(f"Failed to access player: {e}", ephemeral=True)
                        return
                    track = player.current or player.last_played
                    if not track:
                        await interaction.response.send_message("No current track to add.", ephemeral=True)
                        return
                    owner = str(interaction.user.id)
                    pname = "Saved"
                    try:
                        await playlists.create_playlist(owner, pname)
                    except Exception:
                        pass
                    ok = await playlists.add_item(owner, pname, getattr(track, 'title', 'Unknown'), getattr(track, 'webpage_url', None), getattr(track, 'source_url', None), getattr(track, 'duration', None), bool(getattr(track, 'is_live', False)))
                    if ok:
                        await interaction.response.send_message(f"Saved '{track.title}' to your playlist '{pname}'.", ephemeral=True)
                    else:
                        await interaction.response.send_message("Failed to save to playlist.", ephemeral=True)
                elif act == "edit":
                    if str(interaction.user.id) != str(p.get('owner_id')):
                        await interaction.response.send_message("Only the owner can edit.", ephemeral=True)
                        return
                    try:
                        await interaction.response.send_modal(EditPlaylistModal(p.get('name')))
                    except Exception as e:
                        await interaction.response.send_message(f"Failed to open edit modal: {e}", ephemeral=True)
                elif act == "remove":
                    if str(interaction.user.id) != str(p.get('owner_id')):
                        await interaction.response.send_message("Only the owner can remove items.", ephemeral=True)
                        return
                    try:
                        await interaction.response.send_modal(RemoveItemModal(p.get('name')))
                    except Exception as e:
                        await interaction.response.send_message(f"Failed to open remove modal: {e}", ephemeral=True)
                elif act == "delete":
                    if str(interaction.user.id) != str(p.get('owner_id')):
                        await interaction.response.send_message("Only the owner can delete.", ephemeral=True)
                        return
                    confirm = ConfirmDeleteView(p.get('name'))
                    await interaction.response.send_message(f"Confirm delete playlist '{p.get('name')}'?", ephemeral=True, view=confirm)
                elif act == "publish":
                    if str(interaction.user.id) != str(p.get('owner_id')):
                        await interaction.response.send_message("Only the owner can publish playlists.", ephemeral=True)
                        return
                    try:
                        meta = await playlists.view_playlist(p.get('owner_id'), p.get('name'))
                    except Exception as e:
                        await interaction.response.send_message(f"Failed to load playlist: {e}", ephemeral=True)
                        return
                    if not meta:
                        await interaction.response.send_message("Playlist not available to publish.", ephemeral=True)
                        return
                    # build public embed
                    emb = discord.Embed(title=f"Shared playlist: {meta.get('name')}", description=f"Shared by <@{meta.get('owner_id')}> - import to your playlists")
                    if meta.get('items'):
                        lines = []
                        for it in meta.get('items', [])[:10]:
                            lines.append(f"{it.get('position')}. {it.get('title')}")
                        emb.add_field(name="Top items", value="\n".join(lines), inline=False)
                    public_view = PublicShareView(meta)
                    # send public message to channel
                    try:
                        await interaction.response.defer()
                        ch = interaction.channel
                        if ch:
                            await ch.send(embed=emb, view=public_view)
                            await interaction.followup.send("Published playlist to channel.", ephemeral=True)
                        else:
                            await interaction.followup.send("Could not find channel to publish in.", ephemeral=True)
                    except Exception as e:
                        try:
                            await interaction.response.send_message(f"Failed to publish: {e}", ephemeral=True)
                        except Exception:
                            pass
                else:
                    await interaction.response.send_message("Unknown action.", ephemeral=True)

        self.add_item(_ActionSelect())

    async def on_timeout(self):
        # Intentionally no-op: keep this select view open until the user closes it.
        return


class EditPlaylistModal(ui.Modal, title="Edit Playlist"):
    def __init__(self, current_name: str):
        super().__init__()
        self.current_name = current_name
        self.name_input = ui.TextInput(label="New name (leave same to keep)", default=current_name, required=False)
        self.add_item(self.name_input)

    async def on_submit(self, interaction: discord.Interaction):
        newname = self.name_input.value.strip() or None
        owner = str(interaction.user.id)
        try:
            ok = await playlists.edit_playlist(owner, self.current_name, new_name=newname)
        except Exception as e:
            await interaction.response.send_message(f"Edit failed: {e}", ephemeral=True)
            return
        if ok:
            await interaction.response.send_message(f"Updated playlist '{self.current_name}'.", ephemeral=True)
        else:
            await interaction.response.send_message("Edit failed. Are you the owner?", ephemeral=True)


class CreatePlaylistModal(ui.Modal, title="Create Playlist"):
    def __init__(self):
        super().__init__()
        self.name = ui.TextInput(label="Playlist name", placeholder="My playlist", required=True)
        self.vis = ui.TextInput(label="Visibility (public|private)", default="private", required=False)
        self.add_item(self.name)
        self.add_item(self.vis)

    async def on_submit(self, interaction: discord.Interaction):
        pname = self.name.value.strip()
        vis = self.vis.value.strip().lower() or "private"
        if vis not in ("public", "private"):
            await interaction.response.send_message("Visibility must be 'public' or 'private'.", ephemeral=True)
            return
        owner = str(interaction.user.id)
        try:
            await playlists.create_playlist(owner, pname, visibility=vis)
            await interaction.response.send_message(f"Created playlist '{pname}'.", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"Failed to create playlist: {e}", ephemeral=True)


class RemoveItemModal(ui.Modal, title="Remove Item from Playlist"):
    def __init__(self, playlist_name: str):
        super().__init__()
        self.playlist_name = playlist_name
        self.index = ui.TextInput(label="Item index to remove", placeholder="1", required=True)
        self.add_item(self.index)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            idx = int(self.index.value.strip())
        except Exception:
            await interaction.response.send_message("Index must be a number.", ephemeral=True)
            return
        owner = str(interaction.user.id)
        try:
            ok = await playlists.remove_item(owner, self.playlist_name, idx)
        except Exception as e:
            await interaction.response.send_message(f"Remove failed: {e}", ephemeral=True)
            return
        if ok:
            await interaction.response.send_message(f"Removed item {idx} from '{self.playlist_name}'.", ephemeral=True)
        else:
            await interaction.response.send_message("Remove failed. Is the index correct and are you the owner?", ephemeral=True)


class ConfirmDeleteView(ui.View):
    def __init__(self, playlist_name: str, timeout: Optional[float] = None):
        super().__init__(timeout=timeout)
        self.playlist_name = playlist_name

    @ui.button(label="Confirm Delete", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: ui.Button):
        ok = await playlists.delete_playlist(str(interaction.user.id), self.playlist_name)
        if ok:
            await interaction.response.send_message(f"Deleted playlist '{self.playlist_name}'.", ephemeral=True)
        else:
            await interaction.response.send_message("Delete failed. Are you the owner?", ephemeral=True)

    @ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_message("Cancelled.", ephemeral=True)


class SearchModal(ui.Modal, title="Search and Play"):
    def __init__(self, bot):
        super().__init__()
        self.bot = bot
        self.query = ui.TextInput(label="Search or URL", placeholder="song name or URL", required=True)
        self.add_item(self.query)

    async def on_submit(self, interaction: discord.Interaction):
        q = self.query.value.strip()
        # require user in voice channel
        if not interaction.user.voice or not interaction.user.voice.channel:
            await interaction.response.send_message("You must be in a voice channel to play.", ephemeral=True)
            return
        player = self.bot.get_player(interaction.guild)
        channel = interaction.user.voice.channel
        if not player.voice_client or not player.voice_client.is_connected():
            try:
                player.voice_client = await channel.connect()
            except Exception as e:
                await interaction.response.send_message(f"Failed to join voice channel: {e}", ephemeral=True)
                return

        # attempt to resolve via yt_dlp_get_url
        try:
            from player import yt_dlp_get_url, Track
            stream_url, title, webpage_url, is_live, duration = await yt_dlp_get_url(q)
        except Exception as e:
            await interaction.response.send_message(f"Search failed: {e}", ephemeral=True)
            return

        track = Track(title=title or q, source_url=stream_url, webpage_url=webpage_url, is_live=bool(is_live), duration=duration)
        try:
            await player.enqueue(track)
            await interaction.response.send_message(f"Enqueued: {track.title}", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"Failed to enqueue: {e}", ephemeral=True)

