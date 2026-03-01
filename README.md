# DiscordMusicBot

A lightweight Discord music bot that uses `yt-dlp` for downloads/streaming and a separate worker process for downloads. Includes helper tooling (`start.py`) to create a virtualenv, install dependencies and launch the bot.

**Features**
- Play audio in voice channels via `!play <query>` (supports URLs and search queries)
- Queue management: `!queue`, `!skip`, `!stop`, `!pause`, `!resume`
- Autoplay seed buffer and simple autoplay toggle
- Worker process for downloads to avoid blocking the main bot

**Requirements**
- Python 3.10+ (project code uses modern typing features)
- System dependencies: `ffmpeg` (required for playback). `deno` is optional but recommended for some `yt-dlp` extraction cases.
- Python packages (see `requirements.txt`):

```
discord.py[voice]==2.6.4
PyNaCl==1.5.0
yt-dlp
pytest==7.4.0
```

**Quickstart (Local development)**

1. Clone the repo and change into the project directory.

2. Provide your bot token either by creating a file named `token` with the token on a single line, or set the `DISCORD_TOKEN` environment variable.

3. Create a virtual environment, install dependencies and run the bot using the included helper:

```bash
python start.py
```

`start.py` will:
- create and use a `.venv` virtual environment if one does not exist
- attempt to install Python dependencies from `requirements.txt`
- try to auto-install system deps like `ffmpeg`/`deno` in supported container environments (best-effort)

Alternatively, to run directly after installing requirements manually:

```bash
python -m pip install -r requirements.txt
# then either
python bot.py
# or run via the helper to use the venv: python start.py
```

**Token & environment variables**
- `DISCORD_TOKEN` — Discord bot token (preferred) or create a `token` file in the project root containing the token.
- `DMBOT_FORCE_DOWNLOAD=1` — when set, forces download-based playback instead of attempting to stream; helpful in restricted/container environments. `start.py` sets this automatically for container runs.

**Common commands (in Discord)**
- `!play <query>` — Play or queue a song (URL or search term)
- `!skip` / `!next` — Skip current track
- `!queue` — Show queue
- `!pause`, `!resume`, `!stop` — Playback controls
- `!autoplay [genre]` — Toggle or enable autoplay with optional genre
- `!help` — Sends `HELP.txt` contents if available

**Notes & troubleshooting**
- Ensure `ffmpeg` is installed and available on `PATH` — playback will fail without it.
- If `yt-dlp` extraction raises EJS/JS-related warnings, installing `deno` or Node/EJS may help; otherwise use `DMBOT_FORCE_DOWNLOAD=1`.
- On Windows, installing `ffmpeg` is typically done via a package manager (scoop/choco) or by adding the static binary to your PATH.

**Development & tests**
- Tests (if present) can be run with `pytest`:

```bash
pytest
```

**License**
This repository does not include a license file. Add one if you plan to publish the project.

---
If you'd like, I can also add a sample `dockerfile` or GitHub Actions workflow to build and run the bot in a container or CI environment.
