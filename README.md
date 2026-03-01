# DMusicBot

A lightweight Discord music bot for playing audio in voice channels.

**Contents**
- **Features:** queue, play from URL/search, pause/resume, skip, stop, now playing, volume
- **Files:** core bot logic in [bot.py](bot.py) and audio logic in [player.py](player.py)

## Requirements
- Python 3.10+
- See [requirements.txt](requirements.txt) for exact dependencies

## Installation
1. Clone the repository.
2. Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

## Configuration
- The bot reads the Discord token from the `token` file in the repo root. Alternatively set the environment variable `DISCORD_TOKEN`.

Create a `token` file containing only your bot token or run:

```powershell
$env:DISCORD_TOKEN = 'YOUR_TOKEN_HERE'
```

## Running
- Start the bot with:

```powershell
python start.py
```

If needed you can run `bot.py` directly:

```powershell
python bot.py
```

## Common Commands
Usage depends on your server prefix (common examples below assume `!`). Typical commands implemented by this bot:

- `play <url or search>` : Add track to queue / start playback
- `pause` : Pause current track
- `resume` : Resume playback
- `skip` : Skip current track
- `stop` : Stop playback and clear queue
- `queue` : Show queued tracks
- `nowplaying` : Show current track info
- `join` / `leave` : Bot joins or leaves voice channel
- `volume <0-100>` : Set playback volume

Check the command implementation in [bot.py](bot.py) for exact names and behaviour.

## Development
- Code lives in `bot.py`, `player.py`, and `worker.py`.
- Tests: see `test_extract.py` for example tests.

## Troubleshooting
- If audio doesn't play, check that FFmpeg/avconv is installed and available in PATH.
- Ensure the bot has `CONNECT` and `SPEAK` permissions in Discord voice channels.

## Contributing
PRs and issues welcome. Keep changes focused and include tests where appropriate.

## License
MIT — see LICENSE if included, otherwise consider this project permissively licensed.
