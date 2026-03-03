@echo off
cd /d %~dp0
REM Activate virtualenv if present, otherwise create it then activate
if exist ".venv\Scripts\activate.bat" (
  call ".venv\Scripts\activate.bat"
) else (
  python -m venv .venv
  call ".venv\Scripts\activate.bat"
)
REM Run the project's start script which handles setup and launching the bot
python start.py
pause
