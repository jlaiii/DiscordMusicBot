import os
import platform
import shutil
import subprocess
import sys
import time


def venv_python():
    base = os.path.join(os.getcwd(), ".venv")
    if platform.system() == "Windows":
        return os.path.join(base, "Scripts", "python.exe")
    return os.path.join(base, "bin", "python")


def ensure_venv():
    vpy = venv_python()
    if os.path.exists(vpy):
        print("Virtualenv found:", vpy)
        return vpy

    print("Creating virtualenv...")
    subprocess.check_call([sys.executable, "-m", "venv", ".venv"])
    return venv_python()


def ensure_system_deps():
    """Try to install ffmpeg and deno inside Debian-based containers when missing.

    This runs automatically only if `apt-get` is available and the current
    user can run it. If installation fails, we continue and let runtime
    warnings/errors surface — this is a best-effort helper for hosted Docker
    environments like Pterodactyl where you are inside a container.
    """
    import shutil

    ffmpeg = shutil.which("ffmpeg")
    deno = shutil.which("deno")
    apt = shutil.which("apt-get")
    apk = shutil.which("apk")
    curl = shutil.which("curl")

    if ffmpeg and deno:
        print("ffmpeg and deno already installed")
        return

    # Determine package manager
    pkg_mgr = None
    if apt:
        pkg_mgr = "apt"
    elif apk:
        pkg_mgr = "apk"

    if not pkg_mgr and not curl:
        print("No supported package manager or curl found; skipping automatic system dep installation")
        return

    print(f"Attempting to install system dependencies (ffmpeg, curl, deno) using {pkg_mgr or 'script'}...")
    try:
        if pkg_mgr == "apt":
            subprocess.check_call(["apt-get", "update"]) 
            subprocess.check_call(["apt-get", "install", "-y", "ffmpeg", "curl", "unzip"])
        elif pkg_mgr == "apk":
            subprocess.check_call(["apk", "update"]) 
            subprocess.check_call(["apk", "add", "--no-cache", "ffmpeg", "curl", "unzip"])

        # install deno using official install script if curl available
        if not shutil.which("deno") and shutil.which("curl"):
            sh_path = shutil.which("bash") or shutil.which("sh")
            if not sh_path:
                print("No shell available to run deno installer; skipping deno install")
            else:
                subprocess.check_call([sh_path, "-c", "curl -fsSL https://deno.land/x/install/install.sh | sh"], env=os.environ)
                deno_path = os.path.expanduser("~/.deno/bin")
                if os.path.isdir(deno_path):
                    os.environ["PATH"] = deno_path + os.pathsep + os.environ.get("PATH", "")

        # final checks
        ffmpeg = shutil.which("ffmpeg")
        deno = shutil.which("deno")
        print("Post-install: ffmpeg=", bool(ffmpeg), "deno=", bool(deno))
        if not ffmpeg:
            print("Warning: ffmpeg still not found. Attempting to download a static ffmpeg build as a fallback...")
            try:
                # Try to download static ffmpeg for common Linux x86_64 containers
                import urllib.request, tarfile, tempfile
                import platform as _platform

                if _platform.system() == "Linux" and _platform.machine() in ("x86_64", "amd64"):
                    url = "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz"
                    tmpdir = tempfile.mkdtemp(prefix="ffmpeg_dl_")
                    archive = os.path.join(tmpdir, "ffmpeg.tar.xz")
                    print("Downloading static ffmpeg from", url)
                    urllib.request.urlretrieve(url, archive)
                    with tarfile.open(archive, "r:xz") as tf:
                        tf.extractall(path=tmpdir)
                    # find ffmpeg binary inside extracted folder
                    binpath = None
                    for root, dirs, files in os.walk(tmpdir):
                        if "ffmpeg" in files:
                            binpath = os.path.join(root, "ffmpeg")
                            break
                    if binpath:
                        local_bin = os.path.expanduser("~/.local/bin")
                        os.makedirs(local_bin, exist_ok=True)
                        dest = os.path.join(local_bin, "ffmpeg")
                        shutil.copy2(binpath, dest)
                        os.chmod(dest, 0o755)
                        os.environ["PATH"] = local_bin + os.pathsep + os.environ.get("PATH", "")
                        ffmpeg = shutil.which("ffmpeg")
                        print("Static ffmpeg installed to", dest)
                    else:
                        print("Could not locate ffmpeg binary inside downloaded archive")
                else:
                    print("Static ffmpeg download is only implemented for Linux x86_64; skipping")
            except Exception as e:
                print("Failed to download/install static ffmpeg fallback:", e)
                print("Playback may fail until ffmpeg is installed in the container.")
        if not deno:
            print("Info: deno still not found. yt-dlp may warn about EJS; consider installing deno or node+ejs.")
        print("System dependencies installation attempt finished")
    except Exception as e:
        print("Automatic system deps install failed:", e)
        print("You may need to install ffmpeg and deno manually in the container.")


def pip_install(vpy):
    print("Installing requirements via:", vpy)
    subprocess.check_call([vpy, "-m", "pip", "install", "--upgrade", "pip"])
    subprocess.check_call([vpy, "-m", "pip", "install", "-r", "requirements.txt"])


def read_or_create_token(token_path: str):
    if os.path.exists(token_path):
        print("Using token file at", token_path)
        return open(token_path, "r").read().strip()
    # fallback: read DISCORD_TOKEN env var
    tok = os.environ.get("DISCORD_TOKEN")
    if tok:
        open(token_path, "w").write(tok + "\n")
        print("Wrote DISCORD_TOKEN to token file")
        return tok
    raise RuntimeError("No token found: put token in 'token' file or set DISCORD_TOKEN env var")


def run_bot(vpy, token):
    env = os.environ.copy()
    env["DISCORD_TOKEN"] = token
    # ensure the project root is on PYTHONPATH so local modules can be imported
    os.environ["PYTHONPATH"] = os.getcwd()
    # decide how to launch: prefer module `src.bot` when `src` exists, otherwise run bot.py
    if os.path.isdir(os.path.join(os.getcwd(), "src")):
        cmd = [vpy, "-m", "src.bot"]
    elif os.path.exists(os.path.join(os.getcwd(), "bot.py")):
        cmd = [vpy, os.path.join(os.getcwd(), "bot.py")]
    else:
        # last resort: try module
        cmd = [vpy, "-m", "src.bot"]

    print("Execing bot with:", " ".join(cmd))
    # Replace this process with the bot process so the container runtime (Pterodactyl) sees it
    os.execv(cmd[0], cmd)


def main():
    token_path = os.path.join(os.getcwd(), "token")
    vpy = ensure_venv()
    # attempt to install system deps first (helps hosted containers)
    ensure_system_deps()
    pip_install(vpy)
    token = read_or_create_token(token_path)
    # Force download mode in container environments where installing a JS runtime fails
    # This prevents yt-dlp EJS extraction issues and ensures playback uses the download fallback.
    print("Enabling forced download mode (DMBOT_FORCE_DOWNLOAD=1) to ensure reliable playback in restricted containers")
    os.environ["DMBOT_FORCE_DOWNLOAD"] = "1"

    proc = run_bot(vpy, token)
    print("Bot started (PID: {}), you can inspect logs or terminate it.")


if __name__ == "__main__":
    main()
