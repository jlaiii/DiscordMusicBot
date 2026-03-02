import logging
import os
import tempfile
from multiprocessing import Queue
from typing import Dict

import yt_dlp

logger = logging.getLogger(__name__)


class _YTDLLogger:
    def debug(self, msg):
        return

    def info(self, msg):
        return

    def warning(self, msg):
        return

    def error(self, msg):
        return


def download_to_file(url: str, out_dir: str) -> str:
    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": os.path.join(out_dir, "%(id)s.%(ext)s"),
        "quiet": True,
        "no_warnings": True,
    }
    try:
        ydl_ctx = yt_dlp.YoutubeDL(ydl_opts, logger=_YTDLLogger())
    except TypeError:
        ydl_ctx = yt_dlp.YoutubeDL(ydl_opts)
    with ydl_ctx as ydl:
        import sys, io
        old_out, old_err = sys.stdout, sys.stderr
        sys.stdout, sys.stderr = io.StringIO(), io.StringIO()
        try:
            info = ydl.extract_info(url, download=True)
        finally:
            sys.stdout, sys.stderr = old_out, old_err
        filename = ydl.prepare_filename(info)
        return filename


def run_worker(task_queue: Queue):
    os.makedirs("tmp_audio", exist_ok=True)
    while True:
        try:
            task = task_queue.get()
            if task is None:
                break
            url, task_id = task
            logger.info(f"Worker downloading {url}")
            try:
                path = download_to_file(url, "tmp_audio")
                logger.info(f"Downloaded to {path}")
            except Exception as e:
                logger.exception("Download failed")
        except Exception:
            logger.exception("Worker loop exception")
