# file: folder_watcher.py
from __future__ import annotations
import os
import time
from pathlib import Path
from typing import List

import structlog
import orjson
import typer
from watchdog.observers import Observer
from watchdog.events import FileSystemEvent, FileSystemEventHandler

app = typer.Typer(add_completion=False)

def _json_dumps(obj) -> str:
    # orjson returns bytes; ensure str
    return orjson.dumps(obj, option=orjson.OPT_INDENT_2).decode()

def configure_logger(log_file: Path | None) -> None:
    # Stream to stdout + optional file (Splunk can tail either)
    processors = [
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.dict_tracebacks,
        structlog.processors.JSONRenderer(serializer=_json_dumps),
    ]
    structlog.configure(processors=processors)
    global log
    log = structlog.get_logger("folder_watcher")

    # If a file was requested, wrap logger to tee to file.
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        def tee_write(event_dict):
            with log_file.open("a", encoding="utf-8") as fh:
                fh.write(_json_dumps(event_dict) + "\n")
            return event_dict
        structlog.configure(processors=[*processors[:-1], tee_write, processors[-1]])

class JsonHandler(FileSystemEventHandler):
    def __init__(self, directory: Path):
        self.directory = directory

    def _snapshot(self) -> List[str]:
        try:
            return sorted(os.listdir(self.directory))
        except OSError as e:
            log.error("snapshot_error", directory=str(self.directory), error=str(e))
            return []

    def on_any_event(self, event: FileSystemEvent) -> None:
        # ignore directory events
        if event.is_directory:
            return

        kind = event.event_type.upper()  # created|deleted|modified|moved
        payload = {
            "ts": time.time(),
            "event": kind,
            "directory": str(self.directory),
            "current_files": self._snapshot(),
        }

        # add paths; moved events have dest_path
        src = getattr(event, "src_path", None)
        dst = getattr(event, "dest_path", None)
        if src:
            payload["file"] = os.path.basename(src)
            payload["path"] = src
        if dst:
            payload["file_to"] = os.path.basename(dst)
            payload["path_to"] = dst

        log.info("fs_event", **payload)

@app.command()
def watch(
    directory: Path = typer.Argument(Path("/tmp"), exists=True, file_okay=False, dir_okay=True),
    log_file: Path | None = typer.Option(
        "/var/log/folder_watcher.log",
        "--log-file", "-o", help="Write JSON lines to this file (and stdout).",
    ),
    recursive: bool = typer.Option(False, help="Watch recursively."),
    poll: bool = typer.Option(False, help="Use polling observer (e.g., on network filesystems)."),
):
    """Watch a directory and emit one JSON object per event (Splunk-friendly)."""
    configure_logger(Path(log_file) if log_file else None)

    # initial snapshot
    log.info("startup", ts=time.time(), event="STARTUP",
             directory=str(directory), current_files=sorted(os.listdir(directory)))

    # choose observer
    observer = (Observer if not poll else __import__("watchdog.observers.polling").observers.polling.PollingObserver)()
    handler = JsonHandler(directory)
    observer.schedule(handler, str(directory), recursive=recursive)
    observer.start()
    log.info("status", ts=time.time(), status="monitoring_started", directory=str(directory))

    try:
        while observer.is_alive():
            observer.join(1.0)
    except KeyboardInterrupt:
        log.info("status", ts=time.time(), status="stopping")
        observer.stop()
    observer.join()
    log.info("status", ts=time.time(), status="stopped")

if __name__ == "__main__":
    app()
