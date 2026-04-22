from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


def _runtime_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[2]


def load_environment() -> Path:
    runtime_dir = _runtime_dir()
    env_path = runtime_dir / ".env"
    load_dotenv(env_path, override=False)
    return runtime_dir


@dataclass(frozen=True)
class Settings:
    app_title: str
    host: str
    port: int
    refresh_ms: int
    open_browser: bool
    secret_key: str
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str


def get_settings() -> Settings:
    runtime_dir = load_environment()
    app_title = os.getenv("APP_TITLE", "Аналитический агент учебных материалов")
    return Settings(
        app_title=app_title,
        host=os.getenv("APP_HOST", "127.0.0.1"),
        port=int(os.getenv("APP_PORT", "8050")),
        refresh_ms=int(os.getenv("APP_REFRESH_MS", "60000")),
        open_browser=os.getenv("APP_OPEN_BROWSER", "1") not in {"0", "false", "False"},
        secret_key=os.getenv("APP_SECRET_KEY", "chempionat-module-b-secret"),
        db_host=os.getenv("DB_HOST", "127.0.0.1"),
        db_port=int(os.getenv("DB_PORT", "3306")),
        db_name=os.getenv("DB_NAME", "module_b"),
        db_user=os.getenv("DB_USER", "root"),
        db_password=os.getenv("DB_PASSWORD", "1234"),
    )
