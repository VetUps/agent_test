from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

from werkzeug.security import check_password_hash, generate_password_hash


@dataclass(frozen=True)
class DemoUser:
    username: str
    password_hash: str
    role: str
    display_name: str


def build_demo_users() -> Dict[str, DemoUser]:
    raw_users = [
        ("viewer", "viewer123", "viewer", "Гость"),
        ("analyst", "analyst123", "analyst", "Аналитик"),
        ("admin", "admin123", "admin", "Администратор"),
    ]
    return {
        username: DemoUser(
            username=username,
            password_hash=generate_password_hash(password),
            role=role,
            display_name=display_name,
        )
        for username, password, role, display_name in raw_users
    }


def authenticate(users: Dict[str, DemoUser], username: str, password: str) -> DemoUser | None:
    user = users.get(username.strip().lower())
    if not user:
        return None
    if not check_password_hash(user.password_hash, password):
        return None
    return user
