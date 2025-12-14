from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from aiogram.types import User

from .domain import escape_html
from .i18n import I18n


def get_full_name(user: User) -> str:
    if user.last_name:
        return f"{user.first_name} {user.last_name}"
    return user.first_name


def time_till_next_day(i18n: I18n, locale: str, now: Optional[datetime] = None) -> str:
    if now is None:
        now = datetime.now(tz=timezone.utc)
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    delta = tomorrow - now
    hrs = int(delta.total_seconds() // 3600)
    mins = int((delta.total_seconds() - hrs * 3600) // 60)
    return i18n.t("titles.time_till_next_day.some", locale, hours=hrs, minutes=mins)


@dataclass(frozen=True)
class HtmlUser:
    uid: int
    name_escaped: str

    @classmethod
    def from_aiogram(cls, user: User) -> "HtmlUser":
        return cls(uid=user.id, name_escaped=escape_html(get_full_name(user)))
