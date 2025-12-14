from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from html import escape
from typing import Optional


def escape_html(text: str) -> str:
    return escape(text, quote=False)


class SupportedLanguage(str, Enum):
    EN = "en"
    RU = "ru"


def normalize_locale(raw: Optional[str]) -> str:
    if not raw:
        return SupportedLanguage.EN.value
    raw = raw.lower()
    if raw.startswith("ru"):
        return SupportedLanguage.RU.value
    if raw.startswith("en"):
        return SupportedLanguage.EN.value
    return SupportedLanguage.EN.value


@dataclass(frozen=True)
class UserRow:
    uid: int
    name: str
    created_at: datetime


@dataclass(frozen=True)
class DickRow:
    length: int
    owner_uid: int
    owner_name: str
    grown_at: datetime
    position: Optional[int]


@dataclass(frozen=True)
class GrowthResult:
    new_length: int
    pos_in_top: Optional[int]
