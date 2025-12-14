from __future__ import annotations

import os
import secrets
from dataclasses import dataclass, field
from typing import Protocol

from .repo import ChatIdKind, DicksRepo


def _env_int(key: str, default: int) -> int:
    value = os.getenv(key)
    if not value:
        return default
    return int(value)


def _env_float(key: str, default: float) -> float:
    value = os.getenv(key)
    if not value:
        return default
    return float(value)


def _env_disabled(key: str) -> bool:
    return os.getenv(key) is not None


@dataclass(frozen=True)
class ChangeIntent:
    current_length: int
    base_increment: int


class Perk(Protocol):
    name: str

    async def apply(self, uid: int, chat: ChatIdKind, intent: ChangeIntent) -> int: ...

    def enabled(self) -> bool:
        env_key = f"DISABLE_{self.name.upper().replace('-', '_')}"
        return not _env_disabled(env_key)


@dataclass
class Increment:
    base: int
    by_perks: dict[str, int] = field(default_factory=dict)
    total: int = 0

    def __post_init__(self) -> None:
        if self.total == 0:
            self.total = self.base


def _base_increment(min_value: int, max_value: int, sign_ratio: float) -> int:
    # Similar to the Rust logic: choose sign first (when range crosses zero).
    rng = secrets.SystemRandom()
    sign_ratio_percent = max(0, min(100, round(sign_ratio * 100)))
    if min_value > 0:
        return rng.randrange(min_value, max_value + 1)
    if max_value < 0:
        return rng.randrange(min_value, max_value + 1)
    positive = rng.randrange(0, 100) < sign_ratio_percent
    if positive:
        return rng.randrange(1, max_value + 1)
    return rng.randrange(min_value, 0)


class Incrementor:
    def __init__(self, dicks: DicksRepo, perks: list[Perk]) -> None:
        self._dicks = dicks
        self._perks = [p for p in perks if p.enabled()]
        self._growth_min = _env_int("GROWTH_MIN", -5)
        self._growth_max = _env_int("GROWTH_MAX", 10)
        self._grow_shrink_ratio = _env_float("GROW_SHRINK_RATIO", 0.5)
        self._newcomers_grace_days = _env_int("NEWCOMERS_GRACE_DAYS", 7)
        self._dod_bonus_max = _env_int("GROWTH_DOD_BONUS_MAX", 5)

    async def growth_increment(self, uid: int, chat: ChatIdKind, days_since_registration: int) -> Increment:
        ratio = 1.0 if days_since_registration <= self._newcomers_grace_days else self._grow_shrink_ratio
        base = _base_increment(self._growth_min, self._growth_max, ratio)
        return await self._with_perks(uid, chat, base)

    async def dod_increment(self, uid: int, chat: ChatIdKind) -> Increment:
        rng = secrets.SystemRandom()
        base = rng.randrange(1, self._dod_bonus_max + 1)
        return await self._with_perks(uid, chat, base)

    async def _with_perks(self, uid: int, chat: ChatIdKind, base: int) -> Increment:
        current_length = await self._dicks.fetch_length(uid, chat)
        intent = ChangeIntent(current_length=current_length, base_increment=base)
        additional = 0
        by_perks: dict[str, int] = {}
        for perk in self._perks:
            change = await perk.apply(uid, chat, intent)
            if change:
                by_perks[perk.name] = change
            additional += change
        total = base + additional
        return Increment(base=base, by_perks=by_perks, total=total)
