from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class DickOfDaySelectionMode(str, Enum):
    WEIGHTS = "WEIGHTS"
    EXCLUSION = "EXCLUSION"
    RANDOM = "RANDOM"


@dataclass(frozen=True)
class BattlesFeatureToggles:
    check_acceptor_length: bool
    callback_locks: bool
    show_stats: bool
    show_stats_notice: bool


@dataclass(frozen=True)
class FeatureToggles:
    chats_merging: bool
    top_unlimited: bool
    multiple_loans: bool
    dod_selection_mode: DickOfDaySelectionMode
    pvp: BattlesFeatureToggles


@dataclass(frozen=True)
class AnnouncementsConfig:
    max_shows: int


@dataclass(frozen=True)
class AppConfig:
    features: FeatureToggles
    top_limit: int
    loan_payout_ratio: float
    dod_rich_exclusion_ratio: Optional[float]
    pvp_default_bet: int
    announcements: AnnouncementsConfig


def _env_bool(key: str, default: bool) -> bool:
    value = os.getenv(key)
    if value is None or value == "":
        return default
    return value.strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(key: str, default: int) -> int:
    value = os.getenv(key)
    if value is None or value == "":
        return default
    return int(value)


def _env_float(key: str, default: float) -> float:
    value = os.getenv(key)
    if value is None or value == "":
        return default
    return float(value)


def _env_str(key: str, default: str = "") -> str:
    value = os.getenv(key)
    if value is None:
        return default
    return value


def load_config() -> AppConfig:
    top_limit = _env_int("TOP_LIMIT", 10)
    loan_payout_ratio = _env_float("LOAN_PAYOUT_COEF", 0.0)
    dod_selection_mode = DickOfDaySelectionMode(_env_str("DOD_SELECTION_MODE", "RANDOM"))
    dod_rich_exclusion_ratio_raw = _env_float("DOD_RICH_EXCLUSION_RATIO", -1.0)
    dod_rich_exclusion_ratio = (
        dod_rich_exclusion_ratio_raw
        if 0.0 < dod_rich_exclusion_ratio_raw < 1.0
        else None
    )
    chats_merging = _env_bool("CHATS_MERGING_ENABLED", False)
    top_unlimited = _env_bool("TOP_UNLIMITED_ENABLED", False)
    multiple_loans = _env_bool("MULTIPLE_LOANS_ENABLED", False)
    pvp_default_bet = _env_int("PVP_DEFAULT_BET", 1)

    pvp = BattlesFeatureToggles(
        check_acceptor_length=_env_bool("PVP_CHECK_ACCEPTOR_LENGTH", False),
        callback_locks=_env_bool("PVP_CALLBACK_LOCKS_ENABLED", True),
        show_stats=_env_bool("PVP_STATS_SHOW", True),
        show_stats_notice=_env_bool("PVP_STATS_SHOW_NOTICE", True),
    )

    announcements = AnnouncementsConfig(max_shows=_env_int("ANNOUNCEMENT_MAX_SHOWS", 5))

    return AppConfig(
        features=FeatureToggles(
            chats_merging=chats_merging,
            top_unlimited=top_unlimited,
            multiple_loans=multiple_loans,
            dod_selection_mode=dod_selection_mode,
            pvp=pvp,
        ),
        top_limit=top_limit,
        loan_payout_ratio=loan_payout_ratio,
        dod_rich_exclusion_ratio=dod_rich_exclusion_ratio,
        pvp_default_bet=pvp_default_bet,
        announcements=announcements,
    )


def disable_cmd(key: str) -> bool:
    return os.getenv(f"DISABLE_CMD_{key.upper()}") is not None

