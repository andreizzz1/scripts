from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from dickgrowerbot.i18n import I18n
from dickgrowerbot.utils import time_till_next_day


def test_time_till_next_day_formats() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    i18n = I18n.from_locales_dir(repo_root / "locales", fallback_locale="en")
    now = datetime(2023, 10, 21, 22, 10, 57, tzinfo=timezone.utc)
    # should end with "...<b>1</b>h <b>49</b>m." like the Rust test
    s = time_till_next_day(i18n, "en", now=now)
    assert s.endswith("<b>1</b>h <b>49</b>m.")
