from __future__ import annotations

from pathlib import Path

from dickgrowerbot.i18n import I18n


def test_i18n_renders_placeholders() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    i18n = I18n.from_locales_dir(
        repo_root / "pybot" / "dickgrowerbot" / "assets" / "locales",
        fallback_locale="en",
    )
    text = i18n.t("commands.top.line", "en", n=1, name="Bob", length=10)
    assert "1" in text
    assert "Bob" in text
    assert "10" in text
