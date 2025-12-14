from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .domain import SupportedLanguage


@dataclass(frozen=True)
class HelpContainer:
    en: str
    ru: str

    def get_help_message(self, locale: str) -> str:
        lang = SupportedLanguage.RU.value if locale.startswith("ru") else SupportedLanguage.EN.value
        return self.ru if lang == SupportedLanguage.RU.value else self.en

    def get_start_message(self, username_escaped: str, locale: str, greeting: str) -> str:
        return f"{greeting}, <b>{username_escaped}</b>!\n\n{self.get_help_message(locale)}"


def render_help_messages(
    *,
    rust_help_dir: Path,
    context: dict[str, Any],
) -> HelpContainer:
    en_template = (rust_help_dir / "en.html").read_text(encoding="utf-8")
    ru_template = (rust_help_dir / "ru.html").read_text(encoding="utf-8")
    return HelpContainer(
        en=en_template.format_map(context),
        ru=ru_template.format_map(context),
    )


def load_privacy_policy(*, rust_privacy_dir: Path) -> dict[str, str]:
    return {
        "en": (rust_privacy_dir / "en.html").read_text(encoding="utf-8"),
        "ru": (rust_privacy_dir / "ru.html").read_text(encoding="utf-8"),
    }

