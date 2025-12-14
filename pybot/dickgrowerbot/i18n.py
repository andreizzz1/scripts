from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

_PLACEHOLDER_RE = re.compile(r"%\{([a-zA-Z_][a-zA-Z0-9_]*)\}")


def _deep_get(d: dict[str, Any], key: str) -> Any:
    cur: Any = d
    for part in key.split("."):
        if not isinstance(cur, dict) or part not in cur:
            raise KeyError(key)
        cur = cur[part]
    return cur


def _render_template(template: str, params: dict[str, Any]) -> str:
    def repl(match: re.Match[str]) -> str:
        name = match.group(1)
        return str(params.get(name, match.group(0)))

    return _PLACEHOLDER_RE.sub(repl, template)


@dataclass(frozen=True)
class I18n:
    messages: dict[str, dict[str, Any]]
    fallback_locale: str = "en"

    @classmethod
    def from_locales_dir(cls, locales_dir: Path, fallback_locale: str = "en") -> "I18n":
        messages: dict[str, dict[str, Any]] = {}
        for path in locales_dir.glob("*.yml"):
            locale = path.stem
            messages[locale] = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        return cls(messages=messages, fallback_locale=fallback_locale)

    def t(self, key: str, locale: str, **params: Any) -> str:
        locale_dict = self.messages.get(locale) or self.messages.get(self.fallback_locale) or {}
        value = _deep_get(locale_dict, key)
        if not isinstance(value, str):
            raise TypeError(f"i18n key {key!r} is not a string")
        return _render_template(value, params)
