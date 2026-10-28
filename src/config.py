from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import yaml

from .paths import SOURCES_YAML


@dataclass
class Source:
    id: str
    name: str
    format: str = "json"
    role: str = "indicator"
    transformer: str = "generic_passthrough"
    url: str | None = None
    url_template: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    def resolve_url(self, iso2: str = "", iso3: str = "") -> str:
        if self.url_template:
            return self.url_template.format(iso2=iso2, iso3=iso3)
        if self.url:
            return self.url
        raise ValueError(f"Fuente {self.id}: falta url o url_template")


@dataclass
class FocusCountry:
    iso2: str
    iso3: str


@dataclass
class Config:
    sources: list[Source]
    focus_countries: list[FocusCountry]


def load_config(path=SOURCES_YAML) -> Config:
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    sources = [Source(**s) for s in raw.get("sources", [])]
    focus = [FocusCountry(**c) for c in raw.get("focus_countries", [])]
    return Config(sources=sources, focus_countries=focus)
