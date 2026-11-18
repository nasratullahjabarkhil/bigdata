"""Helpers compartidos por las comprobaciones del Hito 1."""

from __future__ import annotations

import re

import yaml

FIXED_WB = ("NY.GDP.PCAP.CD", "SP.POP.TOTL")


_WDI = re.compile(r"\b([A-Z]{2}\.[A-Z0-9_.]+)\b")


def _es_fuente_worldbank(src: dict) -> bool:
    sid = str(src.get("id") or "")
    url = " ".join(
        str(src.get(clave) or "")
        for clave in ("url", "url_template")
    )
    transformer = str(src.get("transformer") or "")
    return (
        "worldbank" in sid.lower()
        or "api.worldbank.org" in url
        or transformer == "worldbank_indicator"
    )


def worldbank_indicator_ids(sources: dict) -> list[str]:
    """Códigos WDI declarados en sources.yaml (solo fuentes habilitadas).

    Acepta ``indicator_id``, listas ``indicator_ids`` / ``indicators`` y
    cualquier código WDI escrito en la URL o en los metadatos de esa fuente.
    """
    found: list[str] = []
    for src in sources.get("sources") or []:
        if not isinstance(src, dict) or not _es_fuente_worldbank(src):
            continue
        params = src.get("params") or {}
        meta = src.get("meta") or {}
        for bloque in (params, meta, src):
            if not isinstance(bloque, dict):
                continue
            uno = bloque.get("indicator_id")
            if isinstance(uno, str) and uno.strip():
                found.append(uno.strip())
            for clave in ("indicator_ids", "indicators"):
                varios = bloque.get(clave)
                if isinstance(varios, str):
                    found.extend(_WDI.findall(varios))
                elif isinstance(varios, list):
                    for item in varios:
                        if isinstance(item, str):
                            found.append(item.strip())
                        elif isinstance(item, dict) and item.get("indicator_id"):
                            found.append(str(item["indicator_id"]).strip())
        url = " ".join(str(src.get(clave) or "") for clave in ("url", "url_template"))
        found.extend(_WDI.findall(url))
        found.extend(_WDI.findall(yaml.dump(meta, allow_unicode=True)))
    vistos: list[str] = []
    for codigo in found:
        if codigo and codigo not in vistos:
            vistos.append(codigo)
    return vistos


def source_mentions(sources: dict, needle: str) -> bool:
    return needle in yaml.dump(sources, allow_unicode=True)
