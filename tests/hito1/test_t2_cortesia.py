"""T2: Cortesía: ritmo ≤ 5 pet/s y User-Agent del equipo."""

from __future__ import annotations

import re

import pytest

pytestmark = pytest.mark.hito1


def _user_agent(extract_source: str, sources_yaml: dict) -> str:
    """Constante del extract o bloque http.user_agent de sources.yaml."""
    http = sources_yaml.get("http") or {}
    if isinstance(http, dict) and http.get("user_agent"):
        return str(http["user_agent"])
    m = re.search(r'USER_AGENT\s*=\s*["\']([^"\']+)["\']', extract_source)
    if m:
        return m.group(1)
    m = re.search(r'user_agent\s*[:=]\s*["\']([^"\']+)["\']', extract_source)
    if m:
        return m.group(1)
    return ""


def test_user_agent_identifica_al_equipo(extract_source: str, sources_yaml: dict):
    """El User-Agent debe identificar al equipo, no al starter ni a un marcador."""
    ua = _user_agent(extract_source, sources_yaml)
    assert ua, (
        "No hay User-Agent: ni constante USER_AGENT en src/extract.py "
        "ni http.user_agent en sources.yaml."
    )
    generico = "UCJC-DSBD-practica1" in ua and "educational ETL framework" in ua
    marcador = "CAMBIAD_ESTO" in ua or "cambia" in ua.lower() and "esto" in ua.lower()
    assert not generico and not marcador, (
        f"User-Agent actual: {ua!r}. "
        "Tiene que identificar a vuestro equipo "
        "(asignatura + nombre del equipo o repo), no el texto del starter "
        "ni un marcador del tipo CAMBIAD_ESTO."
    )


def test_hay_limitacion_de_ritmo(extract_source: str):
    """Nunca más de 5 peticiones por segundo contra la misma API."""
    lower = extract_source.lower()
    senales = (
        "sleep" in lower
        or "rate" in lower
        or "throttle" in lower
        or "time.sleep" in lower
        or (
            ("peticiones" in lower or "requests" in lower)
            and ("segundo" in lower or "second" in lower)
        )
    )
    assert senales, (
        "No se observa limitación de ritmo (p. ej. time.sleep, rate limit). "
        "El enunciado pide no superar 5 peticiones/segundo frente a la misma API."
    )
