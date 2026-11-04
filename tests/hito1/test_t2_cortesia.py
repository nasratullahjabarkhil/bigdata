"""T2 · Cortesía: ritmo ≤ 5 pet/s y User-Agent del equipo."""

from __future__ import annotations

import re

import pytest

pytestmark = pytest.mark.hito1


def test_user_agent_identifica_al_equipo(extract_source: str):
    """El User-Agent debe identificar al equipo, no solo al starter genérico."""
    m = re.search(r'USER_AGENT\s*=\s*["\']([^"\']+)["\']', extract_source)
    assert m, (
        "No se encuentra una constante USER_AGENT en src/extract.py. "
        "Es obligatoria para identificarse ante las APIs."
    )
    ua = m.group(1)
    generico = "UCJC-DSBD-practica1" in ua and "educational ETL framework" in ua
    assert not generico, (
        f"User-Agent actual: {ua!r}. "
        "Seguís con el identificador genérico del starter. "
        "Sustituidlo por uno que identifique a vuestro equipo "
        "(asignatura + nombre del equipo o repo)."
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
