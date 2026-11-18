"""T2: Reintentos con espera creciente ante errores transitorios."""

from __future__ import annotations

import re

import pytest

pytestmark = pytest.mark.hito1

_RETRY = re.compile(r"\b(retry|retries|reintent|backoff|429|sleep)\b", re.I)


def test_hay_politica_de_reintentos(extract_source: str):
    """429, 5xx y timeouts deben reintentarse; 404 no."""
    assert _RETRY.search(extract_source), (
        "En src/extract.py no se observa una política de reintentos "
        "(palabras esperables: retry/reintento, backoff, sleep, 429). "
        "Ante 429/5xx/timeout hace falta espera creciente y un máximo de intentos; "
        "un 404 no se reintenta."
    )


def test_404_no_se_reintenta_de_forma_explicita(extract_source: str):
    """El enunciado distingue 404 (definitivo) de errores transitorios."""
    if "404" in extract_source:
        return
    conjunto = re.search(r"RETRY_STATUS\s*=\s*\{([^}]+)\}", extract_source)
    transitorios = conjunto.group(1) if conjunto else ""
    reconoce_transitorios = any(codigo in transitorios for codigo in ("429", "500", "502", "503"))
    assert conjunto and reconoce_transitorios and "404" not in transitorios, (
        "No aparece el caso HTTP 404 en el extract, ni un conjunto de reintento "
        "(429/5xx) que lo deje fuera. Un 404 es definitivo: no se reintenta."
    )
