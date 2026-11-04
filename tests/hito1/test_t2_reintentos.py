"""T2 · Reintentos con espera creciente ante errores transitorios."""

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
    lower = extract_source.lower()
    menciona_404 = "404" in lower
    # Señal mínima: el código contempla 404 como caso aparte.
    assert menciona_404, (
        "No aparece el caso HTTP 404 en el extract. "
        "Conviene tratarlo como error definitivo (sin reintento), "
        "distinto de 429/5xx."
    )
