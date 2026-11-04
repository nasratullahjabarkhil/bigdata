"""T2 · Paginación completa frente a metadatos del Banco Mundial."""

from __future__ import annotations

import ast
import re

import pytest

pytestmark = pytest.mark.hito1

_PAGE_HINTS = re.compile(
    r"\b(page|pages|per_page|pagina|paginación|pagination)\b",
    re.IGNORECASE,
)


def test_extract_contempla_paginacion(extract_source: str, extract_tree: ast.AST):
    """El extract debe recorrer páginas; una sola petición por URL no basta."""
    tiene_texto = bool(_PAGE_HINTS.search(extract_source))
    # Un bucle que lea metadatos tipo pages/total es la señal mínima.
    tiene_atributo_page = any(
        isinstance(n, ast.Attribute) and n.attr in {"page", "pages", "per_page"}
        for n in ast.walk(extract_tree)
    )
    assert tiene_texto or tiene_atributo_page, (
        "En src/extract.py no aparece lógica de paginación. "
        "Las respuestas del Banco Mundial traen metadatos (page, pages, total): "
        "hay que pedir todas las páginas y comprobar que lo guardado suma el total."
    )


def test_extract_contrasta_total_de_la_api(extract_source: str):
    """Si la suma de registros no cuadra con total, el extract debe fallar."""
    lower = extract_source.lower()
    menciona_total = "total" in lower
    menciona_error_o_assert = any(
        tok in lower for tok in ("raise", "assert", "runtimeerror", "valueerror", "exit")
    )
    assert menciona_total and menciona_error_o_assert, (
        "No se ve una comprobación explícita de que los registros descargados "
        "coinciden con el campo total de la API. Sin ese contraste, la paginación "
        "puede quedar incompleta sin que el pipeline se entere."
    )
