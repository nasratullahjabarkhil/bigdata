"""T2 · Manifiesto acumulativo con run_id."""

from __future__ import annotations

import ast
import re

import pytest

pytestmark = pytest.mark.hito1


def test_manifiesto_incluye_run_id(extract_source: str):
    """Cada fila del manifiesto debe llevar run_id."""
    assert re.search(r"run_id", extract_source), (
        "El manifiesto del extract no menciona run_id. "
        "Cada ejecución debe etiquetar sus filas con un identificador de corrida "
        "para poder auditar qué se descargó en cada intento."
    )


def test_manifiesto_no_se_sobrescribe_entero(extract_tree: ast.AST, extract_source: str):
    """Una ejecución nueva añade filas; no borra el historial previo."""
    # El starter abre el manifiesto con mode "w" (destruye el historial).
    modos_escritura = []
    for node in ast.walk(extract_tree):
        if isinstance(node, ast.Call):
            # open(...) o Path.open(...)
            func = node.func
            name = ""
            if isinstance(func, ast.Name):
                name = func.id
            elif isinstance(func, ast.Attribute):
                name = func.attr
            if name != "open":
                continue
            # Buscar literal de modo
            for arg in node.args[1:2]:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    modos_escritura.append(arg.value)
            for kw in node.keywords:
                if kw.arg in {"mode", "newline"} and isinstance(
                    kw.value, ast.Constant
                ):
                    if kw.arg == "mode":
                        modos_escritura.append(str(kw.value.value))

    modos_w = [m for m in modos_escritura if m.startswith("w")]
    modos_a = [m for m in modos_escritura if m.startswith("a")]
    menciona_append = bool(re.search(r"append|concat|acumul", extract_source, re.I))

    assert modos_a or menciona_append or not modos_w, (
        "El manifiesto parece abrirse en modo escritura destructiva ('w'), "
        "como en el starter. En el Hito 1 debe ser acumulativo: "
        "la nueva ejecución añade filas y conserva las anteriores."
    )
