"""Hito 1 · CONTRIBUTORS.md con identidad institucional."""

from __future__ import annotations

import re

import pytest

pytestmark = pytest.mark.hito1


def test_existe_contributors_md(git_root):
    """En la raíz del repositorio Git debe figurar CONTRIBUTORS.md."""
    path = git_root / "CONTRIBUTORS.md"
    assert path.is_file(), (
        "Falta CONTRIBUTORS.md en la raíz del repositorio. "
        "El Hito 1 pide Nombre Apellidos y email institucional UCJC "
        "de cada integrante del equipo."
    )


def test_contributors_incluye_email_ucjc(git_root):
    """Cada integrante debe aparecer con correo @...ucjc.edu."""
    path = git_root / "CONTRIBUTORS.md"
    if not path.is_file():
        pytest.skip("Sin CONTRIBUTORS.md; lo cubre el test de existencia.")
    texto = path.read_text(encoding="utf-8")
    assert re.search(r"[\w.+-]+@[\w.-]*ucjc\.edu\b", texto, re.I), (
        "CONTRIBUTORS.md no contiene un email institucional UCJC "
        "(patrón esperado: …@…ucjc.edu)."
    )
