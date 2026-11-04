"""T2 · Idempotencia del extract (sin --force no se re-descarga)."""

from __future__ import annotations

import re

import pytest

pytestmark = pytest.mark.hito1

_FORCE = re.compile(r"--force|force\s*=|if\s+.*force", re.I)
_SKIP_EXISTING = re.compile(
    r"exists\(\)|is_file\(\)|already|idempot|skip.*download|no vuelve",
    re.I,
)


def test_existe_mecanismo_force_o_equivalente(extract_source: str):
    """Debe existir una vía explícita para forzar la re-descarga."""
    assert _FORCE.search(extract_source), (
        "No se encuentra un flag o parámetro --force (o equivalente) en el extract. "
        "La idempotencia exige: por defecto no repetir descargas correctas; "
        "con --force, sí."
    )


def test_se_omite_descarga_si_el_artefacto_ya_es_valido(extract_source: str):
    """Relanzar el pipeline no debe volver a pedir lo ya correcto."""
    assert _SKIP_EXISTING.search(extract_source), (
        "No se ve lógica que omita la descarga cuando el fichero en data/raw/ "
        "ya es válido. Sin eso, cada ejecución vuelve a golpear la API "
        "y el Hito 1 no puede darse por cumplido en idempotencia."
    )
