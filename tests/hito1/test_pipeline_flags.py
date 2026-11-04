"""Hito 1 · La segunda ejecución del pipeline no vuelve a descargar."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.hito1


def test_pipeline_expone_force(src_texts):
    """--force (en pipeline o en extract) es la única vía para repetir una descarga buena."""
    texto = src_texts.get("pipeline.py", "") + "\n" + src_texts.get("extract.py", "")
    assert "--force" in texto, (
        "Ni pipeline.py ni extract.py declaran --force. "
        "La segunda ejecución de `python -m src.pipeline`, sin ese flag, "
        "tiene que reutilizar lo ya descargado bien."
    )


def test_skip_extract_no_sustituye_la_idempotencia(extract_source: str):
    """--skip-extract sigue siendo de depuración; el extract en sí es idempotente."""
    assert "skip-extract" not in extract_source or "--force" in extract_source
    assert "--force" in extract_source or "force" in extract_source, (
        "La idempotencia no puede depender de que alguien recuerde --skip-extract. "
        "El propio extract omite lo que ya está bien, salvo --force."
    )
