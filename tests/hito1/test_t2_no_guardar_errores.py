"""T2: Solo HTTP 200 se materializa en data/raw/ como dato."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.hito1


def test_respuesta_no_200_no_escribe_raw(repo_root: Path, monkeypatch, tmp_path):
    """Una respuesta 500 no debe acabar en raw/ como si fuera JSON válido."""
    raiz = str(repo_root)
    sys.path[:] = [p for p in sys.path if p != raiz]
    sys.path.insert(0, raiz)
    for nombre in list(sys.modules):
        if nombre == "src" or nombre.startswith("src."):
            del sys.modules[nombre]
    try:
        # Importación diferida: el alumno puede reorganizar módulos.
        extract = importlib.import_module("src.extract")
        paths = importlib.import_module("src.paths")
    except Exception as exc:  # noqa: BLE001: mensaje pedagógico
        pytest.fail(f"No se pudo importar src.extract/src.paths: {exc}")

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    monkeypatch.setattr(paths, "RAW", raw_dir, raising=False)
    if hasattr(extract, "RAW"):
        monkeypatch.setattr(extract, "RAW", raw_dir, raising=False)

    fake = MagicMock()
    fake.status_code = 500
    fake.content = b'{"message":"error interno de prueba"}'
    fake.raise_for_status.side_effect = RuntimeError("HTTP 500")

    if hasattr(extract, "_download"):
        monkeypatch.setattr(extract.requests, "get", lambda *a, **k: fake)
        dest = raw_dir / "probe_error.json"
        status, _nbytes = extract._download("https://example.invalid/probe", dest)
        assert status == 500
        assert not dest.exists(), (
            "El extract ha escrito en data/raw/ una respuesta con status 500. "
            "Lo que no sea 200 se anota en el manifiesto y no se guarda como dato."
        )
        return

    if hasattr(extract, "HttpClient"):
        http_cls = getattr(extract, "HttpConfig", None)
        if http_cls is None:
            config = importlib.import_module("src.config")
            http_cls = config.HttpConfig
        cliente = extract.HttpClient(
            http_cls(max_retries=0, max_requests_per_second=1000, timeout_seconds=1)
        )
        monkeypatch.setattr(cliente.session, "get", lambda *a, **k: fake)
        status, content, _intentos, _nota = cliente.get("https://example.invalid/probe")
        assert status == 500
        assert content is None, (
            "Una respuesta 500 no puede devolverse como cuerpo de dato. "
            "Solo HTTP 200 se materializa en data/raw/."
        )
        assert list(raw_dir.iterdir()) == []
        return

    pytest.fail(
        "No se encuentra _download ni HttpClient en src.extract. "
        "Hace falta una descarga que distinga éxito de error HTTP."
    )
