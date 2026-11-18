"""Importa el ``src`` del repo objetivo y lo desvía a un directorio temporal.

Sirve para ejecutar el código del equipo (o del starter) sin tocar su
``data/`` y sin red. Los módulos se recargan en cada test: si no, pytest
seguiría usando el ``src`` del primer clon importado.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import SimpleNamespace

DIRS = ("ROOT", "DATA", "RAW", "CLEAN", "SERVING", "CARDS", "MANIFEST")


def import_src(repo_root: Path) -> SimpleNamespace:
    """Carga src.config, paths, extract, transform, load, build_cards y pipeline."""
    root = str(repo_root.resolve())
    for name in list(sys.modules):
        if name == "src" or name.startswith("src."):
            del sys.modules[name]
    sys.path[:] = [p for p in sys.path if p != root]
    sys.path.insert(0, root)
    def cargar(nombre: str):
        try:
            return importlib.import_module(f"src.{nombre}")
        except ModuleNotFoundError:
            if nombre in ("config", "paths"):
                return importlib.import_module(f"src.core.{nombre}")
            raise

    nombres = (
        "config",
        "paths",
        "extract",
        "transform",
        "load",
        "build_cards",
        "pipeline",
    )
    mods = {nombre: cargar(nombre) for nombre in nombres}
    mods["pkg"] = importlib.import_module("src")
    return SimpleNamespace(**mods)


def sandbox(mods: SimpleNamespace, tmp: Path, monkeypatch) -> dict[str, Path]:
    """Apunta RAW/CLEAN/SERVING/CARDS (y los alias ya importados) a ``tmp``."""
    raw = tmp / "data" / "raw"
    clean = tmp / "data" / "clean"
    serving = tmp / "data" / "serving"
    cards = tmp / "cards"
    for directorio in (raw, clean, serving, cards):
        directorio.mkdir(parents=True, exist_ok=True)
    mapping = {
        "ROOT": tmp,
        "DATA": tmp / "data",
        "RAW": raw,
        "CLEAN": clean,
        "SERVING": serving,
        "CARDS": cards,
        "MANIFEST": raw / "manifest.csv",
    }
    for mod in mods.__dict__.values():
        for clave, destino in mapping.items():
            if hasattr(mod, clave):
                monkeypatch.setattr(mod, clave, destino, raising=False)
    db = serving / "observatorio.sqlite"
    for nombre in ("load", "build_cards"):
        mod = getattr(mods, nombre)
        if hasattr(mod, "DB_PATH"):
            monkeypatch.setattr(mod, "DB_PATH", db, raising=False)
    mapping["DB"] = db
    return mapping
