"""Fixtures compartidas: Hito 1, Hito 2 y ejecución sobre otro clon.

El repo objetivo es, por este orden:

1. ``--repo RUTA``
2. variable de entorno ``DSBD_REPO``
3. el propio repositorio donde viven estos tests

Si la ruta es un clon cuyo ETL está un nivel por debajo (un único
directorio con ``sources.yaml`` y ``src/``), se usa ese directorio.
Así la batería corre sobre el código de un equipo sin copiar ``tests/``
dentro de su repositorio.
"""

from __future__ import annotations

import ast
import os
from pathlib import Path

import pytest
import yaml


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "hito1: comprobaciones del Hito 1 (04/11) de la Entrega 2",
    )
    config.addinivalue_line(
        "markers",
        "hito2: comprobaciones del Hito 2 (11/11) de la Entrega 2",
    )
    config.addinivalue_line(
        "markers",
        "runtime: ejecuta el código actual del ETL (práctica 1) sin red",
    )


def _es_raiz_etl(path: Path) -> bool:
    return (path / "sources.yaml").is_file() and (path / "src").is_dir()


def _resolver_raiz(raw: Path) -> Path:
    raw = raw.resolve()
    if _es_raiz_etl(raw):
        return raw
    if raw.is_dir():
        anidados = sorted(
            p for p in raw.iterdir() if p.is_dir() and _es_raiz_etl(p)
        )
        if len(anidados) == 1:
            return anidados[0]
    pytest.fail(
        f"No hay un ETL en {raw} (hace falta sources.yaml y src/). "
        "Si el código está en un subdirectorio, dejad solo uno o pasad esa ruta."
    )


def _raiz_por_defecto() -> Path:
    here = Path(__file__).resolve()
    for candidate in here.parents:
        if _es_raiz_etl(candidate):
            return candidate
    pytest.fail(
        "No se encuentra la raíz del ETL (sources.yaml + src/). "
        "Colocad tests/ dentro del repositorio o pasad --repo."
    )


@pytest.fixture(scope="session")
def repo_root(request) -> Path:
    """Raíz del proyecto ETL que se está comprobando."""
    explicito = request.config.getoption("--repo") or os.environ.get("DSBD_REPO")
    if explicito:
        return _resolver_raiz(Path(explicito))
    return _raiz_por_defecto()


@pytest.fixture(scope="session")
def git_root(repo_root: Path) -> Path:
    """Raíz Git del clon (puede coincidir con repo_root o ser un padre)."""
    for candidate in [repo_root, *repo_root.parents]:
        if (candidate / ".git").exists():
            return candidate
    return repo_root


@pytest.fixture(scope="session")
def sources_yaml(repo_root: Path) -> dict:
    path = repo_root / "sources.yaml"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(data, dict), "sources.yaml debe deserializar a un mapeo"
    return data


@pytest.fixture(scope="session")
def extract_path(repo_root: Path) -> Path:
    path = repo_root / "src" / "extract.py"
    if not path.is_file():
        pytest.fail("Falta src/extract.py; sin extract no hay Hito 1.")
    return path


@pytest.fixture(scope="session")
def extract_source(extract_path: Path) -> str:
    return extract_path.read_text(encoding="utf-8")


@pytest.fixture(scope="session")
def extract_tree(extract_source: str) -> ast.AST:
    return ast.parse(extract_source)


@pytest.fixture(scope="session")
def src_texts(repo_root: Path) -> dict[str, str]:
    """Texto de cada módulo de src/ (cadena vacía si el fichero no está)."""
    textos: dict[str, str] = {}
    src = repo_root / "src"
    if not src.is_dir():
        pytest.fail("Falta el paquete src/.")
    for path in sorted(src.glob("*.py")):
        textos[path.name] = path.read_text(encoding="utf-8")
    return textos
