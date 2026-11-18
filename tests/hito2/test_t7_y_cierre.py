"""Hito 2: Piezas B y C, etiqueta entrega-02 y lo que no debe subirse."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.hito2

_EMAIL_UCJC = re.compile(r"[\w.+-]+@[\w.-]*ucjc\.edu\b", re.I)


def _leer(path: Path) -> str:
    if not path.is_file():
        pytest.fail(f"Falta {path.name} en {path.parent}.")
    return path.read_text(encoding="utf-8")


def _plano(texto: str) -> str:
    """Minúsculas y sin tildes, para no fallar por «decisión» frente a «decision»."""
    return (
        texto.lower()
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ñ", "n")
    )


def test_bugs_documenta_al_menos_tres_defectos(repo_root: Path):
    """BUGS.md: tres defectos, con reproducción, efecto y arreglo."""
    texto = _leer(repo_root / "BUGS.md")
    bloques = re.findall(r"(?m)^#{1,3} .+", texto)
    numerados = re.findall(r"(?m)^\s*(?:\d+[\.\)]\s+|[-*]\s+).+", texto)
    assert max(len(bloques), len(numerados)) >= 3, (
        "BUGS.md no separa al menos tres defectos (títulos o lista numerada). "
        "Cada uno: cómo se reproduce, qué efecto tenía y cómo se arregló."
    )
    lower = _plano(texto)
    for palabra in ("reproduc", "efecto", "arregl"):
        assert palabra in lower, (
            f"BUGS.md no contiene «{palabra}». "
            "Hace falta reproducción, efecto en los datos o en las fichas, y arreglo."
        )


def test_entrega_02_cubre_el_guion(repo_root: Path):
    """ENTREGA_02.md responde predicción, resultado, decisiones, evidencia y teoría."""
    texto = _plano(_leer(repo_root / "ENTREGA_02.md"))
    for palabra in (
        "predic",
        "resultado",
        "decision",
        "evidenc",
        "particion",
        "pais",
        "pendient",
    ):
        assert palabra in texto, (
            f"ENTREGA_02.md no menciona «{palabra}». "
            "El guion es predicción, resultado, decisiones, evidencia, "
            "preguntas de teoría y pendientes."
        )


def test_entrega_02_responde_las_tres_preguntas_de_teoria(repo_root: Path):
    """Teoría con datos propios: event log, particiones/shuffle, CSV frente a Parquet."""
    texto = _plano(_leer(repo_root / "ENTREGA_02.md"))
    assert "event" in texto or "event log" in texto, (
        "ENTREGA_02.md no habla del event log (pregunta 1)."
    )
    assert "particion" in texto, "ENTREGA_02.md no habla de particiones (pregunta 2)."
    assert "shuffle" in texto, (
        "ENTREGA_02.md no señala el operador de shuffle en el plan físico."
    )
    assert "parquet" in texto and "csv" in texto, (
        "ENTREGA_02.md no compara el tamaño en CSV y en Parquet (pregunta 3)."
    )


def test_contributors_del_cierre_siguen_siendo_institucionales(git_root: Path):
    """La pieza B repite el requisito: nombre y correo UCJC, no una cuenta personal."""
    texto = _leer(git_root / "CONTRIBUTORS.md")
    assert _EMAIL_UCJC.search(texto), (
        "CONTRIBUTORS.md no tiene un email @…ucjc.edu. "
        "En el cierre sigue identificando a cada integrante con el correo de la facultad."
    )


def test_evidencias_contiene_el_event_log(repo_root: Path):
    """evidencias/ guarda el subdirectorio del event log, no data/ ni logs/ enteros."""
    evidencias = repo_root / "evidencias"
    assert evidencias.is_dir(), (
        "Falta el directorio evidencias/. Ahí se copia el subdirectorio del "
        "event log citado en ENTREGA_02.md."
    )
    piezas = [p for p in evidencias.rglob("*") if p.is_file() and p.name != ".gitkeep"]
    assert piezas, (
        "evidencias/ está vacío. Hay que copiar el subdirectorio del event log "
        "del job (entero, no un pantallazo suelto)."
    )


def test_gitignore_deja_fuera_data_logs_y_venv(git_root: Path):
    """data/, logs/ y .venv/ se regeneran; no viajan en el repositorio."""
    texto = _leer(git_root / ".gitignore")
    for token in ("data", "logs", ".venv"):
        assert token in texto, (
            f".gitignore no menciona {token}. "
            "El contrato de entrega excluye data/, logs/ y .venv/."
        )


def test_la_etiqueta_entrega_02_esta_en_el_clon(git_root: Path):
    """La marca temporal de la entrega es el tag entrega-02, no un ZIP."""
    git_dir = git_root / ".git"
    assert git_dir.exists(), f"{git_root} no es un repositorio git."
    proc = subprocess.run(
        ["git", "-C", str(git_root), "tag", "-l", "entrega-02"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    assert "entrega-02" in proc.stdout.split(), (
        "No existe la etiqueta entrega-02 en este clon. "
        "El Hito 2 se cierra con `git tag -a entrega-02` y el push de esa etiqueta. "
        "La fecha del tag en GitHub es la marca de la entrega."
    )


def test_el_job_es_invocable_como_pide_el_enunciado(repo_root: Path):
    """La pieza A deja spark/job_01.py en la ruta exacta del enunciado."""
    path = repo_root / "spark" / "job_01.py"
    assert path.is_file(), (
        "Desde el clon, el cierre ejecuta `python spark/job_01.py` y espera exit=0. "
        "Ese fichero no está."
    )
    texto = path.read_text(encoding="utf-8")
    assert "if __name__" in texto, (
        "spark/job_01.py no tiene bloque __main__. "
        "`python spark/job_01.py` no arrancaría el job."
    )
