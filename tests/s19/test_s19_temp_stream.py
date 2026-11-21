"""S19: streaming/temp_stream.py y ACTA_S19.md (Spark Structured Streaming).

Comprobación estática: no arranca Spark ni Kafka. Que pase no demuestra
recuperación; la recuperación se demuestra en DataLab con el ensayo A-F y
se cuenta en el acta. Esto solo verifica que el query declarado es el del
contrato de aula y que la memoria (checkpoint) tiene una ruta estable.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.s19

RUTA = Path("streaming") / "temp_stream.py"


FALTA = (
    "Falta streaming/temp_stream.py. Copiad el starter de "
    "cesar-andress/bigdata2026 (streaming/temp_stream.py) a vuestro "
    "repositorio y resolved los huecos T1 a T6."
)


def _buscar(repo_root: Path, git_root: Path) -> Path | None:
    for base in (repo_root, git_root):
        if (base / RUTA).is_file():
            return base / RUTA
    return None


def test_existe_temp_stream(repo_root: Path, git_root: Path):
    if _buscar(repo_root, git_root) is None:
        pytest.fail(FALTA)


def _fichero(repo_root: Path, git_root: Path) -> Path:
    path = _buscar(repo_root, git_root)
    if path is not None:
        return path
    pytest.skip("sin streaming/temp_stream.py (ver test_existe_temp_stream)")


@pytest.fixture(scope="module")
def codigo(repo_root: Path, git_root: Path) -> str:
    return _fichero(repo_root, git_root).read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def arbol(codigo: str) -> ast.AST:
    return ast.parse(codigo)


def _cadenas(arbol: ast.AST) -> list[str]:
    return [
        n.value
        for n in ast.walk(arbol)
        if isinstance(n, ast.Constant) and isinstance(n.value, str)
    ]


def _llamadas(arbol: ast.AST) -> list[ast.Call]:
    return [n for n in ast.walk(arbol) if isinstance(n, ast.Call)]


def _nombre(call: ast.Call) -> str:
    f = call.func
    return f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", "")


def _opcion(arbol: ast.AST, clave: str) -> list[ast.expr]:
    """Valores pasados a .option(clave, valor)."""
    return [
        c.args[1]
        for c in _llamadas(arbol)
        if _nombre(c) == "option"
        and len(c.args) == 2
        and isinstance(c.args[0], ast.Constant)
        and c.args[0].value == clave
    ]


def test_es_python_valido(codigo: str):
    ast.parse(codigo)


def test_no_quedan_huecos(codigo: str, arbol: ast.AST):
    assert "TODO" not in codigo, "Quedan TODO en streaming/temp_stream.py."
    assert "NotImplementedError" not in {
        getattr(n, "id", None) for n in ast.walk(arbol)
    }, "Queda algún raise NotImplementedError (T1 a T6 sin resolver)."


def test_t1_fuente_kafka(codigo: str, arbol: ast.AST):
    cadenas = _cadenas(arbol)
    assert "readStream" in codigo, "La fuente no es readStream."
    assert "kafka" in cadenas, 'Falta format("kafka").'
    assert "127.0.0.1:9092" in cadenas, "El broker no es 127.0.0.1:9092."
    assert "sensores.temp" in cadenas, "No se suscribe a sensores.temp."
    assert _opcion(arbol, "kafka.bootstrap.servers"), (
        'Falta .option("kafka.bootstrap.servers", ...).'
    )
    assert _opcion(arbol, "subscribe"), 'Falta .option("subscribe", ...).'
    assert _opcion(arbol, "startingOffsets"), (
        'Falta .option("startingOffsets", ...) (frontera del primer día).'
    )


def test_t2_parseo_json_tipado(codigo: str, arbol: ast.AST):
    cadenas = _cadenas(arbol)
    assert "from_json" in {_nombre(c) for c in _llamadas(arbol)}, (
        "El value de Kafka no se interpreta con from_json."
    )
    assert re.search(r"CAST\s*\(\s*value\s+AS\s+STRING", codigo, re.I) or (
        "cast" in codigo and "value" in cadenas
    ), "value llega en bytes: hay que pasarlo a cadena antes de from_json."
    for campo in ("sensor", "valor", "ts"):
        assert campo in cadenas, f"El esquema no declara el campo {campo!r}."


def test_t3_watermark_y_ventana(arbol: ast.AST):
    llamadas = _llamadas(arbol)
    wm = [c for c in llamadas if _nombre(c) == "withWatermark"]
    assert wm, "Falta withWatermark sobre ts."
    args = [a.value for a in wm[0].args if isinstance(a, ast.Constant)]
    assert args[:2] == ["ts", "10 minutes"], (
        f'La watermark debe ser withWatermark("ts", "10 minutes"); hay {args}.'
    )
    ventanas = [c for c in llamadas if _nombre(c) == "window"]
    assert any(
        any(isinstance(a, ast.Constant) and a.value == "1 minute" for a in c.args)
        for c in ventanas
    ), 'Falta window(col("ts"), "1 minute").'
    nombres = {_nombre(c) for c in llamadas}
    assert "groupBy" in nombres and "count" in nombres, (
        "Falta groupBy(window, sensor).count(): sin agregación no hay estado."
    )


def test_t4_sink_trigger_y_nombre(codigo: str, arbol: ast.AST):
    cadenas = _cadenas(arbol)
    nombres = {_nombre(c) for c in _llamadas(arbol)}
    assert "writeStream" in codigo and "start" in nombres, (
        "No hay writeStream ... start(): sin start() no hay query activo."
    )
    assert "console" in cadenas, 'El sink de aula es format("console").'
    assert "update" in cadenas, 'Falta outputMode("update").'
    assert "5 seconds" in cadenas, 'Falta trigger(processingTime="5 seconds").'
    assert "temp-stream" in cadenas, 'Falta queryName("temp-stream").'


def test_t4_checkpoint_estable(codigo: str, arbol: ast.AST):
    valores = _opcion(arbol, "checkpointLocation")
    assert valores, (
        'Falta .option("checkpointLocation", ...). Sin checkpoint estable '
        "cada arranque es un primer día."
    )
    assert "checkpoints/temp-stream" in codigo, (
        "La ruta canónica es ~/checkpoints/temp-stream "
        "(= /home/admin/checkpoints/temp-stream)."
    )
    for prohibido in ("tempfile", "mkdtemp", "uuid", "time.time", "datetime.now"):
        assert prohibido not in codigo, (
            f"La ruta de checkpoint no puede depender de {prohibido}: "
            "si cambia en cada arranque no hay reanudación."
        )


def test_t5_espera_y_no_borra_el_checkpoint(codigo: str, arbol: ast.AST):
    nombres = {_nombre(c) for c in _llamadas(arbol)}
    assert nombres & {"awaitTermination", "awaitAnyTermination"}, (
        "Falta spark.streams.awaitAnyTermination() (o q.awaitTermination()): "
        "el proceso terminaría y los queries con él."
    )
    assert "stop" in nombres, "Falta q.stop() al interrumpir."
    for prohibido in ("rmtree", "os.remove", "unlink", "rm -rf"):
        assert prohibido not in codigo, (
            f"{prohibido} en el query: borrar el checkpoint convierte la "
            "reanudación en un arranque nuevo (paso F)."
        )


def test_t6_reto_query_de_nulos_con_su_checkpoint(codigo: str, arbol: ast.AST):
    cadenas = _cadenas(arbol)
    assert "temp-nulos" in cadenas, 'Falta el segundo query, queryName("temp-nulos").'
    assert "isNull" in {_nombre(c) for c in _llamadas(arbol)}, (
        'El query temp-nulos debe filtrar col("valor").isNull().'
    )
    assert "checkpoints/temp-nulos" in codigo, (
        "temp-nulos necesita su propio checkpoint: ~/checkpoints/temp-nulos."
    )
    assert len(_opcion(arbol, "checkpointLocation")) >= 2, (
        "Hay dos queries y cada uno declara su checkpointLocation."
    )


def test_acta_s19(repo_root: Path, git_root: Path):
    for base in (repo_root, git_root):
        acta = base / "ACTA_S19.md"
        if acta.is_file():
            break
    else:
        pytest.fail("Falta ACTA_S19.md en la raíz de vuestro repositorio.")
    texto = acta.read_text(encoding="utf-8").lower()
    for palabra in ("job_01", "temp-stream", "checkpoint", "progreso",
                    "estado", "temp-nulos"):
        assert palabra in texto, f"ACTA_S19.md no menciona «{palabra}»."
    assert len(texto.split()) >= 80, (
        "ACTA_S19.md es demasiado corta para las cinco respuestas."
    )
