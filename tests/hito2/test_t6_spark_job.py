"""T6: spark/job_01.py: RDD, DataFrame, Spark SQL y SQLite.

El Hito 2 pide un solo programa, ``spark/job_01.py``, que lee el Parquet
de indicators, calcula la cobertura por indicador y década de tres maneras
y no termina en 0 si esas cuentas no coinciden.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

pytestmark = pytest.mark.hito2


def _codigo(repo_root: Path) -> str:
    path = repo_root / "spark" / "job_01.py"
    if not path.is_file():
        pytest.fail(
            "Falta spark/job_01.py. El Hito 2 arranca por ese programa: "
            "cobertura por (indicator_id, década) en RDD, DataFrame y Spark SQL, "
            "contrastada con el SQLite de serving/."
        )
    return path.read_text(encoding="utf-8")


def test_el_job_es_python_valido(repo_root: Path):
    """Si el fichero existe, tiene que parsear."""
    ast.parse(_codigo(repo_root))


def test_arranca_spark_en_local_y_lee_el_parquet(repo_root: Path):
    job_source = _codigo(repo_root)
    """SparkSession local y lectura de data/clean/indicators."""
    assert "SparkSession" in job_source, "No se construye un SparkSession."
    assert "local" in job_source, (
        "El master no parece local. En la entrega el job corre en la máquina "
        "del aula (local, local[1] o local[*]), no contra un clúster."
    )
    assert "indicators" in job_source and "parquet" in job_source.lower(), (
        "El job no lee el dataset Parquet de indicators "
        "(data/clean/indicators)."
    )


def test_calcula_la_decada(repo_root: Path):
    job_source = _codigo(repo_root)
    """La clave de agregación es el indicador y la década, no el año suelto."""
    lower = job_source.lower()
    assert "decad" in lower or "// 10" in job_source or "/ 10" in job_source, (
        "No aparece el cálculo de la década (año // 10, o una columna década). "
        "La agregación es por (indicator_id, década)."
    )
    assert "indicator_id" in job_source, "La clave tiene que incluir indicator_id."


def test_no_cuenta_valores_nulos(repo_root: Path):
    job_source = _codigo(repo_root)
    """Solo entran filas con valor no nulo."""
    lower = job_source.lower()
    assert any(
        token in lower
        for token in ("isnotnull", "is not null", "notnull", "dropna", "value is not none")
    ), (
        "No se ve un filtro de value nulo. La cobertura cuenta filas con "
        "valor presente; un nulo no es una observación."
    )


def test_version_rdd_en_estilo_mapreduce(repo_root: Path):
    job_source = _codigo(repo_root)
    """RDD: emitir pares y reducir por clave. No basta un groupBy de DataFrame."""
    assert ".rdd" in job_source or "parallelize" in job_source, (
        "No hay paso a RDD. La primera versión es MapReduce sobre RDD."
    )
    assert any(token in job_source for token in ("map(", "flatMap(", "mapValues(")), (
        "El RDD no emite pares (map / flatMap). El estilo pedido es "
        "clave-valor y reducir por clave."
    )
    assert any(
        token in job_source for token in ("reduceByKey", "aggregateByKey", "combineByKey")
    ), (
        "Falta reduceByKey (o aggregateByKey / combineByKey). "
        "groupByKey no es la reducción que pide el enunciado."
    )


def test_version_dataframe_con_paises_y_rango_de_anios(repo_root: Path):
    job_source = _codigo(repo_root)
    """DataFrame: filas no nulas, países distintos, primer y último año con dato."""
    assert "groupBy" in job_source or "group_by" in job_source, (
        "La versión DataFrame no agrupa."
    )
    lower = job_source.lower()
    assert "countdistinct" in lower or "count_distinct" in lower or "nunique" in lower, (
        "Falta el número de países distintos (countDistinct sobre iso3)."
    )
    assert "min(" in lower and "max(" in lower, (
        "Faltan el primer y el último año con dato (min/max de year)."
    )


def test_version_spark_sql_con_las_mismas_columnas(repo_root: Path):
    job_source = _codigo(repo_root)
    """Vista temporal y spark.sql; no un tercer cálculo escrito a mano en pandas."""
    assert "createOrReplaceTempView" in job_source or "createTempView" in job_source, (
        "No se registra una vista temporal para Spark SQL."
    )
    assert "spark.sql" in job_source or ".sql(" in job_source, (
        "No hay una consulta Spark SQL."
    )
    sql = job_source.lower()
    assert "group by" in sql, "La consulta SQL no agrupa."
    assert "count" in sql and ("count(distinct" in sql or "countdistinct" in sql), (
        "La versión SQL tiene que repetir el conteo de filas y los países distintos."
    )


def test_las_tres_versiones_y_sqlite_tienen_que_coincidir(repo_root: Path):
    job_source = _codigo(repo_root)
    """Si RDD, DataFrame, SQL o SQLite difieren, el proceso sale con código distinto de 0."""
    assert "sqlite" in job_source.lower(), (
        "El job no consulta el SQLite de data/serving/observatorio.sqlite. "
        "La versión DataFrame tiene que coincidir con esa misma agregación."
    )
    sale_mal = any(
        token in job_source
        for token in ("sys.exit", "SystemExit", "raise SystemExit", "exit(")
    )
    assert sale_mal and ("!=" in job_source or "differ" in job_source.lower() or "==" in job_source), (
        "No se ve la comparación que aborta el proceso cuando las cuentas no coinciden. "
        "El enunciado pide código de salida distinto de 0."
    )


def test_escribe_la_cobertura_en_parquet(repo_root: Path):
    job_source = _codigo(repo_root)
    """El resultado DataFrame queda en data/serving/spark/cobertura_decada/."""
    assert "cobertura_decada" in job_source, (
        "No aparece data/serving/spark/cobertura_decada/. "
        "Ahí se escribe el Parquet de la versión DataFrame."
    )
    assert "parquet" in job_source.lower(), "Esa salida tiene que ser Parquet."


def test_imprime_plan_fisico_y_numero_de_particiones(repo_root: Path):
    job_source = _codigo(repo_root)
    """Plan físico de la versión DataFrame y particiones del DataFrame leído."""
    assert "explain" in job_source.lower(), (
        "El job no imprime el plan (explain). Hace falta el plan físico."
    )
    assert "getNumPartitions" in job_source or "numPartitions" in job_source, (
        "No se imprime el número de particiones del DataFrame leído."
    )


def test_deja_event_log(repo_root: Path):
    job_source = _codigo(repo_root)
    """logs/spark-events sobrevive a la sesión (event log activado)."""
    lower = job_source.lower()
    assert "eventlog" in lower or "event_log" in lower or "spark-events" in lower, (
        "No se configura el event log de Spark. "
        "Tiene que escribirse en logs/spark-events/ para poder copiarlo a evidencias/."
    )


def test_no_hardcodea_los_cinco_paises_del_starter(repo_root: Path):
    job_source = _codigo(repo_root)
    """El job agrega el universo que haya en el Parquet, no una lista cerrada."""
    for iso in ("ESP", "FRA", "DEU", "ITA", "PRT"):
        assert iso not in job_source, (
            f"job_01.py menciona {iso}. La cobertura sale de los datos, "
            "no de los cinco países de la Práctica 1."
        )
