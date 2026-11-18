"""Job Spark del Hito 2: cobertura por (indicator_id, década).

AÑADIDO (Hito 2). Uso, desde la raíz del repo y después de `python -m src.pipeline`:

    python spark/job_01.py                 # master local[*]
    python spark/job_01.py --master local[1]
    python spark/job_01.py --master local[4]

Qué hace:
1. Lee el Parquet particionado data/clean/indicators/ (indicator_id=<id>/).
2. Calcula la cobertura por (indicator_id, década) de tres maneras:
   a) RDD al estilo MapReduce: map → (clave, 1) y reduceByKey.
   b) DataFrame: groupBy + filas con dato, países distintos, primer y último año.
   c) Spark SQL: la misma consulta sobre una vista temporal.
3. Hace la misma agregación en el SQLite de data/serving/ y compara las cuatro.
   Si alguna no coincide, termina con código de salida 1.
4. Escribe la versión DataFrame en data/serving/spark/cobertura_decada/ (Parquet).
5. Imprime el plan físico y el número de particiones del DataFrame leído.
6. Deja el event log en logs/spark-events/ (para copiarlo a evidencias/).

Solo cuentan filas con valor no nulo: un nulo no es una observación.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
INDICATORS = ROOT / "data" / "clean" / "indicators"
SQLITE = ROOT / "data" / "serving" / "observatorio.sqlite"
OUTPUT = ROOT / "data" / "serving" / "spark" / "cobertura_decada"
EVENT_LOG = ROOT / "logs" / "spark-events"

# Columnas comunes de las cuatro versiones (RDD solo calcula n_rows).
COLUMNS = ["indicator_id", "decade", "n_rows", "n_countries", "first_year", "last_year"]


def build_session(master: str) -> SparkSession:
    """Arranque del enunciado: master local y event log en logs/spark-events/."""
    EVENT_LOG.mkdir(parents=True, exist_ok=True)
    return (
        SparkSession.builder
        .appName("job01")
        .master(master)
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", EVENT_LOG.as_uri())
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def cobertura_rdd(df) -> dict[tuple[str, int], int]:
    """MapReduce sobre RDD: cada fila emite ((indicador, década), 1) y se suma por clave."""
    pares = (
        df.select("indicator_id", "year", "value")
        .rdd
        .filter(lambda row: row["value"] is not None and row["year"] is not None)  # value is not None
        .map(lambda row: ((row["indicator_id"], (int(row["year"]) // 10) * 10), 1))  # Map
    )
    conteos = pares.reduceByKey(lambda a, b: a + b)  # Shuffle + Reduce
    return dict(conteos.collect())


def cobertura_dataframe(df):
    """DataFrame: misma clave, filas con dato, países distintos y rango de años."""
    return (
        df.filter(F.col("value").isNotNull())
        .withColumn("decade", (F.floor(F.col("year") / 10) * 10).cast("int"))
        .groupBy("indicator_id", "decade")
        .agg(
            F.count("*").alias("n_rows"),
            F.countDistinct("iso3").alias("n_countries"),
            F.min("year").cast("int").alias("first_year"),
            F.max("year").cast("int").alias("last_year"),
        )
        .orderBy("indicator_id", "decade")
    )


SQL_COBERTURA = """
SELECT indicator_id,
       CAST(FLOOR(year / 10) * 10 AS INT) AS decade,
       COUNT(*)                            AS n_rows,
       COUNT(DISTINCT iso3)                AS n_countries,
       CAST(MIN(year) AS INT)              AS first_year,
       CAST(MAX(year) AS INT)              AS last_year
FROM indicators
WHERE value IS NOT NULL
GROUP BY indicator_id, CAST(FLOOR(year / 10) * 10 AS INT)
ORDER BY indicator_id, decade
"""

# En SQLite, year es INTEGER: year / 10 ya es división entera.
SQLITE_COBERTURA = """
SELECT indicator_id,
       (year / 10) * 10      AS decade,
       COUNT(*)              AS n_rows,
       COUNT(DISTINCT iso3)  AS n_countries,
       MIN(year)             AS first_year,
       MAX(year)             AS last_year
FROM indicators
WHERE value IS NOT NULL AND year IS NOT NULL
GROUP BY indicator_id, (year / 10) * 10
ORDER BY indicator_id, decade
"""


def cobertura_sqlite() -> dict[tuple[str, int], tuple]:
    with sqlite3.connect(SQLITE) as conn:
        filas = conn.execute(SQLITE_COBERTURA).fetchall()
    return {(r[0], int(r[1])): tuple(int(x) for x in r[2:]) for r in filas}


def _a_dict(rows) -> dict[tuple[str, int], tuple]:
    return {
        (r["indicator_id"], int(r["decade"])): (
            int(r["n_rows"]),
            int(r["n_countries"]),
            int(r["first_year"]),
            int(r["last_year"]),
        )
        for r in rows
    }


def comparar(nombre: str, a: dict, b: dict) -> list[str]:
    """Lista de diferencias entre dos resultados (vacía si coinciden)."""
    errores = []
    for clave in sorted(set(a) | set(b)):
        if a.get(clave) != b.get(clave):
            errores.append(f"{nombre}: {clave} → {a.get(clave)} != {b.get(clave)}")
    return errores


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Cobertura por indicador y década")
    parser.add_argument("--master", default="local[*]", help="local, local[1], local[4], local[*]")
    args = parser.parse_args(argv)

    if not INDICATORS.is_dir():
        print(f"[job_01] falta {INDICATORS}: ejecutad antes python -m src.pipeline", file=sys.stderr)
        return 2
    if not SQLITE.is_file():
        print(f"[job_01] falta {SQLITE}: ejecutad antes python -m src.pipeline", file=sys.stderr)
        return 2

    spark = build_session(args.master)
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(f"[job_01] master={args.master} app_id={spark.sparkContext.applicationId}")
        print(f"[job_01] event log → {EVENT_LOG}")

        df = spark.read.parquet(str(INDICATORS))
        print(f"[job_01] particiones del DataFrame leído: {df.rdd.getNumPartitions()}")
        df.printSchema()

        # 1) RDD estilo MapReduce
        rdd_counts = cobertura_rdd(df)

        # 2) DataFrame
        cobertura_df = cobertura_dataframe(df)
        print("[job_01] plan físico de la versión DataFrame:")
        cobertura_df.explain(mode="formatted")
        df_rows = cobertura_df.collect()
        df_dict = _a_dict(df_rows)

        # 3) Spark SQL sobre vista temporal
        df.createOrReplaceTempView("indicators")
        sql_dict = _a_dict(spark.sql(SQL_COBERTURA).collect())

        # 4) SQLite de serving
        sqlite_dict = cobertura_sqlite()

        errores = []
        errores += comparar("RDD vs DataFrame", rdd_counts, {k: v[0] for k, v in df_dict.items()})
        errores += comparar("DataFrame vs SQL", df_dict, sql_dict)
        errores += comparar("DataFrame vs SQLite", df_dict, sqlite_dict)

        print(f"[job_01] grupos (indicator_id, década): {len(df_dict)}")
        print(f"[job_01] filas con dato contadas: {sum(v[0] for v in df_dict.values())}")
        cobertura_df.show(200, truncate=False)

        if errores:
            print("[job_01] ERROR: las versiones no coinciden:", file=sys.stderr)
            for linea in errores[:50]:
                print("  " + linea, file=sys.stderr)
            return 1
        print("[job_01] OK: RDD == DataFrame == Spark SQL == SQLite")

        cobertura_df.write.mode("overwrite").parquet(str(OUTPUT))
        print(f"[job_01] cobertura → {OUTPUT} (parquet)")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
