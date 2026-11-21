"""Sesión 19: query continuo ``temp-stream`` (Spark Structured Streaming).

AÑADIDO (sesión 19): starter del profesor copiado a este repositorio, con los
seis huecos (T1 a T6) ya resueltos. Cada hueco lleva un comentario
"T<n> (resuelto)" con lo que hace.

Contrato de aula (DataLab, ``admin@datalab``):

* broker Kafka ``127.0.0.1:9092``, topic ``sensores.temp``;
* eventos JSON ``{"sensor": "s01", "valor": 22.5, "ts": "2026-11-18T12:00:00Z"}``;
* conteo por ventana de 1 minuto y por sensor, watermark de 10 minutos;
* sink ``console`` en modo ``update``, trigger de 5 segundos;
* ``checkpointLocation`` estable: ``~/checkpoints/temp-stream``.

Reto sorpresa (T6): un segundo query, ``temp-nulos``, que muestra los
eventos cuyo ``valor`` llega nulo (esquema roto). Tiene **su propio**
checkpoint, ``~/checkpoints/temp-nulos``: dos queries nunca comparten ruta.

Arranque (el conector Kafka va aparte de PySpark):

    spark-submit \\
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 \\
      streaming/temp_stream.py

Ctrl+C detiene el query (equivale a ``q.stop()``). Volver a lanzar el mismo
comando con la misma ruta de checkpoint es la prueba de parar y arrancar.
"""

from __future__ import annotations

import argparse
import os
import signal
import threading

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import DoubleType, StringType, StructType, TimestampType

BROKER = "127.0.0.1:9092"
TOPIC = "sensores.temp"
CHECKPOINT = os.path.expanduser("~/checkpoints/temp-stream")
QUERY_NAME = "temp-stream"
CHECKPOINT_NULOS = os.path.expanduser("~/checkpoints/temp-nulos")
QUERY_NULOS = "temp-nulos"


def construir_fuente(spark: SparkSession, starting_offsets: str) -> DataFrame:
    """T1. Fuente Kafka.

    ``readStream`` con ``format("kafka")``, el broker ``BROKER``, suscrito a
    ``TOPIC`` y ``startingOffsets`` = ``starting_offsets``. Recordad:
    ``startingOffsets`` solo cuenta si el checkpoint aún no tiene progreso.
    """
    # T1 (resuelto): fuente de streaming leída de Kafka. Cada fila trae el
    # mensaje en `value` (bytes) más key, topic, partition, offset y timestamp.
    # El offset es lo que el checkpoint apunta como "progreso".
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", starting_offsets)
        .load()
    )


def parsear(df: DataFrame) -> DataFrame:
    """T2. de ``value`` (bytes) a JSON y a columnas tipadas ``sensor``, ``valor``, ``ts``."""
    esquema = (
        StructType()
        .add("sensor", StringType())
        .add("valor", DoubleType())
        .add("ts", TimestampType())
    )
    # T2 (resuelto): bytes → texto → columnas. from_json no falla con un
    # mensaje roto: deja a nulo el campo que no encuentra (lo vigila T6).
    return (
        df.selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), esquema).alias("d"))
        .select("d.*")
    )


def agregar(ev: DataFrame) -> DataFrame:
    """T3. Estado: watermark de 10 minutos sobre ``ts`` y conteo por
    ``window(ts, "1 minute")`` y ``sensor``."""
    # T3 (resuelto): conteo por (minuto, sensor). Las cuentas abiertas son el
    # "estado" que guarda el checkpoint; la watermark permite cerrar ventanas
    # y tirar los eventos que llegan más de 10 minutos tarde.
    return (
        ev.withWatermark("ts", "10 minutes")
        .groupBy(window(col("ts"), "1 minute"), col("sensor"))
        .count()
    )


def arrancar(agg: DataFrame, checkpoint: str):
    """T4. Sink: console, ``outputMode("update")``, ``checkpointLocation``
    = ``checkpoint``, ``trigger(processingTime="5 seconds")``,
    ``queryName(QUERY_NAME)`` y ``start()``. Devuelve el query."""
    # T4 (resuelto): un micro-lote cada 5 s; "update" solo imprime las cuentas
    # que cambian en ese micro-lote. El checkpoint es una ruta fija.
    return (
        agg.writeStream
        .format("console")
        .outputMode("update")
        .option("checkpointLocation", checkpoint)
        .option("truncate", "false")
        .trigger(processingTime="5 seconds")
        .queryName(QUERY_NAME)
        .start()
    )


def vigilar_nulos(ev: DataFrame, checkpoint: str):
    """T6. Reto sorpresa. Segundo query sobre los mismos eventos parseados:
    ``filter(col("valor").isNull())``, sink console en modo ``append``,
    ``checkpointLocation`` = ``checkpoint`` (¡distinta de la de temp-stream!),
    ``queryName(QUERY_NULOS)``, mismo trigger y ``start()``."""
    # T6 (resuelto): segundo query sobre los mismos eventos. Sin agregación,
    # así que el modo es "append" (cada evento nulo se imprime una vez). Su
    # checkpoint es otro: cada query lleva su propio progreso.
    return (
        ev.filter(col("valor").isNull())
        .writeStream
        .format("console")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .option("truncate", "false")
        .trigger(processingTime="5 seconds")
        .queryName(QUERY_NULOS)
        .start()
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Query continuo temp-stream (S19)")
    p.add_argument("--checkpoint", default=CHECKPOINT)
    p.add_argument(
        "--starting-offsets",
        default="earliest",
        choices=["earliest", "latest"],
        help="Frontera inicial; ignorada si el checkpoint ya tiene progreso.",
    )
    p.add_argument("--checkpoint-nulos", default=CHECKPOINT_NULOS)
    args = p.parse_args()

    spark = SparkSession.builder.appName(QUERY_NAME).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ev = parsear(construir_fuente(spark, args.starting_offsets))
    q = arrancar(agregar(ev), args.checkpoint)
    qn = vigilar_nulos(ev, args.checkpoint_nulos)
    for query, ruta in ((q, args.checkpoint), (qn, args.checkpoint_nulos)):
        print(f"query={query.name} id={query.id} checkpoint={ruta}")

    # T5 (resuelto): el proceso se queda esperando a los queries. Con Ctrl+C
    # se paran los dos con stop(); el checkpoint se queda en disco tal cual
    # para que el siguiente arranque siga donde lo dejó.
    # Ctrl+C solo levanta una marca: el manejador de PySpark llamaba a Java
    # (cancelAllJobs) en mitad de awaitAnyTermination() y py4j fallaba con
    # "reentrant call". Por eso se espera en tramos de 1 s y se para aquí.
    parar = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: parar.set())
    try:
        while not parar.is_set():
            if spark.streams.awaitAnyTermination(timeout=1):
                break  # algún query terminó (o falló) por su cuenta
        print("Ctrl+C: parando temp-stream y temp-nulos (los checkpoints se conservan)")
    except KeyboardInterrupt:
        print("Ctrl+C: parando temp-stream y temp-nulos (los checkpoints se conservan)")
    finally:
        q.stop()
        qn.stop()


if __name__ == "__main__":
    main()
