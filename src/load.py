"""Load: data/clean/ → data/serving/observatorio.sqlite (+ opcional Parquet)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from .paths import CLEAN, SERVING, ensure_dirs

DB_PATH = SERVING / "observatorio.sqlite"
TABLES = ("countries", "languages", "indicators", "excluded_entities", "join_orphans")

# CAMBIO (Hito 2): de dónde se lee cada tabla de la zona limpia.
# ANTES: siempre pd.read_csv(CLEAN / f"{name}.csv").
# AHORA: primero el Parquet tipado del contrato (countries.parquet,
# languages.parquet y el dataset Hive data/clean/indicators/); el CSV solo
# queda como respaldo para tablas que no tienen Parquet (excluidos, huérfanos)
# o para las pruebas de la Práctica 1.
# Las rutas se calculan al llamar (no al importar) para respetar CLEAN si se
# redirige a otro directorio (lo hacen las pruebas en un directorio temporal).
def _clean_parquet(name: str) -> Path | None:
    return {
        "countries": CLEAN / "countries.parquet",
        "languages": CLEAN / "languages.parquet",
        "indicators": CLEAN / "indicators",  # directorio Hive indicator_id=<id>/
    }.get(name)


def _read_clean(name: str) -> pd.DataFrame | None:
    parquet = _clean_parquet(name)
    if parquet is not None and parquet.exists():
        df = pd.read_parquet(parquet)
        if name == "indicators":
            # La columna de partición vuelve como categoría: se deja como texto,
            # y se mantiene el tipado del contrato (year entero, value real).
            df["indicator_id"] = df["indicator_id"].astype(str)
            df = df.astype({"year": "Int64", "value": "float64"})
        return df
    csv_path = CLEAN / f"{name}.csv"
    if csv_path.exists():
        return pd.read_csv(csv_path)
    return None


def run_load(write_parquet: bool = False) -> Path:
    ensure_dirs()
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(DB_PATH)
    try:
        for name in TABLES:
            df = _read_clean(name)
            if df is None:
                print(f"[load] omitido (no hay datos en clean/): {name}")
                continue
            df.to_sql(name, conn, index=False, if_exists="replace")
            print(f"[load] {name}: {len(df)} filas → sqlite")
            if write_parquet:
                # CAMBIO: se añade Parquet como formato de salida columnar en data/serving/.
                # ANTES: solo se cargaban los datos en la base de datos relacional SQLite (observatorio.sqlite).
                # AHORA: opcionalmente con --parquet se exportan también en formato Parquet (.parquet),
                # optimizado para consultas analíticas y herramientas de Big Data (Spark, DuckDB, etc.).
                pq = SERVING / f"{name}.parquet"
                try:
                    df.to_parquet(pq, index=False)
                    print(f"[load] {name} → {pq}")
                except ImportError:
                    print(
                        "[load] Parquet omitido: instalad pyarrow "
                        "(pip install pyarrow)"
                    )
                    write_parquet = False
        # CAMBIO (Hito 2): índice para que las fichas y el job Spark consulten rápido.
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "indicators" in tables:
            columns = {row[1] for row in conn.execute("PRAGMA table_info(indicators)")}
            if {"iso3", "indicator_id", "year"} <= columns:
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_indicators_key "
                    "ON indicators (iso3, indicator_id, year)"
                )
        conn.commit()
    finally:
        conn.close()

    print(f"[load] base → {DB_PATH}")
    return DB_PATH


if __name__ == "__main__":
    run_load()
