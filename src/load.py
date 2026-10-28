"""Load: data/clean/ → data/serving/observatorio.sqlite (+ opcional Parquet)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from .paths import CLEAN, SERVING, ensure_dirs

DB_PATH = SERVING / "observatorio.sqlite"
TABLES = ("countries", "languages", "indicators", "join_orphans")


def run_load(write_parquet: bool = False) -> Path:
    ensure_dirs()
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(DB_PATH)
    try:
        for name in TABLES:
            csv_path = CLEAN / f"{name}.csv"
            if not csv_path.exists():
                print(f"[load] omitido (no hay CSV): {name}")
                continue
            df = pd.read_csv(csv_path)
            df.to_sql(name, conn, index=False, if_exists="replace")
            print(f"[load] {name}: {len(df)} filas → sqlite")
            if write_parquet:
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
    finally:
        conn.close()

    print(f"[load] base → {DB_PATH}")
    return DB_PATH


if __name__ == "__main__":
    run_load()
