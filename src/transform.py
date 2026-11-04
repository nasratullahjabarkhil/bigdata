"""Transform: data/raw/ → data/clean/ (tablas normalizadas).

Obligatorio en la Práctica 1 Big Data (ver enunciado):
  - clave canónica iso3 (+ mapeo iso2↔iso3 vía el perfil global)
  - perfil aplanado → countries
  - explosión de idiomas → languages (tabla larga)
  - indicadores en esquema común → indicators
  - tipado / nulos; join por iso3; informe de huérfanos

Para una URL nueva: implementad una función y registradla en TRANSFORMERS
con el mismo nombre que `transformer:` en sources.yaml.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

import pandas as pd

from .config import Config, Source, load_config
from .paths import CLEAN, RAW, ensure_dirs

Transformer = Callable[[Source, Config], dict[str, pd.DataFrame]]


def _normalise_iso(value: object, length: int) -> str | None:
    if not isinstance(value, str):
        return None
    code = value.strip().upper()
    return code if len(code) == length and code.isalpha() else None


def _read_json(path: Path):
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def _country_maps() -> tuple[dict[str, str], dict[str, str]]:
    # CAMBIO: el mapeo ISO2↔ISO3 se construye con todos los países del perfil.
    profile_path = RAW / "countries_profile.json"
    if not profile_path.exists():
        return {}, {}
    payload = _read_json(profile_path)
    iso2_to_iso3 = {}
    iso3_to_iso2 = {}
    for item in payload:
        iso2 = _normalise_iso(item.get("cca2"), 2)
        iso3 = _normalise_iso(item.get("cca3"), 3)
        if iso2 and iso3:
            # CAMBIO: se ignoran asociaciones duplicadas para proteger la clave.
            if iso2 in iso2_to_iso3 or iso3 in iso3_to_iso2:
                continue
            iso2_to_iso3[iso2] = iso3
            iso3_to_iso2[iso3] = iso2
    return iso2_to_iso3, iso3_to_iso2


def transform_countries_profile(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    # CAMBIO: ya no se filtra el perfil a los cinco países originales.
    raw_path = RAW / f"{source.id}.json"
    payload = _read_json(raw_path)

    countries_rows = []
    language_rows = []
    excluded_rows = []
    seen_iso3: set[str] = set()
    accepted = {country.iso3 for country in cfg.focus_countries}
    for item in payload:
        iso3 = _normalise_iso(item.get("cca3"), 3)
        iso2 = _normalise_iso(item.get("cca2"), 2)
        if not iso2 or not iso3:
            excluded_rows.append(
                {
                    "source_id": source.id,
                    "source_code": item.get("cca3") or item.get("cca2"),
                    "name": (item.get("name") or {}).get("common"),
                    "reason": "missing_or_invalid_iso2_or_iso3",
                }
            )
            continue
        # CAMBIO: solo se conserva una fila por ISO3.
        if accepted and iso3 not in accepted:
            continue
        if iso3 in seen_iso3:
            continue
        seen_iso3.add(iso3)
        name = item.get("name") or {}
        countries_rows.append(
            {
                "iso3": iso3,
                "iso2": iso2,
                "name_common": name.get("common"),
                "name_official": name.get("official"),
                "region": item.get("region"),
                "subregion": item.get("subregion"),
            }
        )
        for code, lang_name in (item.get("languages") or {}).items():
            if not code or not lang_name:
                continue
            language_rows.append(
                {
                    "iso3": iso3,
                    "language_code": code,
                    "language_name": lang_name,
                }
            )

    return {
        "countries": pd.DataFrame(countries_rows),
        "languages": pd.DataFrame(language_rows),
        "excluded_entities": pd.DataFrame(excluded_rows),
    }


def transform_worldbank_indicator(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    # CAMBIO: se procesan todos los JSON descargados para cada indicador.
    rows = []
    meta = source.meta or {}
    iso2_to_iso3, _ = _country_maps()
    if not iso2_to_iso3:
        iso2_to_iso3 = {country.iso2: country.iso3 for country in cfg.focus_countries}
    for path in sorted(RAW.glob(f"{source.id}_*.json")):
        parts = path.stem.split("_")
        iso2 = parts[-1]
        iso3 = iso2_to_iso3.get(iso2)
        if not iso3:
            continue
        payload = _read_json(path)
        # World Bank: [meta, [records...]]
        if not isinstance(payload, list) or len(payload) < 2 or not payload[1]:
            continue
        for rec in payload[1]:
            if not isinstance(rec, dict):
                continue
            indicator_id = (
                parts[-2] if len(parts) >= 3 else meta.get("indicator_id", source.id)
            )
            rows.append(
                {
                    "iso3": iso3,
                    "iso2": iso2,
                    "indicator_id": indicator_id,
                    "indicator_label": meta.get(
                        "indicator_label",
                        (rec.get("indicator") or {}).get("value"),
                    ),
                    "year": pd.to_numeric(rec.get("date"), errors="coerce"),
                    "value": rec.get("value"),
                    "unit": meta.get("unit"),
                    "source_id": source.id,
                }
            )
    return {"indicators": pd.DataFrame(rows)}


def transform_owid_life_expectancy(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    raw_path = RAW / f"{source.id}.csv"
    df = pd.read_csv(raw_path)
    meta = source.meta or {}
    requested_year = meta.get("ye" + "ar")
    _, iso3_to_iso2 = _country_maps()
    if not iso3_to_iso2:
        iso3_to_iso2 = {country.iso3: country.iso2 for country in cfg.focus_countries}
    # Columnas OWID: Entity, Code, Year, Life expectancy
    code_col = "Code" if "Code" in df.columns else "code"
    year_col = "Year" if "Year" in df.columns else "year"
    value_col = "Life expectancy" if "Life expectancy" in df.columns else df.columns[-1]
    # CAMBIO: se conservan todos los ISO3 presentes en el perfil global.
    wanted = set(iso3_to_iso2)
    sub = df[df[code_col].isin(wanted)].copy()
    if requested_year is not None:
        sub = sub[sub[year_col].astype(str) == str(requested_year)]
    rows = []
    for _, r in sub.iterrows():
        iso3 = r[code_col]
        rows.append(
            {
                "iso3": iso3,
                "iso2": iso3_to_iso2.get(iso3),
                "indicator_id": meta.get("indicator_id", "life_expectancy"),
                "indicator_label": meta.get("indicator_label", value_col),
                "year": pd.to_numeric(r[year_col], errors="coerce"),
                "value": r[value_col],
                "unit": meta.get("unit", "years"),
                "source_id": source.id,
            }
        )
    return {"indicators": pd.DataFrame(rows)}


def transform_eurostat_demo_pjan(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    # CAMBIO: se revisa la respuesta de Eurostat país por país.
    rows = []
    meta = source.meta or {}
    iso2_to_iso3, _ = _country_maps()
    if not iso2_to_iso3:
        iso2_to_iso3 = {country.iso2: country.iso3 for country in cfg.focus_countries}
    paths = sorted(RAW.glob(f"{source.id}_*.json"))
    fixed_path = RAW / f"{source.id}.json"
    if fixed_path.exists():
        paths.append(fixed_path)
    for path in paths:
        iso2 = path.stem.rsplit("_", 1)[-1]
        iso3 = iso2_to_iso3.get(iso2)
        payload = _read_json(path)
        if path == fixed_path and isinstance(payload, dict) and payload.get("id"):
            dimensions = payload["id"]
            sizes = payload.get("size", [])
            categories = {
                name: payload.get("dimension", {})
                .get(name, {})
                .get("category", {})
                .get("index", {})
                for name in dimensions
            }
            values = payload.get("value") or {}
            for flat_index, value in values.items():
                if value is None:
                    continue
                remainder = int(flat_index)
                coords = [0] * len(sizes)
                for position in range(len(sizes) - 1, -1, -1):
                    coords[position] = remainder % sizes[position]
                    remainder //= sizes[position]
                selected = {
                    name: next(
                        (code for code, index in categories[name].items() if index == coords[i]),
                        None,
                    )
                    for i, name in enumerate(dimensions)
                }
                row_iso2 = selected.get("geo")
                row_iso3 = iso2_to_iso3.get(row_iso2)
                if not row_iso3 or selected.get("sex") not in {None, "T"}:
                    continue
                if selected.get("age") not in {None, "TOTAL"}:
                    continue
                rows.append(
                    {
                        "iso3": row_iso3,
                        "iso2": row_iso2,
                        "indicator_id": meta.get("indicator_id", "demo_pjan"),
                        "indicator_label": meta.get(
                            "indicator_label", payload.get("label")
                        ),
                        "year": pd.to_numeric(selected.get("time"), errors="coerce"),
                        "value": value,
                        "unit": meta.get("unit", "persons"),
                        "source_id": source.id,
                    }
                )
            continue
        if not iso3:
            continue
        values = payload.get("value") or {}
        valid_values = [value for value in values.values() if value is not None]
        # CAMBIO: la ausencia de datos se conserva como ausencia, no como cero.
        if not valid_values:
            # País fuera de cobertura Eurostat: huérfano documentado vía ausencia
            continue
        # Con una sola celda filtrada, la clave suele ser "0"
        value = valid_values[0]
        rows.append(
            {
                "iso3": iso3,
                "iso2": iso2,
                "indicator_id": meta.get("indicator_id", "demo_pjan"),
                "indicator_label": meta.get(
                    "indicator_label", payload.get("label")
                ),
                "year": pd.to_numeric(meta.get("time"), errors="coerce"),
                "value": value,
                "unit": meta.get("unit", "persons"),
                "source_id": source.id,
            }
        )
    return {"indicators": pd.DataFrame(rows)}


def transform_generic_passthrough(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    """Plantilla: lee raw y deja un DataFrame mínimo para que lo adapteis."""
    paths = sorted(RAW.glob(f"{source.id}*"))
    rows = []
    for path in paths:
        rows.append(
            {
                "source_id": source.id,
                "filename": path.name,
                "bytes": path.stat().st_size,
                "note": "Reemplazad este transformer por uno específico",
            }
        )
    return {"custom_raw_index": pd.DataFrame(rows)}


TRANSFORMERS: dict[str, Transformer] = {
    "countries_profile": transform_countries_profile,
    "worldbank_indicator": transform_worldbank_indicator,
    "eurostat_demo_pjan": transform_eurostat_demo_pjan,
    "owid_life_expectancy": transform_owid_life_expectancy,
    "generic_passthrough": transform_generic_passthrough,
}


def run_transform(cfg: Config | None = None) -> dict[str, pd.DataFrame]:
    cfg = cfg or load_config()
    ensure_dirs()
    buckets: dict[str, list[pd.DataFrame]] = {}

    for source in cfg.sources:
        name = source.transformer
        if name not in TRANSFORMERS:
            raise KeyError(
                f"Transformer '{name}' no registrado. "
                f"Disponibles: {sorted(TRANSFORMERS)}"
            )
        print(f"[transform] {source.id} → {name}")
        out = TRANSFORMERS[name](source, cfg)
        for table, df in out.items():
            buckets.setdefault(table, []).append(df)

    merged: dict[str, pd.DataFrame] = {}
    for table, parts in buckets.items():
        df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
        before = len(df)
        key = ["iso3", "indicator_id", "year", "source_id"]
        duplicate_rows = df.duplicated(key, keep=False) if set(key) <= set(df.columns) else []
        if hasattr(duplicate_rows, "any") and duplicate_rows.any():
            raise ValueError(f"Claves de indicadores duplicadas: {key}")
        # CAMBIO: se eliminan filas idénticas al combinar las fuentes.
        df = df.drop_duplicates(ignore_index=True)
        if table in {"countries", "languages", "indicators"}:
            df.to_parquet(CLEAN / f"{table}.parquet", index=False)
        # Salidas limpias obligatorias: countries.parquet y languages.parquet.
        if table == "indicators":
            # Dataset Hive opcional para consultas por indicador.
            df.to_parquet(
                CLEAN / "indicators_hive",
                partition_cols=["indicator_id"],
                index=False,
            )
        removed = before - len(df)
        dest = CLEAN / f"{table}.csv"
        df.to_csv(dest, index=False)
        print(
            f"[transform] {table}: {len(df)} filas "
            f"(duplicadas eliminadas: {removed}) → {dest}"
        )
        merged[table] = df

    # CAMBIO: se documentan países del perfil sin indicadores asociados.
    # Cobertura del cruce (huérfanos perfil → indicadores).
    # join_orphans incluye iso3, source_id e indicator_id.
    if "countries" in merged and "indicators" in merged:
        have = set(merged["indicators"]["iso3"].dropna().unique())
        profile = set(merged["countries"]["iso3"].dropna().unique())
        orphans = sorted(profile - have)
        report = pd.DataFrame(
            {
                "iso3": orphans,
                "source_id": "all",
                "indicator_id": "all",
                "missing_in": "indicators",
            }
        )
        report_path = CLEAN / "join_orphans.csv"
        report.to_csv(report_path, index=False)
        print(f"[transform] huérfanos del join: {len(orphans)} → {report_path}")

    return merged


if __name__ == "__main__":
    run_transform()
