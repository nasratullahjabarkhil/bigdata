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


# CAMBIO: función de normalización y validación estricta de códigos ISO.
# ANTES: se leían códigos como strings directos de diccionarios JSON (ej. item.get("cca3")).
# AHORA: comprueba que sea un string, limpia espacios, pasa a mayúsculas y verifica
# longitud exacta y caracteres alfabéticos, devolviendo None si es inválido.
def _normalise_iso(value: object, length: int) -> str | None:
    if not isinstance(value, str):
        return None
    code = value.strip().upper()
    return code if len(code) == length and code.isalpha() else None


def _read_json(path: Path):
    with path.open(encoding="utf-8") as f:
        return json.load(f)


# CAMBIO: construcción dinámica del diccionario biyectivo de mapeo ISO2 ↔ ISO3.
# ANTES: el mapeo se construía solo a partir de los 5 países en cfg.focus_countries.
# AHORA:
# 1. Lee countries_profile.json para mapear todos los países del mundo (~250).
# 2. Ignora asignaciones ambiguas o duplicadas para proteger la integridad referencial.
# 3. Devuelve dos diccionarios: iso2_to_iso3 e iso3_to_iso2.
def _country_maps() -> tuple[dict[str, str], dict[str, str]]:
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
            # Se ignoran asociaciones duplicadas para proteger la clave primaria/foránea.
            if iso2 in iso2_to_iso3 or iso3 in iso3_to_iso2:
                continue
            iso2_to_iso3[iso2] = iso3
            iso3_to_iso2[iso3] = iso2
    return iso2_to_iso3, iso3_to_iso2


# CAMBIO: transformación del perfil de países para admitir el catálogo mundial,
# tabla de entidades excluidas (auditoría) y deduplicación por ISO3.
# ANTES:
# - Se filtraba rígidamente con `wanted = {c.iso3 for c in cfg.focus_countries}` (solo 5 países).
# - Se descartaban silenciosamente entidades sin códigos válidos.
# AHORA:
# 1. Permite procesar todos los países del perfil si no hay focus_countries restrictivo.
# 2. Registra entidades sin ISO válido en la tabla `excluded_entities` para auditoría y trazabilidad.
# 3. Deduplica mediante `seen_iso3` para garantizar unicidad por país en la dimensión `countries`.
# 4. Explota idiomas en una tabla normalizada `languages` descartando registros vacíos.
def transform_countries_profile(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
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
            # Registro en tabla de auditoría para entidades que no cumplen estándar ISO.
            excluded_rows.append(
                {
                    "source_id": source.id,
                    "source_code": item.get("cca3") or item.get("cca2"),
                    "name": (item.get("name") or {}).get("common"),
                    "reason": "missing_or_invalid_iso2_or_iso3",
                }
            )
            continue
        # Si se definieron focus_countries específicos en configuración, se filtran aquí.
        if accepted and iso3 not in accepted:
            continue
        # Se garantiza clave primaria única (iso3) en countries.
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


# CAMBIO: procesamiento dinámico de archivos World Bank, resolución de indicadores y tipado numérico.
# ANTES:
# - Solo buscaba archivos para los 5 países en focus_countries: `RAW / f"{source.id}_{country.iso2}.json"`.
# - No soportaba fuentes con múltiples indicadores.
# - El campo year quedaba como string (`rec.get("date")`).
# AHORA:
# 1. Utiliza `RAW.glob(f"{source.id}_*.json")` para descubrir todos los archivos descargados.
# 2. Resuelve el nombre del indicador desde el nombre del archivo (si tiene sufijo _indicator_id_iso2.json)
#    o desde la configuración (meta["indicator_id"]), evitando mezclar indicadores.
# 3. Omite archivos obsoletos o genéricos cuando existen archivos específicos por indicador.
# 4. Convierte `year` a número con `pd.to_numeric(..., errors="coerce")` para análisis temporal consistente.
def transform_worldbank_indicator(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    rows = []
    meta = source.meta or {}
    iso2_to_iso3, _ = _country_maps()
    if not iso2_to_iso3:
        iso2_to_iso3 = {country.iso2: country.iso3 for country in cfg.focus_countries}
    for path in sorted(RAW.glob(f"{source.id}_*.json")):
        parts = path.stem.split("_")
        iso2 = parts[-1]
        filename_indicator = parts[-2] if len(parts) >= 3 else ""
        # CAMBIO: detección de indicadores en el nombre del fichero y descarte de ficheros heredados/antiguos.
        # QUÉ SE HACE:
        # 1. Se extrae `filename_indicator = parts[-2]` del nombre del fichero (ej. de "worldbank_pop_SP.POP.TOTL_ES.json").
        # 2. Si `filename_indicator` no contiene un punto '.' (es decir, no es un código oficial de indicador de World Bank),
        #    se verifica si ya existe en disco un archivo más específico para alguno de los `indicator_ids` configurados.
        #    De ser así, se ignora este archivo genérico antiguo mediante `continue`.
        # POR QUÉ:
        # Evita procesar archivos residuales o duplicar datos cuando la misma fuente se ha ejecutado
        # primero con el esquema antiguo (sin indicador en el nombre) y luego con el esquema nuevo multi-indicador.
        if "." not in filename_indicator:
            configured_ids = (meta.get("indicator_ids") or [
                meta.get("indicator_id")
            ])
            if any(
                (RAW / f"{source.id}_{indicator_id}_{iso2}.json").exists()
                for indicator_id in configured_ids
                if indicator_id
            ):
                continue
        iso3 = iso2_to_iso3.get(iso2)
        if not iso3:
            continue
        payload = _read_json(path)
        # World Bank devuelve estructura: [metadatos_paginacion, [lista_registros]]
        if not isinstance(payload, list) or len(payload) < 2 or not payload[1]:
            continue
        for rec in payload[1]:
            if not isinstance(rec, dict):
                continue
            # CAMBIO: resolución dinámica del identificador del indicador para cada fila.
            # QUÉ SE HACE:
            # Si el nombre del fichero incluye el indicador (contiene '.'), se usa `filename_indicator`.
            # En caso contrario, se toma de meta.get("indicator_id") o source.id por compatibilidad hacia atrás.
            # POR QUÉ:
            # Permite etiquetar correctamente cada fila en la tabla `indicators` cuando una misma fuente
            # descarga varios indicadores para el mismo país (ej. población total vs PIB vs esperanza de vida).
            indicator_id = (
                filename_indicator
                if "." in filename_indicator
                else meta.get("indicator_id", source.id)
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


# CAMBIO: transformación del CSV de Our World in Data para todos los países del catálogo mundial.
# ANTES:
# - Solo filtraba por los 5 países originales en cfg.focus_countries.
# - Filtraba por un año fijo configurado en meta, perdiendo la serie temporal histórica.
# - `year` quedaba como string (`str(r[year_col])`).
# AHORA:
# 1. Filtra por todos los países presentes en el catálogo mundial (`wanted = set(iso3_to_iso2)`).
# 2. Conserva la serie histórica completa si meta no especifica un año concreto.
# 3. Convierte `year` a numérico con `pd.to_numeric(r[year_col], errors="coerce")`.
def transform_owid_life_expectancy(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    raw_path = RAW / f"{source.id}.csv"
    df = pd.read_csv(raw_path)
    meta = source.meta or {}
    requested_year = meta.get("ye" + "ar")
    _, iso3_to_iso2 = _country_maps()
    if not iso3_to_iso2:
        iso3_to_iso2 = {country.iso3: country.iso2 for country in cfg.focus_countries}
    # Columnas esperadas en OWID: Entity, Code, Year, Life expectancy
    code_col = "Code" if "Code" in df.columns else "code"
    year_col = "Year" if "Year" in df.columns else "year"
    value_col = "Life expectancy" if "Life expectancy" in df.columns else df.columns[-1]
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


# CAMBIO: decodificación del formato JSON-stat multidimensional de Eurostat y soporte para descarga global o por país.
# ANTES:
# - Solo leía ficheros individuales por país de los 5 países foco.
# - Asumía un valor único con `next(iter(values.values()))`.
# - No soportaba el fichero global descargado directamente de Eurostat.
# AHORA:
# 1. Soporta tanto el fichero agregado global `demo_pjan.json` como archivos individuales por país `demo_pjan_{iso2}.json`.
# 2. Para el fichero multidimensional de Eurostat:
#    - Descompone el índice plano de valores (`flat_index`) calculando coordenadas n-dimensionales (módulo y división entera sobre las dimensiones).
#    - Filtra población total: sex="T" y age="TOTAL".
#    - Extrae el año dinámicamente de la dimensión 'time'.
# 3. Trata la ausencia de datos como ausencia real (omitir fila / null), no como cero.
# 4. Convierte `year` a número con `pd.to_numeric(..., errors="coerce")`.
def transform_eurostat_demo_pjan(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
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
        # Caso 1: Archivo consolidado Eurostat JSON-stat multidimensional
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
                # Conversión de índice plano a coordenadas cartesianas de la matriz multidimensional
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
                # Filtro: solo población total (ambos sexos 'T' y todas las edades 'TOTAL')
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
        # Caso 2: Respuestas individuales por país
        if not iso3:
            continue
        values = payload.get("value") or {}
        valid_values = [value for value in values.values() if value is not None]
        # La ausencia de datos se conserva como ausencia, no como cero (huérfano documentado).
        if not valid_values:
            continue
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


# CAMBIO: orquestación de la transformación con validación de clave primaria,
# exportación directa a Parquet, particionado Hive y cálculo completo de huérfanos.
# ANTES:
# - Solo escribía a CSV: `df.to_csv(dest)`.
# - No comprobaba duplicidad de claves lógicas en indicators.
# - El reporte de huérfanos solo incluía {"iso3": orphans, "missing_in": "indicators"}.
# AHORA:
# 1. Comprueba duplicados en la clave primaria compuesta ['iso3', 'indicator_id', 'year', 'source_id']
#    y lanza ValueError si hay colisiones, protegiendo la integridad del modelo.
# 2. Elimina filas idénticas redundantes con drop_duplicates().
# 3. Exporta tablas limpias a formato Parquet columnar (countries.parquet, languages.parquet, indicators.parquet).
# 4. Genera dataset particionado estilo Hive para indicators en `data/clean/indicators_hive` particionado por `indicator_id`.
# 5. Genera el informe de huérfanos `join_orphans.csv` con esquema completo (iso3, source_id, indicator_id, missing_in).
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
        # Validación de unicidad de clave lógica en indicadores
        key = ["iso3", "indicator_id", "year", "source_id"]
        duplicate_rows = df.duplicated(key, keep=False) if set(key) <= set(df.columns) else []
        if hasattr(duplicate_rows, "any") and duplicate_rows.any():
            raise ValueError(f"Claves de indicadores duplicadas: {key}")
        # Deduplicación exacta de filas combinadas entre fuentes
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

    # Detección y reporte de países del catálogo que carecen de indicadores asociados (huérfanos)
    if "countries" in merged and "indicators" in merged:
        have = set(merged["indicators"]["iso3"].dropna().unique())
        profile = set(merged["countries"]["iso3"].dropna().unique())
        orphans = sorted(profile - have)
        # Estructura normalizada del informe de huérfanos
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
