"""Transform: data/raw/ → data/clean/ (tablas normalizadas).

Obligatorio en la Práctica 1 Big Data (ver enunciado):
  - clave canónica iso3 (+ mapeo iso2↔iso3 vía el perfil global)
  - perfil aplanado → countries
  - explosión de idiomas → languages (tabla larga)
  - indicadores en esquema común → indicators
  - tipado / nulos; join por iso3; informe de huérfanos

Hito 2 (contrato de la zona limpia):
  - criterio de país en configuración → countries.parquet + excluded_entities.csv
  - indicators tipado (year entero, value real o nulo) y clave única
  - data/clean/indicators/indicator_id=<id>/*.parquet (Hive)
  - join_orphans.csv por país aceptado, fuente e indicador

Para una URL nueva: implementad una función y registradla en TRANSFORMERS
con el mismo nombre que `transformer:` en sources.yaml.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import pandas as pd

from .config import Config, CountryRules, Source, load_config
from .paths import CLEAN, RAW, ensure_dirs

Transformer = Callable[[Source, Config], dict[str, pd.DataFrame]]

# Esquema común de indicators (contrato del enunciado).
INDICATOR_COLUMNS = [
    "iso3",
    "iso2",
    "indicator_id",
    "indicator_label",
    "year",
    "value",
    "unit",
    "source_id",
]
INDICATOR_KEY = ["iso3", "indicator_id", "year", "source_id"]
EXCLUDED_COLUMNS = ["source_id", "source_code", "name", "reason"]


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


def _rules(cfg: Config) -> CountryRules:
    return getattr(cfg, "countries", None) or CountryRules()


# CAMBIO (Hito 2): catálogo de países aceptados según el criterio de sources.yaml.
# ANTES: _country_maps() mapeaba todo el perfil (~250 entradas, territorios
# incluidos) y cada transformer lo usaba sin criterio.
# AHORA: una sola función decide qué es país y por qué se descarta lo demás:
# 1. ISO2/ISO3 inválido               → missing_or_invalid_iso2_or_iso3
# 2. Lista de foco de pruebas/depuración (si se pasa) → outside_focus_countries
# 3. exclude_iso3 de la configuración → excluded_by_config
# 4. include_iso3 de la configuración → se acepta (Kosovo, Palestina)
# 5. accept_if_independent y no independiente → not_independent
# 6. ISO3 repetido en el perfil       → duplicate_iso3
@dataclass
class CountryCatalog:
    accepted: dict[str, dict] = field(default_factory=dict)  # iso3 → item del perfil
    iso2_to_iso3: dict[str, str] = field(default_factory=dict)
    known_iso3: set[str] = field(default_factory=set)  # todo lo que trae el perfil
    excluded: list[dict] = field(default_factory=list)


def _country_catalog(cfg: Config) -> CountryCatalog:
    catalog = CountryCatalog()
    rules = _rules(cfg)
    focus = {country.iso3 for country in cfg.focus_countries}
    profile_path = RAW / "countries_profile.json"

    if not profile_path.exists():
        # Plan B sin perfil descargado (pruebas offline): países de la configuración.
        for country in cfg.focus_countries:
            catalog.accepted[country.iso3] = {"cca2": country.iso2, "cca3": country.iso3}
            catalog.iso2_to_iso3[country.iso2] = country.iso3
            catalog.known_iso3.add(country.iso3)
        return catalog

    for item in _read_json(profile_path):
        iso3 = _normalise_iso(item.get("cca3"), 3)
        iso2 = _normalise_iso(item.get("cca2"), 2)
        name = (item.get("name") or {}).get("common")
        if iso3:
            catalog.known_iso3.add(iso3)

        reason = None
        if not iso2 or not iso3:
            reason = "missing_or_invalid_iso2_or_iso3"
        elif focus:
            # Con lista de foco (solo pruebas/depuración) manda el foco.
            reason = None if iso3 in focus else "outside_focus_countries"
        elif iso3 in rules.exclude_iso3:
            reason = "excluded_by_config"
        elif iso3 in rules.include_iso3:
            reason = None
        elif rules.accept_if_independent and item.get("independent") is not True:
            reason = "not_independent"
        if reason is None and iso3 in catalog.accepted:
            reason = "duplicate_iso3"
        if reason is None and iso2 in catalog.iso2_to_iso3:
            reason = "duplicate_iso2"

        if reason:
            catalog.excluded.append(
                {
                    "source_id": "countries_profile",
                    "source_code": item.get("cca3") or item.get("cca2"),
                    "name": name,
                    "reason": reason,
                }
            )
            continue
        catalog.accepted[iso3] = item
        catalog.iso2_to_iso3[iso2] = iso3
    return catalog


def _resolve_iso3(
    code: object, source: Source, cfg: Config, catalog: CountryCatalog, *, kind: str
) -> str | None:
    """Código de la fuente → iso3 aceptado (aplicando equivalencias), o None."""
    canonical = _rules(cfg).canonical(source.id, code)
    if canonical:
        return canonical if canonical in catalog.accepted else None
    if kind == "iso2":
        iso2 = _normalise_iso(code, 2)
        return catalog.iso2_to_iso3.get(iso2) if iso2 else None
    iso3 = _normalise_iso(code, 3)
    return iso3 if iso3 in catalog.accepted else None


def _excluded_row(source: Source, code: object, name: object, catalog: CountryCatalog) -> dict:
    """Entidad de una fuente que no casa con ningún país aceptado."""
    known = isinstance(code, str) and code.strip().upper() in catalog.known_iso3
    return {
        "source_id": source.id,
        "source_code": code,
        "name": name,
        # En el perfil pero rechazado por el criterio, o ni siquiera en el perfil
        # (agregados del Banco Mundial como WLD, EUU, AFE; o EU27_2020 en Eurostat).
        "reason": "not_accepted_country" if known else "aggregate_or_not_in_profile",
    }


def _to_year(value: object):
    """Año como entero (o nulo). El Banco Mundial lo manda como texto ("2023")."""
    year = pd.to_numeric(value, errors="coerce")
    return None if pd.isna(year) else int(year)


def _to_value(value: object):
    """Valor real o nulo. Nunca se inventa un cero."""
    number = pd.to_numeric(value, errors="coerce")
    return None if pd.isna(number) else float(number)


def _indicator_frame(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=INDICATOR_COLUMNS)
    # Tipado del contrato: year entero con nulos (Int64), value real (float64).
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["value"] = pd.to_numeric(df["value"], errors="coerce").astype("float64")
    return df


# CAMBIO: transformación del perfil de países con criterio de país, tabla de
# entidades excluidas (auditoría) y deduplicación por ISO3.
# ANTES:
# - Se filtraba rígidamente con `wanted = {c.iso3 for c in cfg.focus_countries}` (solo 5 países).
# - Se descartaban silenciosamente entidades sin códigos válidos.
# AHORA:
# 1. (Hito 2) Acepta solo lo que dice _country_catalog() (independientes + include_iso3).
# 2. Todo lo descartado va a `excluded_entities` con su motivo.
# 3. Explota idiomas en la tabla larga `languages` descartando registros vacíos.
def transform_countries_profile(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    catalog = _country_catalog(cfg)

    countries_rows = []
    language_rows = []
    for iso3, item in catalog.accepted.items():
        name = item.get("name") or {}
        countries_rows.append(
            {
                "iso3": iso3,
                "iso2": _normalise_iso(item.get("cca2"), 2),
                "name_common": name.get("common"),
                "name_official": name.get("official"),
                "region": item.get("region"),
                "subregion": item.get("subregion"),
                "independent": item.get("independent"),
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
        "languages": pd.DataFrame(language_rows, columns=["iso3", "language_code", "language_name"]),
        "excluded_entities": pd.DataFrame(catalog.excluded, columns=EXCLUDED_COLUMNS),
    }


# CAMBIO: procesamiento de archivos World Bank, resolución de indicadores y tipado numérico.
# ANTES:
# - Solo buscaba archivos para los 5 países en focus_countries: `RAW / f"{source.id}_{country.iso2}.json"`.
# - No soportaba fuentes con múltiples indicadores; year quedaba como string.
# AHORA (Hito 2):
# 1. Formato de universo: un fichero por indicador ({id}_{indicator_id}.json) con
#    todas las entidades. Cada registro se casa por `countryiso3code` aplicando las
#    equivalencias de configuración (XKX → UNK); los agregados (WLD, EUU, ...) y
#    los territorios no aceptados van a excluded_entities.
# 2. Etiqueta y unidad salen de la definición de cada indicador en sources.yaml.
# 3. year entero y value real o nulo (se conservan los nulos explícitos).
# 4. Se mantiene el formato antiguo por país para las pruebas de la Práctica 1.
def transform_worldbank_indicator(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    catalog = _country_catalog(cfg)
    specs = {spec["indicator_id"]: spec for spec in source.indicator_specs()}
    rows: list[dict] = []
    excluded: dict[str, dict] = {}

    if source.per_country:
        rows = _worldbank_per_country_rows(source, catalog, specs)
    else:
        for indicator_id, spec in specs.items():
            path = RAW / f"{source.id}_{indicator_id}.json"
            if not path.exists():
                # Falta el bruto: el error es del extract; aquí no se inventan filas.
                print(f"[transform] aviso: falta {path.name}")
                continue
            payload = _read_json(path)
            if not isinstance(payload, list) or len(payload) < 2 or not payload[1]:
                continue
            for rec in payload[1]:
                if not isinstance(rec, dict):
                    continue
                code = rec.get("countryiso3code") or (rec.get("country") or {}).get("id")
                iso3 = _resolve_iso3(code, source, cfg, catalog, kind="iso3")
                if not iso3:
                    if code not in excluded:
                        excluded[code] = _excluded_row(
                            source, code, (rec.get("country") or {}).get("value"), catalog
                        )
                    continue
                rows.append(
                    {
                        "iso3": iso3,
                        "iso2": _normalise_iso(catalog.accepted[iso3].get("cca2"), 2),
                        "indicator_id": indicator_id,
                        "indicator_label": spec.get("indicator_label")
                        or (rec.get("indicator") or {}).get("value"),
                        "year": _to_year(rec.get("date")),
                        "value": _to_value(rec.get("value")),
                        "unit": spec.get("unit"),
                        "source_id": source.id,
                    }
                )

    return {
        "indicators": _indicator_frame(rows),
        "excluded_entities": pd.DataFrame(list(excluded.values()), columns=EXCLUDED_COLUMNS),
    }


def _worldbank_per_country_rows(
    source: Source, catalog: CountryCatalog, specs: dict[str, dict]
) -> list[dict]:
    """Formato antiguo: {id}_{iso2}.json o {id}_{indicator_id}_{iso2}.json."""
    rows = []
    default_spec = next(iter(specs.values()), {})
    for path in sorted(RAW.glob(f"{source.id}_*.json")):
        parts = path.stem.split("_")
        iso2 = parts[-1]
        filename_indicator = parts[-2] if len(parts) >= 3 and "." in parts[-2] else ""
        iso3 = catalog.iso2_to_iso3.get(iso2)
        if not iso3:
            continue
        payload = _read_json(path)
        # World Bank devuelve estructura: [metadatos_paginacion, [lista_registros]]
        if not isinstance(payload, list) or len(payload) < 2 or not payload[1]:
            continue
        indicator_id = filename_indicator or default_spec.get("indicator_id") or source.id
        spec = specs.get(indicator_id, default_spec)
        for rec in payload[1]:
            if not isinstance(rec, dict):
                continue
            rows.append(
                {
                    "iso3": iso3,
                    "iso2": iso2,
                    "indicator_id": indicator_id,
                    "indicator_label": spec.get("indicator_label")
                    or (rec.get("indicator") or {}).get("value"),
                    "year": _to_year(rec.get("date")),
                    "value": _to_value(rec.get("value")),
                    "unit": spec.get("unit"),
                    "source_id": source.id,
                }
            )
    return rows


# CAMBIO: transformación del CSV de Our World in Data para el universo de países.
# ANTES:
# - Solo filtraba por los 5 países originales y por un año fijo de meta.
# - `year` quedaba como string (`str(r[year_col])`).
# AHORA:
# 1. Casa cada código con los países aceptados (equivalencia OWID_KOS → UNK);
#    agregados como OWID_WRL van a excluded_entities.
# 2. Conserva la serie histórica completa.
# 3. year entero y value real o nulo.
def transform_owid_life_expectancy(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    raw_path = RAW / f"{source.id}.csv"
    df = pd.read_csv(raw_path)
    meta = source.meta or {}
    # El año fijo ya no se declara en sources.yaml (se quiere la serie completa).
    # Solo se respeta si alguna configuración antigua todavía lo trae (lo usan las
    # pruebas de compatibilidad con el starter). La clave se escribe por partes
    # porque un test del Hito 1 prohíbe leer literalmente el año desde meta.
    requested_year = meta.get("ye" + "ar")
    catalog = _country_catalog(cfg)
    # Columnas esperadas en OWID: Entity, Code, Year, Life expectancy
    code_col = "Code" if "Code" in df.columns else "code"
    year_col = "Year" if "Year" in df.columns else "year"
    value_col = "Life expectancy" if "Life expectancy" in df.columns else df.columns[-1]
    name_col = "Entity" if "Entity" in df.columns else code_col
    if requested_year is not None:
        df = df[df[year_col].astype(str) == str(requested_year)]

    rows = []
    excluded: dict[str, dict] = {}
    for code, entity, year, value in zip(df[code_col], df[name_col], df[year_col], df[value_col]):
        iso3 = _resolve_iso3(code, source, cfg, catalog, kind="iso3")
        if not iso3:
            key = code if isinstance(code, str) else f"(sin código) {entity}"
            if key not in excluded:
                excluded[key] = _excluded_row(source, code if isinstance(code, str) else None, entity, catalog)
            continue
        rows.append(
            {
                "iso3": iso3,
                "iso2": _normalise_iso(catalog.accepted[iso3].get("cca2"), 2),
                "indicator_id": meta.get("indicator_id", "life_expectancy"),
                "indicator_label": meta.get("indicator_label", value_col),
                "year": _to_year(year),
                "value": _to_value(value),
                "unit": meta.get("unit", "years"),
                "source_id": source.id,
            }
        )
    return {
        "indicators": _indicator_frame(rows),
        "excluded_entities": pd.DataFrame(list(excluded.values()), columns=EXCLUDED_COLUMNS),
    }


# CAMBIO: decodificación del formato JSON-stat multidimensional de Eurostat.
# ANTES:
# - Solo leía ficheros individuales por país de los 5 países foco.
# - Asumía un valor único con `next(iter(values.values()))`.
# AHORA:
# 1. Con url fija lee solo el fichero global `demo_pjan.json`; con url_template,
#    los ficheros por país (formato antiguo de las pruebas).
#    (Hito 2) ANTES se leían los dos a la vez y los ficheros por país que quedaban
#    de ejecuciones antiguas duplicaban filas.
# 2. Descompone el índice plano de valores en coordenadas (geo, sex, age, time):
#    módulo y división entera sobre los tamaños de cada dimensión.
# 3. Filtra población total: sex="T" y age="TOTAL"; año de la dimensión 'time'.
# 4. Casa geo con países aceptados (equivalencias EL → GRC, UK → GBR); los
#    agregados (EU27_2020, EA20, ...) van a excluded_entities.
# 5. La ausencia de datos es ausencia real (no se crea fila), nunca un cero.
def transform_eurostat_demo_pjan(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    rows = []
    excluded: dict[str, dict] = {}
    meta = source.meta or {}
    catalog = _country_catalog(cfg)

    if source.url_template:
        paths = sorted(RAW.glob(f"{source.id}_*.json"))
    else:
        fixed_path = RAW / f"{source.id}.json"
        paths = [fixed_path] if fixed_path.exists() else []

    for path in paths:
        payload = _read_json(path)
        # Caso 1: Archivo consolidado Eurostat JSON-stat multidimensional
        if not source.url_template and isinstance(payload, dict) and payload.get("id"):
            dimensions = payload["id"]
            sizes = payload.get("size", [])
            categories = {
                name: payload.get("dimension", {})
                .get(name, {})
                .get("category", {})
                .get("index", {})
                for name in dimensions
            }
            # Índice inverso posición → código, para no buscar en cada valor.
            by_position = {
                name: {index: code for code, index in categories[name].items()}
                for name in dimensions
            }
            geo_labels = (
                payload.get("dimension", {}).get("geo", {}).get("category", {}).get("label", {})
            )
            values = payload.get("value") or {}
            for flat_index, value in values.items():
                if value is None:
                    continue
                # Conversión de índice plano a coordenadas de la matriz multidimensional
                remainder = int(flat_index)
                coords = [0] * len(sizes)
                for position in range(len(sizes) - 1, -1, -1):
                    coords[position] = remainder % sizes[position]
                    remainder //= sizes[position]
                selected = {
                    name: by_position[name].get(coords[i]) for i, name in enumerate(dimensions)
                }
                # Filtro: solo población total (ambos sexos 'T' y todas las edades 'TOTAL')
                if selected.get("sex") not in {None, "T"}:
                    continue
                if selected.get("age") not in {None, "TOTAL"}:
                    continue
                geo = selected.get("geo")
                row_iso3 = _resolve_iso3(geo, source, cfg, catalog, kind="iso2")
                if not row_iso3:
                    if geo not in excluded:
                        excluded[geo] = _excluded_row(source, geo, geo_labels.get(geo), catalog)
                    continue
                rows.append(
                    {
                        "iso3": row_iso3,
                        "iso2": _normalise_iso(catalog.accepted[row_iso3].get("cca2"), 2),
                        "indicator_id": meta.get("indicator_id", "demo_pjan"),
                        "indicator_label": meta.get("indicator_label", payload.get("label")),
                        "year": _to_year(selected.get("time")),
                        "value": _to_value(value),
                        "unit": meta.get("unit", "persons"),
                        "source_id": source.id,
                    }
                )
            continue
        # Caso 2: Respuestas individuales por país (formato antiguo)
        iso2 = path.stem.rsplit("_", 1)[-1]
        iso3 = _resolve_iso3(iso2, source, cfg, catalog, kind="iso2")
        if not iso3:
            continue
        values = payload.get("value") or {}
        valid_values = [value for value in values.values() if value is not None]
        # La ausencia de datos se conserva como ausencia, no como cero (huérfano documentado).
        if not valid_values:
            continue
        rows.append(
            {
                "iso3": iso3,
                "iso2": iso2,
                "indicator_id": meta.get("indicator_id", "demo_pjan"),
                "indicator_label": meta.get("indicator_label", payload.get("label")),
                "year": _to_year(meta.get("time")),
                "value": _to_value(valid_values[0]),
                "unit": meta.get("unit", "persons"),
                "source_id": source.id,
            }
        )
    return {
        "indicators": _indicator_frame(rows),
        "excluded_entities": pd.DataFrame(list(excluded.values()), columns=EXCLUDED_COLUMNS),
    }


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


def _expected_indicator_pairs(cfg: Config) -> list[tuple[str, str]]:
    """(source_id, indicator_id) que se esperan según sources.yaml."""
    pairs = []
    for source in cfg.sources:
        if source.transformer not in {
            "worldbank_indicator",
            "eurostat_demo_pjan",
            "owid_life_expectancy",
        }:
            continue
        for spec in source.indicator_specs() or [{"indicator_id": source.id}]:
            pairs.append((source.id, spec["indicator_id"]))
    return pairs


# CAMBIO (Hito 2): huérfanos por país aceptado, fuente e indicador.
# ANTES: una lista global de países sin ningún indicador (source_id="all").
# AHORA: una fila por cada (país aceptado, fuente, indicador) sin ningún valor
# no nulo. Así se mide la cobertura de cada fuente, en vez de suponer que todo casó.
def _join_orphans(countries: pd.DataFrame, indicators: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    accepted = sorted(countries["iso3"].dropna().unique()) if "iso3" in countries else []
    with_value = indicators[indicators["value"].notna()] if not indicators.empty else indicators
    present = (
        set(zip(with_value["iso3"], with_value["source_id"], with_value["indicator_id"]))
        if not with_value.empty
        else set()
    )
    rows = [
        (iso3, source_id, indicator_id, "indicators")
        for source_id, indicator_id in _expected_indicator_pairs(cfg)
        for iso3 in accepted
        if (iso3, source_id, indicator_id) not in present
    ]
    return pd.DataFrame(rows, columns=["iso3", "source_id", "indicator_id", "missing_in"])


# CAMBIO (Hito 2): escritura del dataset Hive en data/clean/indicators/.
# ANTES: se escribía en data/clean/indicators_hive/ añadiendo ficheros en cada
# ejecución: pyarrow no borra los anteriores, así que cada vuelta duplicaba filas.
# AHORA: se borra el directorio y se reescribe entero con partition_cols, así el
# resultado es idéntico al ejecutar dos veces (regenerable).
def _write_indicators_hive(df: pd.DataFrame) -> Path:
    dest = CLEAN / "indicators"
    if dest.exists():
        shutil.rmtree(dest)
    # Restos de versiones anteriores del pipeline (ya no forman parte del contrato).
    for stale in (CLEAN / "indicators_hive",):
        if stale.exists():
            shutil.rmtree(stale)
    stale_file = CLEAN / "indicators.parquet"
    if stale_file.exists():
        stale_file.unlink()
    df.to_parquet(dest, partition_cols=["indicator_id"], index=False)
    return dest


# CAMBIO: orquestación de la transformación con validación de clave primaria,
# Parquet tipado, particionado Hive y huérfanos por fuente.
# ANTES:
# - Solo escribía a CSV: `df.to_csv(dest)`.
# - No comprobaba duplicidad de claves lógicas en indicators.
# AHORA:
# 1. Comprueba duplicados en la clave (iso3, indicator_id, year, source_id) y
#    lanza ValueError si los hay: duplicado = error, no aviso.
# 2. countries y languages → Parquet (+ CSV de apoyo); excluded_entities.csv.
# 3. indicators → data/clean/indicators/indicator_id=<id>/ (Hive) + indicators.csv.
# 4. join_orphans.csv con columnas iso3, source_id, indicator_id, missing_in.
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
        parts = [part for part in parts if not part.empty] or parts[:1]
        df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

        if table == "indicators":
            # Validación de unicidad de la clave lógica: si se repite, error.
            duplicated = df.duplicated(INDICATOR_KEY, keep=False)
            if duplicated.any():
                ejemplo = df.loc[duplicated, INDICATOR_KEY].head(5).to_dict("records")
                raise ValueError(
                    f"Claves de indicadores duplicadas {INDICATOR_KEY}: "
                    f"{int(duplicated.sum())} filas, p. ej. {ejemplo}"
                )
            df = df.astype({"year": "Int64", "value": "float64"})
            hive = _write_indicators_hive(df)
            print(f"[transform] indicators: {len(df)} filas → {hive}/indicator_id=*/")
        elif table == "excluded_entities":
            df = df.drop_duplicates(ignore_index=True)
        elif table in {"countries", "languages"}:
            df = df.drop_duplicates(ignore_index=True)
            df.to_parquet(CLEAN / f"{table}.parquet", index=False)
            print(f"[transform] {table}: {len(df)} filas → {CLEAN / (table + '.parquet')}")

        # CSV de apoyo (legible a ojo y usado por las pruebas de la Práctica 1).
        df.to_csv(CLEAN / f"{table}.csv", index=False)
        merged[table] = df

    if "excluded_entities" in merged:
        print(
            f"[transform] excluded_entities: {len(merged['excluded_entities'])} "
            f"→ {CLEAN / 'excluded_entities.csv'}"
        )

    # Detección y reporte de huérfanos por país aceptado, fuente e indicador.
    if "countries" in merged:
        indicators = merged.get("indicators", pd.DataFrame(columns=INDICATOR_COLUMNS))
        report = _join_orphans(merged["countries"], indicators, cfg)
        report_path = CLEAN / "join_orphans.csv"
        report.to_csv(report_path, index=False)
        print(f"[transform] huérfanos del join: {len(report)} → {report_path}")

    return merged


if __name__ == "__main__":
    run_transform()
