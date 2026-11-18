"""Genera fichas Markdown desde data/serving/ (nunca desde raw/)."""

from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

import pandas as pd

from .paths import CARDS, SERVING, ensure_dirs

DB_PATH = SERVING / "observatorio.sqlite"


def _read_table(conn: sqlite3.Connection, name: str) -> pd.DataFrame:
    try:
        return pd.read_sql_query(f"SELECT * FROM {name}", conn)
    except Exception:
        return pd.DataFrame()


def _fmt(value: float) -> str:
    return f"{value:,.2f}"


# CAMBIO (Hito 2): tabla de medianas por (región, indicador, fuente, año).
# ANTES: la mediana se recalculaba dentro de cada ficha.
# AHORA: se calcula una sola vez para todas las fichas (≈200 países × toda la serie).
# Solo entran valores no nulos de países aceptados (countries ya no tiene agregados,
# así que "European Union" o "World" no mueven la mediana de Europa).
def _regional_medians(countries: pd.DataFrame, indicators: pd.DataFrame) -> pd.DataFrame:
    if indicators.empty or "region" not in countries.columns:
        return pd.DataFrame(columns=["region", "indicator_id", "source_id", "year", "median"])
    with_value = indicators[indicators["value"].notna()].merge(
        countries[["iso3", "region"]], on="iso3", how="inner"
    )
    return (
        with_value.groupby(["region", "indicator_id", "source_id", "year"])["value"]
        .median()
        .rename("median")
        .reset_index()
    )


def _card_markdown(iso3: str, countries: pd.DataFrame, languages: pd.DataFrame,
                   indicators: pd.DataFrame, medians: pd.DataFrame | None = None) -> str:
    c = countries[countries["iso3"] == iso3].iloc[0]
    langs = languages[languages["iso3"] == iso3] if not languages.empty else languages
    inds = indicators[indicators["iso3"] == iso3] if not indicators.empty else indicators
    if medians is None:
        medians = _regional_medians(countries, indicators)

    lang_lines = (
        "\n".join(
            f"- `{r.language_code}`: {r.language_name}" for _, r in langs.iterrows()
        )
        or "- (sin idiomas en clean/)"
    )

    # CAMBIO (Hito 2): un bloque por indicador con su último año con dato.
    # ANTES: se listaban todas las filas del país (con la serie completa eran
    # cientos de líneas) y el comparativo usaba solo el primer indicador.
    # AHORA, por cada (indicador, fuente):
    # 1. Se toma el año más reciente con valor no nulo del país.
    # 2. Se compara con la MEDIANA de su región en el mismo indicador y el mismo year.
    #    POR QUÉ la mediana: es robusta frente a valores extremos (micro-estados,
    #    potencias) y no mezcla años distintos.
    # 3. Si el país solo tiene nulos, la ficha dice que no hay dato (no inventa un cero).
    ind_lines: list[str] = []
    comp_lines: list[str] = []
    region = c.get("region")
    for (ind_id, source_id), group in inds.groupby(["indicator_id", "source_id"], sort=True):
        label = group["indicator_label"].dropna().iloc[0] if group["indicator_label"].notna().any() else ind_id
        unit = group["unit"].dropna().iloc[0] if group["unit"].notna().any() else ""
        with_value = group[group["value"].notna()]
        if with_value.empty:
            years = group["year"].dropna()
            rango = f"{int(years.min())}–{int(years.max())}" if not years.empty else "?"
            ind_lines.append(
                f"- **{label}**: sin dato (todas las filas {rango} son nulas) _(fuente: {source_id})_"
            )
            comp_lines.append(f"- `{ind_id}`: no hay dato para este país en ningún año.")
            continue
        last = with_value.sort_values("year").iloc[-1]
        year = int(last["year"])
        own = float(last["value"])
        n_years = with_value["year"].nunique()
        ind_lines.append(
            f"- **{label}** ({year}): {_fmt(own)} {unit} "
            f"_(fuente: {source_id}; {n_years} años con dato)_"
        )
        peer = medians[
            (medians["region"] == region)
            & (medians["indicator_id"] == ind_id)
            & (medians["source_id"] == source_id)
            & (medians["year"] == year)
        ]
        if peer.empty:
            comp_lines.append(f"- `{ind_id}` ({year}): sin dato regional ese año.")
            continue
        median_v = float(peer.iloc[0]["median"])
        posicion = "por encima" if own > median_v else ("igual" if own == median_v else "por debajo")
        comp_lines.append(
            f"- `{ind_id}` ({year}): {_fmt(own)} frente a la mediana de **{region}** "
            f"({_fmt(median_v)}) → **{posicion}**."
        )

    ind_text = "\n".join(ind_lines) or "- (sin indicadores; posible huérfano del join)"
    comparative = "\n".join(comp_lines) or "_Sin indicadores para comparar._"

    return f"""# Ficha país — {c['name_common']} ({iso3})

- **Nombre oficial:** {c.get('name_official')}
- **ISO2 / ISO3:** {c.get('iso2')} / {iso3}
- **Región / subregión:** {c.get('region')} / {c.get('subregion')}

## Idiomas

{lang_lines}

## Indicadores (último año con dato)

{ind_text}

## Comparativo (mediana de la región, mismo indicador y mismo año)

{comparative}

---
Generado automáticamente el {date.today().isoformat()} desde `data/serving/observatorio.sqlite`.
"""


def run_build_cards() -> list[Path]:
    ensure_dirs()
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"No existe {DB_PATH}. Ejecutad antes extract → transform → load."
        )

    conn = sqlite3.connect(DB_PATH)
    try:
        countries = _read_table(conn, "countries")
        languages = _read_table(conn, "languages")
        indicators = _read_table(conn, "indicators")
    finally:
        conn.close()

    if countries.empty:
        raise RuntimeError("Tabla countries vacía: revisad transform.")

    medians = _regional_medians(countries, indicators)
    accepted = countries["iso3"].tolist()

    # CAMBIO (Hito 2): una ficha por país aceptado y ninguna más.
    # Se borran fichas viejas de entidades que el criterio de país ya no acepta
    # (p. ej. territorios que se aceptaban en el Hito 1).
    for old in CARDS.glob("*.md"):
        if old.stem not in accepted:
            old.unlink()

    written: list[Path] = []
    for iso3 in accepted:
        md = _card_markdown(iso3, countries, languages, indicators, medians)
        path = CARDS / f"{iso3}.md"
        path.write_text(md, encoding="utf-8")
        written.append(path)
    print(f"[cards] {len(written)} fichas → {CARDS}")
    return written


if __name__ == "__main__":
    run_build_cards()
