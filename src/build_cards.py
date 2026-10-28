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


def _card_markdown(iso3: str, countries: pd.DataFrame, languages: pd.DataFrame,
                   indicators: pd.DataFrame) -> str:
    c = countries[countries["iso3"] == iso3].iloc[0]
    langs = languages[languages["iso3"] == iso3]
    inds = indicators[indicators["iso3"] == iso3]

    lang_lines = (
        "\n".join(
            f"- `{r.language_code}`: {r.language_name}" for _, r in langs.iterrows()
        )
        or "- (sin idiomas en clean/)"
    )
    ind_lines = (
        "\n".join(
            f"- **{r.indicator_label}** ({r.year}): {r.value} {r.unit or ''} "
            f"_(fuente: {r.source_id})_"
            for _, r in inds.iterrows()
        )
        or "- (sin indicadores; posible huérfano del join)"
    )

    # Comparativo simple: media regional del primer indicador disponible
    comparative = "_Sin indicadores para comparar._"
    if not inds.empty and "region" in countries.columns:
        ind_id = inds.iloc[0]["indicator_id"]
        region = c["region"]
        peers = countries[countries["region"] == region]["iso3"]
        peer_vals = indicators[
            (indicators["iso3"].isin(peers))
            & (indicators["indicator_id"] == ind_id)
            & indicators["value"].notna()
        ]["value"]
        if not peer_vals.empty:
            mean_v = peer_vals.astype(float).mean()
            own = float(inds.iloc[0]["value"])
            comparative = (
                f"Respecto a la media de **{region}** en `{ind_id}` "
                f"({mean_v:,.2f}): este país está en **{own:,.2f}** "
                f"({'por encima' if own > mean_v else 'por debajo o igual'})."
            )

    return f"""# Ficha país — {c['name_common']} ({iso3})

- **Nombre oficial:** {c.get('name_official')}
- **ISO2 / ISO3:** {c.get('iso2')} / {iso3}
- **Región / subregión:** {c.get('region')} / {c.get('subregion')}

## Idiomas

{lang_lines}

## Indicadores

{ind_lines}

## Comparativo

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

    written: list[Path] = []
    for iso3 in countries["iso3"].tolist():
        md = _card_markdown(iso3, countries, languages, indicators)
        path = CARDS / f"{iso3}.md"
        path.write_text(md, encoding="utf-8")
        written.append(path)
        print(f"[cards] {path}")
    return written


if __name__ == "__main__":
    run_build_cards()
