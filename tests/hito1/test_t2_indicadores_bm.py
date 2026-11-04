"""T2 · Al menos seis indicadores del Banco Mundial (dos fijos)."""

from __future__ import annotations

import pytest

from helpers import source_mentions, worldbank_indicator_ids

pytestmark = pytest.mark.hito1


def test_incluye_pib_per_capita_fijo(sources_yaml):
    """El enunciado fija NY.GDP.PCAP.CD como indicador obligatorio."""
    ids = worldbank_indicator_ids(sources_yaml)
    assert "NY.GDP.PCAP.CD" in ids, (
        "Falta el indicador obligatorio NY.GDP.PCAP.CD (PIB per cápita) "
        "como fuente del Banco Mundial en sources.yaml."
    )


def test_incluye_poblacion_total_fija(sources_yaml):
    """El enunciado fija SP.POP.TOTL como indicador obligatorio."""
    ids = worldbank_indicator_ids(sources_yaml)
    assert "SP.POP.TOTL" in ids, (
        "Falta el indicador obligatorio SP.POP.TOTL (población total) "
        "como fuente del Banco Mundial en sources.yaml."
    )


def test_al_menos_seis_indicadores_world_bank(sources_yaml):
    """T2 exige ≥ 6 indicadores WDI (los dos fijos + cuatro elegidos)."""
    ids = worldbank_indicator_ids(sources_yaml)
    unicos = sorted(set(ids))
    assert len(unicos) >= 6, (
        f"Se observan {len(unicos)} indicador(es) del Banco Mundial: {unicos}. "
        "El Hito 1 / T2 pide al menos seis del catálogo WDI."
    )


def test_serie_completa_no_solo_mrnev1(sources_yaml):
    """La entrega pide toda la serie histórica, no el último valor (mrnev=1)."""
    assert not source_mentions(sources_yaml, "mrnev=1"), (
        "sources.yaml todavía usa mrnev=1 (solo el valor más reciente). "
        "Para el Hito 1 hay que pedir la serie completa de cada indicador."
    )
