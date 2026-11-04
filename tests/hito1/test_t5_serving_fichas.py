"""T5 · SQLite regenerado desde clean y fichas con mediana regional del mismo año."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.hito1


def test_el_sqlite_sale_de_clean(src_texts):
    """serving/observatorio.sqlite se regenera leyendo la zona limpia."""
    load = src_texts.get("load.py", "")
    assert "observatorio.sqlite" in load, "load.py no cita observatorio.sqlite."
    assert "CLEAN" in load or "clean" in load, (
        "load.py no lee data/clean/. El SQLite tiene que reconstruirse desde "
        "la zona limpia en cada ejecución, no desde raw/."
    )


def test_hay_una_ficha_por_pais_de_countries(src_texts):
    """El bucle de fichas recorre countries, no una lista fija de cinco ISO."""
    cards = src_texts.get("build_cards.py", "")
    assert "countries" in cards and "iso3" in cards, (
        "build_cards.py no recorre la tabla countries."
    )
    fijos = ("ESP.md", "FRA.md", "DEU.md", "ITA.md", "PRT.md")
    presentes = [nombre for nombre in fijos if nombre in cards]
    assert not presentes, (
        f"build_cards.py menciona fichas fijas ({presentes}). "
        "Tiene que salir una ficha por cada fila de countries, sea cual sea el universo."
    )


def test_la_comparacion_es_la_mediana_de_la_region_en_el_mismo_anio(src_texts):
    """Media del starter no vale: mediana, misma región, mismo indicador y mismo año."""
    cards = src_texts.get("build_cards.py", "")
    assert "median" in cards.lower() or "mediana" in cards.lower(), (
        "La ficha sigue sin mediana. El bloque comparativo usa la mediana de "
        "la región, no la media del starter."
    )
    assert ".mean(" not in cards, (
        "build_cards.py todavía agrega con mean(). El enunciado pide la mediana."
    )
    assert "year" in cards, (
        "El comparativo no menciona year. Mezclar años distintos en la misma "
        "comparación es un error: país, indicador y año van juntos."
    )


def test_la_ficha_dice_cuando_falta_el_dato_de_ese_anio(src_texts):
    """Si el país no tiene valor ese año, la ficha lo dice; no omite el bloque."""
    cards = src_texts.get("build_cards.py", "").lower()
    assert any(
        frase in cards
        for frase in ("sin dato", "no tiene dato", "no hay dato", "ausente", "nulo")
    ), (
        "No se ve un texto para el país sin dato en ese año. "
        "La ficha tiene que decirlo, no saltarse la comparación en silencio."
    )
