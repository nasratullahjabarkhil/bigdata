"""T3 · Esquema común, clave única y huérfanos por fuente."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.hito1

_COLUMNAS = (
    "iso3",
    "indicator_id",
    "indicator_label",
    "year",
    "value",
    "unit",
    "source_id",
)


def test_el_esquema_de_indicadores_esta_completo(src_texts):
    """Las siete columnas del contrato aparecen en el transform."""
    transform = src_texts.get("transform.py", "")
    faltan = [col for col in _COLUMNAS if col not in transform]
    assert not faltan, (
        "Al esquema de indicators le faltan columnas en transform.py: "
        f"{faltan}. El contrato es iso3, indicator_id, indicator_label, "
        "year, value, unit, source_id."
    )


def test_el_anio_se_guarda_como_entero(src_texts):
    """year es entero en todas las fuentes; nunca un texto dejado por la API."""
    transform = src_texts.get("transform.py", "")
    senales = (
        "astype(int)" in transform
        or 'astype("int' in transform
        or "astype('int" in transform
        or "Int64" in transform
        or "to_numeric" in transform
        or "int(rec.get(\"date\")" in transform
        or "int(rec.get('date')" in transform
    )
    assert senales, (
        "No se ve una conversión de year a entero en transform.py. "
        "El Banco Mundial devuelve el año como texto (date); hay que tiparlo. "
        "Un año en string rompe el Parquet y el job de décadas."
    )


def test_un_valor_ausente_no_se_sustituye_por_cero(src_texts):
    """Falta de dato = nulo. Un cero inventado falsea medianas y fichas."""
    transform = src_texts.get("transform.py", "")
    assert "fillna(0)" not in transform and "fillna(0.0)" not in transform, (
        "transform.py rellena nulos con 0. El enunciado prohíbe inventar ceros: "
        "value queda nulo si la fuente no trae dato."
    )


def test_la_clave_unica_es_un_error(src_texts):
    """Duplicados de (iso3, indicator_id, year, source_id) abortan, no avisan."""
    transform = src_texts.get("transform.py", "")
    menciona_clave = all(
        token in transform for token in ("iso3", "indicator_id", "year", "source_id")
    )
    aborta = any(token in transform for token in ("raise", "duplicat", "drop_duplicates"))
    # drop_duplicates a secas escondería el fallo; exigimos un raise cerca de la clave.
    assert menciona_clave and "raise" in transform and (
        "duplic" in transform.lower() or "unique" in transform.lower()
    ), (
        "No hay una comprobación que lance error si indicators repite "
        "(iso3, indicator_id, year, source_id). Duplicado = error, no aviso "
        f"(clave mencionada={menciona_clave}, aborta={aborta})."
    )


def test_huerfanos_por_fuente_e_indicador(src_texts):
    """join_orphans.csv lleva iso3, source_id e indicator_id."""
    transform = src_texts.get("transform.py", "")
    posicion = transform.find("join_orphans")
    assert posicion != -1, "transform.py no escribe join_orphans."
    ventana = transform[max(0, posicion - 350) : posicion + 250]
    for columna in ("iso3", "source_id", "indicator_id"):
        assert columna in ventana, (
            f"Junto a join_orphans no aparece la columna {columna}. "
            "El huérfano es por fuente e indicador: un país sin ningún dato "
            "en esa pareja, no un listado global con solo el iso3."
        )
