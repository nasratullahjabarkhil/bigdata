"""T1 · Universo de países (lo que el Hito 1 ya tiene que dejar escrito)."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.hito1

_COLUMNAS_EXCLUIDAS = ("source_id", "source_code", "name", "reason")


def test_el_perfil_no_se_queda_en_cinco_paises(src_texts, sources_yaml):
    """Hace falta un criterio de país sobre el perfil completo, no solo el foco."""
    transform = src_texts.get("transform.py", "")
    foco = sources_yaml.get("focus_countries") or []
    sigue_el_recorte = "focus_countries" in transform and "cca3" in transform
    assert not (sigue_el_recorte and len(foco) <= 5 and "excluded_entities" not in transform), (
        "transform.py sigue recortando el perfil con focus_countries y no aparece "
        "excluded_entities. El Hito 1 pide el universo: criterio explícito de país, "
        "countries solo con lo aceptado, y lo descartado anotado (no un filtro mudo "
        "a los cinco ISO del starter)."
    )


def test_se_escribe_countries_parquet(src_texts):
    """data/clean/countries.parquet es parte del contrato, no un CSV suelto."""
    texto = "\n".join(src_texts.values())
    assert "countries.parquet" in texto, (
        "Ningún módulo cita countries.parquet. "
        "El universo aceptado tiene que quedar en data/clean/countries.parquet "
        "con iso3 como clave única."
    )


def test_se_escriben_las_entidades_excluidas(src_texts):
    """Nada se descarta en silencio: excluded_entities.csv con cuatro columnas."""
    texto = "\n".join(src_texts.values())
    assert "excluded_entities" in texto, (
        "No aparece excluded_entities. Cada entidad que no sea país en vuestro "
        "criterio va a data/clean/excluded_entities.csv."
    )
    faltan = [col for col in _COLUMNAS_EXCLUIDAS if col not in texto]
    assert not faltan, (
        "excluded_entities tiene que llevar las columnas "
        + ", ".join(_COLUMNAS_EXCLUIDAS)
        + f". No se ven: {faltan}."
    )


def test_las_equivalencias_de_codigo_no_son_un_if_suelto(src_texts, sources_yaml):
    """Un código distinto por fuente vive en configuración, no en un if del transform."""
    bolsa = "\n".join(
        [
            src_texts.get("transform.py", ""),
            src_texts.get("config.py", ""),
            str(sources_yaml),
        ]
    ).lower()
    tiene_mapa = any(
        token in bolsa
        for token in ("aliases", "alias", "code_map", "equivalen", "crosswalk")
    )
    assert tiene_mapa, (
        "No se ve un sitio donde declarar equivalencias de código entre fuentes. "
        "Si una entidad aceptada usa otro código en alguna API, el mapa va en "
        "configuración (sources.yaml o un bloque leído por config.py), no en un "
        "if aislado del transform."
    )
