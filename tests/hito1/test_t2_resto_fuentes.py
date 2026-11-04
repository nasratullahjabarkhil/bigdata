"""T2 · El resto de fuentes también pasa a universo y serie completa."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.hito1


def _fuentes(sources_yaml: dict) -> list[dict]:
    return [s for s in (sources_yaml.get("sources") or []) if isinstance(s, dict)]


def test_eurostat_no_pide_un_solo_anio_ni_un_solo_geo(sources_yaml):
    """Si Eurostat sigue activo, la URL no puede fijar geo={iso2} y time=2023."""
    euros = [
        s
        for s in _fuentes(sources_yaml)
        if "eurostat" in str(s.get("id", "")).lower()
        or "eurostat" in str(s.get("url_template") or s.get("url") or "")
    ]
    if not euros:
        pytest.skip("Este repo no tiene Eurostat activo; no aplica esta comprobación.")
    for fuente in euros:
        url = str(fuente.get("url_template") or fuente.get("url") or "")
        assert "time=2023" not in url and "time=2024" not in url, (
            f"La fuente {fuente.get('id')} sigue pidiendo un único año ({url}). "
            "Eurostat, si se mantiene, tiene que traer todas las entidades y toda la serie."
        )
        assert "{iso2}" not in url and "{iso3}" not in url, (
            f"La fuente {fuente.get('id')} sigue troceada por país ({url}). "
            "En el Hito 1 la petición es de universo, no una URL por ISO del foco."
        )


def test_owid_no_esta_recortado_a_un_anio_de_meta(sources_yaml, src_texts):
    """OWID, si sigue, no puede quedarse en el año único del starter."""
    owids = [
        s
        for s in _fuentes(sources_yaml)
        if "owid" in str(s.get("id", "")).lower()
        or str(s.get("transformer")) == "owid_life_expectancy"
    ]
    if not owids:
        pytest.skip("Este repo no tiene OWID activo.")
    transform = src_texts.get("transform.py", "")
    for fuente in owids:
        meta = fuente.get("meta") or {}
        assert "year" not in meta, (
            f"{fuente.get('id')} declara meta.year={meta.get('year')!r}. "
            "Eso es el recorte de la Práctica 1 a un solo año. La serie va completa."
        )
    assert 'meta.get("year"' not in transform and "meta.get('year'" not in transform, (
        "transform_owid sigue filtrando por un único año de meta. "
        "La serie histórica tiene que entrar entera."
    )


def test_el_extract_no_itera_solo_focus_countries(extract_source: str):
    """A escala, el bucle del extract no puede ser «un GET por país del foco»."""
    assert "focus_countries" not in extract_source, (
        "src/extract.py sigue bajando una petición por cada focus_country. "
        "El Banco Mundial (y el resto de fuentes de indicador) se piden en "
        "universo: paginación de la API, no cinco URL con {iso2}."
    )


def test_requirements_declara_lo_necesario_para_parquet(repo_root):
    """Si Parquet solo funciona por un pip suelto, la entrega no regenera."""
    path = repo_root / "requirements.txt"
    assert path.is_file(), "Falta requirements.txt."
    texto = path.read_text(encoding="utf-8").lower()
    assert "pyarrow" in texto or "fastparquet" in texto, (
        "requirements.txt no menciona pyarrow ni fastparquet. "
        "El Parquet del Hito 1 tiene que instalarse desde ese fichero."
    )
