"""Ejecución real del ETL de la Práctica 1, sin red y sin tocar data/.

Estas pruebas tienen que pasar sobre el código que ya entregaron los
equipos. No exigen todavía el contrato del Hito 1: comprueban que los
módulos siguen pudiendo recorrerse (config, extract, transform, load,
fichas y pipeline) cuando se les da un raw mínimo.
"""

from __future__ import annotations

import ast
import json
import sqlite3
from pathlib import Path

import pandas as pd
import pytest
import requests

from support.sandbox import import_src, sandbox

pytestmark = [pytest.mark.hito1, pytest.mark.runtime]

_MODULOS = (
    "__init__.py",
    "config.py",
    "paths.py",
    "extract.py",
    "transform.py",
    "load.py",
    "build_cards.py",
    "pipeline.py",
)


@pytest.fixture
def etl(repo_root: Path):
    try:
        return import_src(repo_root)
    except Exception as exc:  # noqa: BLE001 — el fallo debe explicar el módulo
        pytest.fail(f"No se pudo importar el paquete src del repo: {exc}")


@pytest.fixture
def caja(etl, tmp_path: Path, monkeypatch):
    dirs = sandbox(etl, tmp_path, monkeypatch)
    return etl, dirs


def _paises(config):
    return [
        config.FocusCountry(iso2="ES", iso3="ESP"),
        config.FocusCountry(iso2="FR", iso3="FRA"),
    ]


def _perfil() -> list[dict]:
    return [
        {
            "cca2": "ES",
            "cca3": "ESP",
            "name": {"common": "Spain", "official": "Kingdom of Spain"},
            "region": "Europe",
            "subregion": "Southern Europe",
            "languages": {"spa": "Spanish"},
        },
        {
            "cca2": "FR",
            "cca3": "FRA",
            "name": {"common": "France", "official": "French Republic"},
            "region": "Europe",
            "subregion": "Western Europe",
            "languages": {"fra": "French"},
        },
        {
            "cca2": "XX",
            "cca3": "XXX",
            "name": {"common": "Not a focus", "official": "Skipped"},
            "region": "Nowhere",
            "subregion": "None",
            "languages": {"xxx": "Ignored"},
        },
    ]


def _wb() -> list:
    return [
        {"page": 1, "pages": 1, "total": 2},
        [
            None,
            {
                "date": "2020",
                "value": 100.5,
                "indicator": {"value": "GDP per capita"},
            },
            {"date": "2021", "value": None, "indicator": {"value": "GDP per capita"}},
        ],
    ]


@pytest.mark.parametrize("nombre", _MODULOS)
def test_cada_modulo_de_src_compila(repo_root: Path, nombre: str):
    """Los ocho módulos del esqueleto existen y son Python válido."""
    path = repo_root / "src" / nombre
    assert path.is_file(), f"Falta src/{nombre}. El pipeline de la práctica parte de ese fichero."
    ast.parse(path.read_text(encoding="utf-8"))


def test_load_config_lee_el_yaml_del_repo(etl):
    """sources.yaml del clon se carga con las dataclasses del propio código."""
    cfg = etl.config.load_config()
    assert cfg.sources, "sources.yaml no declara ninguna fuente activa."
    assert all(getattr(src, "id", None) for src in cfg.sources)
    assert isinstance(cfg.focus_countries, list)


def test_resolve_url_plantilla_y_url_fija(etl):
    """Source.resolve_url cubre url fija, plantilla y el error si faltan ambas."""
    Source = etl.config.Source
    fija = Source(id="perfil", name="perfil", url="https://example.test/p.json")
    assert fija.resolve_url() == "https://example.test/p.json"
    plantilla = Source(
        id="wb",
        name="wb",
        url_template="https://example.test/{iso2}/{iso3}",
    )
    assert plantilla.resolve_url(iso2="ES", iso3="ESP").endswith("/ES/ESP")
    vacia = Source(id="vacia", name="vacia")
    with pytest.raises(ValueError):
        vacia.resolve_url()


def test_extract_url_fija_escribe_raw_y_manifiesto(caja, monkeypatch):
    """Una fuente sin plantilla deja un fichero y una fila de manifiesto."""
    etl, dirs = caja

    class Respuesta:
        status_code = 200
        content = b'{"ok": true}'

    monkeypatch.setattr(etl.extract.requests, "get", lambda *a, **k: Respuesta())
    cfg = etl.config.Config(
        sources=[
            etl.config.Source(
                id="countries_profile",
                name="perfil",
                url="https://example.test/countries.json",
                format="json",
                transformer="countries_profile",
            )
        ],
        focus_countries=[],
    )
    manifiesto = etl.extract.run_extract(cfg)
    assert manifiesto == dirs["MANIFEST"]
    assert (dirs["RAW"] / "countries_profile.json").read_bytes() == b'{"ok": true}'
    texto = manifiesto.read_text(encoding="utf-8")
    assert "countries_profile" in texto
    assert "200" in texto


def test_extract_plantilla_una_peticion_por_pais_foco(caja, monkeypatch):
    """Con url_template el extract pide una vez por país de la config de prueba."""
    etl, dirs = caja
    vistas: list[str] = []

    class Respuesta:
        status_code = 200
        content = b"[1]"

    def falso_get(url, timeout=None, headers=None):
        vistas.append(url)
        assert headers and "User-Agent" in headers
        assert timeout
        return Respuesta()

    monkeypatch.setattr(etl.extract.requests, "get", falso_get)
    cfg = etl.config.Config(
        sources=[
            etl.config.Source(
                id="worldbank_gdp",
                name="pib",
                url_template="https://example.test/{iso2}/NY.GDP.PCAP.CD",
                format="json",
                transformer="worldbank_indicator",
            )
        ],
        focus_countries=_paises(etl.config),
    )
    etl.extract.run_extract(cfg)
    assert len(vistas) == 2
    assert (dirs["RAW"] / "worldbank_gdp_ES.json").is_file()
    assert (dirs["RAW"] / "worldbank_gdp_FR.json").is_file()


def test_extract_anota_el_fallo_de_red_en_el_manifiesto(caja, monkeypatch):
    """Un timeout no debe tumbar el extract: queda en el manifiesto."""
    etl, dirs = caja

    def falla(url, timeout=None, headers=None):
        raise requests.RequestException("timeout de prueba")

    monkeypatch.setattr(etl.extract.requests, "get", falla)
    cfg = etl.config.Config(
        sources=[
            etl.config.Source(
                id="countries_profile",
                name="perfil",
                url="https://example.test/countries.json",
                format="json",
            )
        ],
        focus_countries=[],
    )
    etl.extract.run_extract(cfg)
    texto = dirs["MANIFEST"].read_text(encoding="utf-8")
    assert "timeout de prueba" in texto
    assert not (dirs["RAW"] / "countries_profile.json").exists()


def test_transform_perfil_filtra_foco_y_explota_idiomas(caja):
    """El perfil produce countries y languages; un ISO3 fuera de foco no entra."""
    etl, dirs = caja
    (dirs["RAW"] / "countries_profile.json").write_text(
        json.dumps(_perfil()), encoding="utf-8"
    )
    fuente = etl.config.Source(
        id="countries_profile",
        name="perfil",
        format="json",
        transformer="countries_profile",
        url="https://example.test/c.json",
    )
    cfg = etl.config.Config(sources=[fuente], focus_countries=_paises(etl.config))
    tablas = etl.transform.transform_countries_profile(fuente, cfg)
    iso3 = set(tablas["countries"]["iso3"])
    assert iso3 == {"ESP", "FRA"}
    assert set(tablas["languages"]["language_code"]) == {"spa", "fra"}


def test_transform_worldbank_ignora_nulos_de_registro_y_payloads_inutiles(caja):
    """El transformer del Banco Mundial tolera nulls, listas cortas y ficheros ausentes."""
    etl, dirs = caja
    fuente = etl.config.Source(
        id="worldbank_gdp",
        name="pib",
        format="json",
        transformer="worldbank_indicator",
        url_template="https://example.test/{iso2}",
        meta={
            "indicator_id": "NY.GDP.PCAP.CD",
            "indicator_label": "GDP per capita",
            "unit": "USD",
        },
    )
    cfg = etl.config.Config(sources=[fuente], focus_countries=_paises(etl.config))
    (dirs["RAW"] / "worldbank_gdp_ES.json").write_text(json.dumps(_wb()), encoding="utf-8")
    (dirs["RAW"] / "worldbank_gdp_FR.json").write_text("{}", encoding="utf-8")
    tabla = etl.transform.transform_worldbank_indicator(fuente, cfg)["indicators"]
    assert list(tabla["iso3"]) == ["ESP", "ESP"]
    assert tabla.iloc[0]["value"] == 100.5
    assert pd.isna(tabla.iloc[1]["value"])

    (dirs["RAW"] / "worldbank_gdp_ES.json").write_text(json.dumps([{"page": 1}, []]), encoding="utf-8")
    vacia = etl.transform.transform_worldbank_indicator(fuente, cfg)["indicators"]
    assert vacia.empty


def test_transform_eurostat_celda_unica_y_valor_vacio(caja):
    """Eurostat: una celda entra; un value vacío no fabrica una fila."""
    etl, dirs = caja
    fuente = etl.config.Source(
        id="eurostat_pop",
        name="poblacion",
        format="json",
        transformer="eurostat_demo_pjan",
        url_template="https://example.test/{iso2}",
        meta={
            "indicator_id": "demo_pjan",
            "indicator_label": "Population",
            "unit": "persons",
            "time": "2023",
        },
    )
    cfg = etl.config.Config(sources=[fuente], focus_countries=_paises(etl.config))
    (dirs["RAW"] / "eurostat_pop_ES.json").write_text(
        json.dumps({"value": {"0": 47000000}, "label": "Population"}),
        encoding="utf-8",
    )
    (dirs["RAW"] / "eurostat_pop_FR.json").write_text(
        json.dumps({"value": {}}),
        encoding="utf-8",
    )
    tabla = etl.transform.transform_eurostat_demo_pjan(fuente, cfg)["indicators"]
    assert list(tabla["iso3"]) == ["ESP"]
    assert int(tabla.iloc[0]["value"]) == 47000000


def test_transform_owid_filtra_anio_de_meta(caja):
    """OWID se queda con el año declarado en meta, no con toda la serie del CSV."""
    etl, dirs = caja
    (dirs["RAW"] / "owid_life.csv").write_text(
        "Entity,Code,Year,Life expectancy\n"
        "Spain,ESP,2023,83.2\n"
        "France,FRA,2023,82.4\n"
        "Spain,ESP,2000,79.0\n"
        "Germany,DEU,2023,81.0\n",
        encoding="utf-8",
    )
    fuente = etl.config.Source(
        id="owid_life",
        name="esperanza",
        format="csv",
        transformer="owid_life_expectancy",
        url="https://example.test/owid.csv",
        meta={"indicator_id": "life_expectancy", "year": "2023", "unit": "years"},
    )
    cfg = etl.config.Config(sources=[fuente], focus_countries=_paises(etl.config))
    tabla = etl.transform.transform_owid_life_expectancy(fuente, cfg)["indicators"]
    assert set(tabla["iso3"]) == {"ESP", "FRA"}
    assert set(tabla["year"].astype(str)) == {"2023"}


def test_transform_generico_indexa_el_raw(caja):
    """El passthrough registra cada fichero crudo de esa fuente."""
    etl, dirs = caja
    (dirs["RAW"] / "extra.bin").write_bytes(b"abc")
    fuente = etl.config.Source(id="extra", name="extra", format="bin", url="https://example.test/x")
    cfg = etl.config.Config(sources=[fuente], focus_countries=[])
    tabla = etl.transform.transform_generic_passthrough(fuente, cfg)["custom_raw_index"]
    assert list(tabla["filename"]) == ["extra.bin"]
    assert int(tabla.iloc[0]["bytes"]) == 3


def test_run_transform_escribe_csv_y_huerfanos(caja):
    """run_transform vuelca countries, languages, indicators y join_orphans."""
    etl, dirs = caja
    (dirs["RAW"] / "countries_profile.json").write_text(json.dumps(_perfil()), encoding="utf-8")
    (dirs["RAW"] / "worldbank_gdp_ES.json").write_text(json.dumps(_wb()), encoding="utf-8")
    cfg = etl.config.Config(
        sources=[
            etl.config.Source(
                id="countries_profile",
                name="perfil",
                format="json",
                transformer="countries_profile",
                url="https://example.test/c.json",
            ),
            etl.config.Source(
                id="worldbank_gdp",
                name="pib",
                format="json",
                transformer="worldbank_indicator",
                url_template="https://example.test/{iso2}",
                meta={"indicator_id": "NY.GDP.PCAP.CD", "unit": "USD"},
            ),
        ],
        focus_countries=_paises(etl.config),
    )
    etl.transform.run_transform(cfg)
    for nombre in ("countries.csv", "languages.csv", "indicators.csv", "join_orphans.csv"):
        assert (dirs["CLEAN"] / nombre).is_file(), nombre
    huerfanos = (dirs["CLEAN"] / "join_orphans.csv").read_text(encoding="utf-8")
    filas = [ln.split(",")[0].strip() for ln in huerfanos.splitlines()[1:] if ln.strip()]
    assert "FRA" in filas
    assert "ESP" not in filas


def test_run_transform_rechaza_transformer_desconocido(caja):
    """Un nombre de transformer que no está en el registro es un error, no un aviso."""
    etl, _dirs = caja
    cfg = etl.config.Config(
        sources=[
            etl.config.Source(
                id="rara",
                name="rara",
                transformer="no_existe",
                url="https://example.test/x",
            )
        ],
        focus_countries=[],
    )
    with pytest.raises(KeyError):
        etl.transform.run_transform(cfg)


def test_load_crea_sqlite_y_omite_tablas_ausentes(caja):
    """Sin CSV, load abre la base y no inventa tablas."""
    etl, dirs = caja
    ruta = etl.load.run_load()
    assert ruta == dirs["DB"]
    assert ruta.is_file()
    with sqlite3.connect(ruta) as conn:
        nombres = {
            fila[0]
            for fila in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    assert "countries" not in nombres


def test_load_y_fichas_desde_clean(caja, monkeypatch):
    """Clean → SQLite → una ficha por país, leyendo solo serving."""
    etl, dirs = caja
    (dirs["RAW"] / "countries_profile.json").write_text(json.dumps(_perfil()), encoding="utf-8")
    (dirs["RAW"] / "worldbank_gdp_ES.json").write_text(json.dumps(_wb()), encoding="utf-8")
    cfg = etl.config.Config(
        sources=[
            etl.config.Source(
                id="countries_profile",
                name="perfil",
                format="json",
                transformer="countries_profile",
                url="https://example.test/c.json",
            ),
            etl.config.Source(
                id="worldbank_gdp",
                name="pib",
                format="json",
                transformer="worldbank_indicator",
                url_template="https://example.test/{iso2}",
                meta={
                    "indicator_id": "NY.GDP.PCAP.CD",
                    "indicator_label": "GDP per capita",
                    "unit": "USD",
                },
            ),
        ],
        focus_countries=_paises(etl.config),
    )
    etl.transform.run_transform(cfg)

    escritos: list[Path] = []

    def falso_parquet(self, pq, index=False):
        Path(pq).write_bytes(b"PAR1")
        escritos.append(Path(pq))

    monkeypatch.setattr(pd.DataFrame, "to_parquet", falso_parquet)
    etl.load.run_load(write_parquet=True)
    assert dirs["DB"].is_file()
    assert escritos, "Con --parquet, load tiene que intentar escribir Parquet."

    fichas = etl.build_cards.run_build_cards()
    assert {path.name for path in fichas} == {"ESP.md", "FRA.md"}
    esp = (dirs["CARDS"] / "ESP.md").read_text(encoding="utf-8")
    fra = (dirs["CARDS"] / "FRA.md").read_text(encoding="utf-8")
    assert "Spain" in esp
    assert "Spanish" in esp
    assert "GDP per capita" in esp
    assert "Europe" in esp
    assert "sin indicadores" in fra.lower() or "Sin indicadores" in fra


def test_load_sobrevive_si_falta_pyarrow(caja, monkeypatch):
    """Si to_parquet no está disponible, la base SQLite sigue generándose."""
    etl, dirs = caja
    (dirs["CLEAN"] / "countries.csv").write_text("iso3,name_common\nESP,Spain\n", encoding="utf-8")

    def sin_pyarrow(self, pq, index=False):
        raise ImportError("pyarrow ausente en la prueba")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", sin_pyarrow)
    ruta = etl.load.run_load(write_parquet=True)
    assert ruta.is_file()
    with sqlite3.connect(ruta) as conn:
        n = conn.execute("SELECT COUNT(*) FROM countries").fetchone()[0]
    assert n == 1


def test_fichas_exigen_base_y_countries_con_filas(caja):
    """Sin SQLite, o con countries vacía, build_cards tiene que fallar."""
    etl, dirs = caja
    with pytest.raises(FileNotFoundError):
        etl.build_cards.run_build_cards()

    etl.load.run_load()
    with sqlite3.connect(dirs["DB"]) as conn:
        conn.execute("CREATE TABLE countries (iso3 TEXT)")
    with pytest.raises(RuntimeError):
        etl.build_cards.run_build_cards()


def test_pipeline_encadena_las_cuatro_fases(etl, monkeypatch):
    """--skip-extract no llama al extract; --parquet se propaga a load."""
    llamadas: list[object] = []
    monkeypatch.setattr(etl.pipeline, "run_extract", lambda: llamadas.append("extract"))
    monkeypatch.setattr(etl.pipeline, "run_transform", lambda: llamadas.append("transform"))
    monkeypatch.setattr(
        etl.pipeline,
        "run_load",
        lambda write_parquet=False: llamadas.append(("load", write_parquet)),
    )
    monkeypatch.setattr(etl.pipeline, "run_build_cards", lambda: llamadas.append("cards"))

    etl.pipeline.main(["--skip-extract"])
    assert llamadas == ["transform", ("load", False), "cards"]

    llamadas.clear()
    etl.pipeline.main(["--parquet"])
    assert llamadas[0] == "extract"
    assert ("load", True) in llamadas
    assert llamadas[-1] == "cards"


def test_ensure_dirs_crea_el_arbol(caja):
    """paths.ensure_dirs materializa raw, clean, serving y cards."""
    etl, dirs = caja
    for clave in ("RAW", "CLEAN", "SERVING", "CARDS"):
        dirs[clave].rmdir()
    etl.paths.ensure_dirs()
    for clave in ("RAW", "CLEAN", "SERVING", "CARDS"):
        assert dirs[clave].is_dir()
