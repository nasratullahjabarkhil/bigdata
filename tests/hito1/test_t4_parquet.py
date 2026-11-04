"""T4 · Parquet particionado estilo Hive por indicator_id."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.hito1


def test_indicators_es_un_dataset_hive(src_texts):
    """data/clean/indicators/indicator_id=<id>/ y no un único indicators.parquet."""
    texto = "\n".join(src_texts.values())
    assert "indicator_id=" in texto or "partition_cols" in texto or "partition_by" in texto, (
        "No aparece el particionado Hive (indicator_id=… o partition_cols / "
        "partition_by). data/clean/indicators/ tiene que ser un dataset con "
        "un directorio por indicador."
    )
    assert "clean" in texto and "parquet" in texto.lower(), (
        "El Parquet de la zona limpia no se escribe desde src/. "
        "El destino es data/clean/, no solo un extra opcional en serving/."
    )


def test_languages_tambien_va_a_parquet(src_texts):
    """countries ya se mira en T1; languages cierra el trío de la zona limpia."""
    texto = "\n".join(src_texts.values())
    assert "languages.parquet" in texto, (
        "Falta languages.parquet. countries, languages e indicators salen "
        "de clean/ en Parquet, no se quedan solo en CSV."
    )


def test_el_flag_parquet_del_starter_no_es_el_contrato(src_texts):
    """--parquet opcional hacia serving/ no sustituye la zona limpia obligatoria."""
    pipeline = src_texts.get("pipeline.py", "")
    load = src_texts.get("load.py", "")
    transform = src_texts.get("transform.py", "")
    escribe_clean = "clean" in transform and (
        "to_parquet" in transform or "write_parquet" in transform or "pq.write" in transform
    )
    assert escribe_clean or ("partition" in load and "CLEAN" in load), (
        "El único Parquet que se ve es el opcional de load (--parquet hacia "
        "data/serving/). El Hito 1 pide la zona limpia particionada en "
        "data/clean/indicators/, generada siempre, no bajo un flag."
    )
    # pipeline puede seguir teniendo el flag de la práctica 1; no es suficiente.
    assert "--parquet" not in pipeline or escribe_clean, (
        "pipeline.py solo ofrece --parquet como extra. La escritura Hive en "
        "clean/ tiene que ocurrir en la ejecución normal del transform."
    )
