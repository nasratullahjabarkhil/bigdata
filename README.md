# Observatorio país — framework mínimo (Práctica 1 Big Data)

Esqueleto ETL alineado con el enunciado: **`raw → clean → serving`**.

Sirve para arrancar con las tres URLs del PDF y **extenderlo a otras 2–3
fuentes** sin romper la estructura.

## Arranque

```bash
cd practica_01_starter
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m src.pipeline
```

Salidas:

| Zona | Ruta |
| --- | --- |
| bruto | `data/raw/*.json` + `data/raw/manifest.csv` |
| limpio | `data/clean/countries.csv`, `languages.csv`, `indicators.csv`, `join_orphans.csv` |
| servicio | `data/serving/observatorio.sqlite` |
| fichas | `cards/ESP.md`, … |

Reutilizar descargas ya hechas:

```bash
python -m src.pipeline --skip-extract
```

Pasos sueltos: `python -m src.extract` · `python -m src.transform` ·
`python -m src.load` · `python -m src.build_cards`.

## Elegir exactamente 3 de las 5 fuentes

En `sources.yaml` hay las cinco URLs del enunciado. Dejad **tres** activas
(comentad o borrad las otras dos) y documentad la elección en el README.

## Cómo cambiar o añadir URLs

1. Editad un bloque en `sources.yaml` (fija o con `{iso2}` / `{iso3}`).
2. Si el JSON/CSV no encaja, cread una función en `src/transform.py` y
   registradla en `TRANSFORMERS`.
3. Poned `transformer: nombre_de_vuestra_funcion` en el YAML.
4. Ejecutad de nuevo `python -m src.pipeline`.

Transformers incluidos: `countries_profile`, `worldbank_indicator`,
`eurostat_demo_pjan`, `owid_life_expectancy`, `generic_passthrough`.

## Reglas del framework (no romper)

- **Extract** no limpia ni une fuentes.
- **Transform** no descarga; solo lee `data/raw/`.
- **Load / fichas** no leen JSON bruto; solo `clean/` → `serving/`.
- Los países de trabajo están en `focus_countries` del YAML (mapeo ISO2↔ISO3).
- Transform obligatorio: `iso3` canónico, idiomas en tabla larga, esquema
  común de indicadores, `join_orphans.csv`.

## Informe

Completad el informe en `informe/` y los insights del enunciado.
No subáis `.venv/` ni cachés.
