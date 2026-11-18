# Observatorio de países — Hito 1 y Hito 2

Proyecto ETL (`raw → clean → serving`) que recoge, limpia y sirve información
de todos los países, más un job de Spark que mide la cobertura de los datos.

## Objetivo

El starter trabajaba con cinco países y un año. El proyecto lo amplía al
**universo**: 196 países aceptados (criterio en `sources.yaml`), toda la serie
histórica, Parquet tipado y particionado, y `spark/job_01.py`. La clave común es
`iso3`. Las equivalencias de código entre fuentes (Kosovo `XKX`/`UNK`/`OWID_KOS`,
Grecia `EL`…) están en configuración.

## Fuentes

1. Perfil de países e idiomas: `mledoze/countries` (catálogo y criterio de país).
2. Banco Mundial, una petición paginada `country/all` por indicador:
   `NY.GDP.PCAP.CD`, `SP.POP.TOTL` (fijos), `SP.DYN.LE00.IN`,
   `SP.URB.TOTL.IN.ZS`, `NY.GDP.MKTP.CD` y `SP.POP.TOTL.FE.IN`.
3. Eurostat `demo_pjan` (población a 1 de enero, dataset completo).
4. Our World in Data: esperanza de vida (CSV completo).

Configuración: [sources.yaml](./sources.yaml).

## Estructura

```text
data/raw/                 Respuestas originales + manifest.csv (acumulativo, run_id)
data/clean/               countries.parquet, languages.parquet,
                          indicators/indicator_id=<id>/*.parquet (Hive),
                          excluded_entities.csv, join_orphans.csv
data/serving/             observatorio.sqlite, spark/cobertura_decada/
cards/                    Una ficha Markdown por país aceptado
src/                      Extract, Transform, Load y Cards
spark/job_01.py           Cobertura por indicador y década (RDD, DataFrame, SQL)
evidencias/               Event log del job y salidas de terminal
tests/hito1, tests/hito2  Batería del profesor
ENTREGA_02.md, BUGS.md    Documentación de la Entrega 2
```

`data/`, `logs/` y `.venv/` se regeneran y están en [.gitignore](./.gitignore).

## Instalación

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
```

Spark 4.2 necesita Java 17, 21 o 25 (por ejemplo `brew install openjdk@21` y
`export JAVA_HOME=/opt/homebrew/opt/openjdk@21`).

## Ejecución

```bash
python -m src.pipeline            # 1.ª vez: descarga (~3,5 min)
python -m src.pipeline            # 2.ª vez: no re-descarga nada (~13 s)
python -m src.pipeline --force    # repetir descargas a propósito
python spark/job_01.py            # exit=0 si RDD == DataFrame == SQL == SQLite
python spark/job_01.py --master "local[1]"
```

El extract respeta como máximo 5 peticiones/s, reintenta 429/5xx/timeouts con
espera creciente (no reintenta 404), solo guarda HTTP 200 y falla si lo
descargado del Banco Mundial no suma el `total` de la API.

## Validación

```bash
python -m pytest -q tests/hito1
python -m pytest -q tests/hito2
```

Los workflows `hito1-tests.yml` y `hito2-tests.yml` las ejecutan en GitHub
Actions en cada push y Pull Request. Resultados, decisiones y respuestas de
teoría: [ENTREGA_02.md](./ENTREGA_02.md). Defectos encontrados: [BUGS.md](./BUGS.md).
