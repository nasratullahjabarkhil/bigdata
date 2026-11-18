# BUGS.md — Defectos que destapó el universo

Cada defecto lleva cómo se reproduce, qué efecto tenía en los datos o en las
fichas, y cómo se arregló. Las cifras de "efecto" salen de los datos que dejó
la versión del Hito 1 (commit `3b9282f`) antes de los arreglos del Hito 2.

## 1. El dataset Hive acumulaba copias en cada ejecución

- **Reproducción:** con el código del Hito 1, ejecutar
  `python -m src.pipeline --skip-extract` varias veces y contar filas:
  `pd.read_parquet("data/clean/indicators_hive")` frente a `indicators.csv`.
- **Efecto:** 350 943 filas en el Parquet frente a 107 477 reales (más del
  triple). Cada partición `indicator_id=…/` tenía 3 ficheros, uno por
  ejecución, porque `to_parquet(..., partition_cols=...)` añade ficheros y no
  borra los anteriores. También quedaban particiones muertas de nombres antiguos
  (`indicator_id=gdp`, `indicator_id=pop`). Cualquier conteo sobre el Parquet
  (por ejemplo, el job Spark) salía inflado.
- **Arreglo:** `_write_indicators_hive()` en `src/transform.py` borra el
  directorio antes de escribir. Además, el dataset vive ahora en
  `data/clean/indicators/`, como pide el contrato. Dos ejecuciones dan el mismo
  número de filas.

## 2. Territorios y entidades no soberanas contadas como países

- **Reproducción:** `pd.read_csv("data/clean/countries.csv")` con el código del
  Hito 1. Se aceptaba cualquier entrada del perfil con ISO2/ISO3 válido.
- **Efecto:** 250 "países", entre ellos la Antártida (ATA), Aruba (ABW),
  Gibraltar (GIB) y Hong Kong (HKG). Entraban en la mediana regional de las
  fichas y salía una ficha para la Antártida. Al pasar a `country/all`, los
  agregados del Banco Mundial (WLD, EUU, AFE…) se habrían colado de la misma
  forma si se aceptara cualquier `countryiso3code`.
- **Arreglo:** criterio de país en `sources.yaml` (bloque `countries`): se
  acepta `independent: true` más Kosovo y Palestina (`include_iso3`). Todo lo
  demás va a `data/clean/excluded_entities.csv` con motivo (`not_independent`,
  `aggregate_or_not_in_profile`, `not_accepted_country`…). Las equivalencias
  (XKX → UNK, EL → GRC, UK → GBR, OWID_KOS → UNK) también están en configuración.

## 3. El manifiesto borraba el historial en cada ejecución

- **Reproducción:** con el código del Hito 1, ejecutar el extract dos veces y
  contar `run_id` distintos en `data/raw/manifest.csv`.
- **Efecto:** 1503 filas y **un solo** `run_id`. El manifiesto se abría con
  `mode="w"` y se guardaba como diccionario por nombre de fichero, así que solo
  quedaba la última ejecución. No se podía auditar qué se descargó en cada intento.
- **Arreglo:** `_append_manifest()` en `src/extract.py` abre el fichero en
  modo `"a"`. Cada descarga o reutilización añade una fila con su `run_id`
  (las reutilizadas llevan la nota "reutilizado … (idempotente)"). La
  idempotencia usa la última fila con HTTP 200 de cada fichero.

## 4. Etiqueta y unidad equivocadas en cinco indicadores del Banco Mundial

- **Reproducción:** en `indicators.csv` del Hito 1, filtrar
  `indicator_id == "SP.DYN.LE00.IN"`.
- **Efecto:** la esperanza de vida al nacer aparecía como
  `indicator_label = "Population, total"` y `unit = "persons"`. Los cinco
  indicadores de `worldbank_pop` compartían la etiqueta y la unidad del primero.
  En la ficha de España (`cards/ESP.md`) el PIB total (NY.GDP.MKTP.CD) salía como
  `**Population, total** (2025.0): 1906453309985.88 persons`.
- **Arreglo:** cada indicador declara su `indicator_label` y su `unit` en
  `meta.indicators` de `sources.yaml`, y `Source.indicator_specs()` los lleva
  hasta el transform.

## 5. Banco Mundial pedido país a país (~1500 peticiones)

- **Reproducción:** `sources.yaml` del Hito 1 con
  `country/{iso2}/indicator/...`, y `pytest tests/hito1` con la batería del
  profesor (`test_el_extract_no_itera_solo_focus_countries` falla).
- **Efecto:** 1503 peticiones por ejecución completa (6 indicadores × ~250
  entidades, más el resto de fuentes). Además, no había pausa entre peticiones
  correctas: solo había esperas en los reintentos, así que se podían superar las
  5 peticiones/s.
- **Arreglo:** una fuente `worldbank` con `country/all/indicator/{indicator_id}`,
  `paginate: worldbank` y `per_page=5000`: 24 peticiones en total. El extract
  comprueba que los registros guardados suman el `total` de la API (si no,
  falla), y `_throttle()` deja al menos 0,2 s entre peticiones al mismo servidor.

## 6. Eurostat leía dos formatos a la vez y dejaba años nulos

- **Reproducción:** en el Hito 1, con `data/raw/` que conserva los
  `eurostat_pop_<ISO2>.json` antiguos y el `eurostat_pop.json` global, contar
  filas de `eurostat_pop` con `year` nulo.
- **Efecto:** 39 filas de población sin año: venían de los ficheros por país
  antiguos, cuyo año dependía de un `meta.time` que ya no existía. No casaban con
  ningún año en las fichas ni en las décadas.
- **Arreglo:** con url fija, `transform_eurostat_demo_pjan` solo lee el fichero
  global; los ficheros por país solo se leen si la fuente usa `url_template`.
  El año sale siempre de la dimensión `time` y se guarda como entero.

## 7. Año como número real y fichas de cientos de líneas

- **Reproducción:** abrir `cards/ESP.md` generada con el código del Hito 1.
- **Efecto:** el año aparecía como `2025.0`: `pd.to_numeric` dejaba float
  cuando la columna tenía algún nulo. Además, la ficha listaba **585** filas de
  indicadores (toda la serie de todas las fuentes), y el comparativo usaba solo
  la primera fila, que a menudo era un año sin dato.
- **Arreglo:** `year` se tipa como `Int64` (entero con nulos) en el transform y
  al leer el Parquet en load. La ficha muestra, por indicador, el último año con
  dato y lo compara con la mediana de su región en el mismo indicador y año; si
  el país no tiene ningún dato, lo dice.

## 8. El job Spark abortaba por versiones de Python distintas

- **Reproducción:** en un Mac con Python 3.14 en el `PATH` y el `.venv` en
  Python 3.12, ejecutar `python spark/job_01.py` (primera versión del job).
- **Efecto:** exit=1 en el primer `collect()` del RDD:
  `PYTHON_VERSION_MISMATCH: Python in worker has different version: 3.14 than
  that in driver: 3.12`. El driver usaba el Python del `.venv`, pero Spark
  lanzaba los workers con el `python3` del sistema. Las versiones DataFrame y
  SQL no lo notan (no ejecutan código Python en los workers); el RDD sí.
- **Arreglo:** `build_session()` fija `PYSPARK_PYTHON` y `PYSPARK_DRIVER_PYTHON`
  a `sys.executable` (sin pisar lo que haya definido el usuario). Con eso, exit=0
  en local[*], local[1] y local[4].
