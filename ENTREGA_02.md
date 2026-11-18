# Entrega 2 — Del ejemplo al universo

Equipo: Nasratullah Jabarkhil (ver `CONTRIBUTORS.md`).

## 1. Predicción (escrita antes de la primera ejecución completa)

Este apartado se escribe y se commitea **antes** de ejecutar el pipeline
completo con la configuración nueva. Solo se han consultado los metadatos de
la API (`per_page=1`), no los datos.

### A. Bruto del Banco Mundial (lo comprueba el extract)

Metadatos consultados el 18/11/2026 (`country/all/indicator/<id>?per_page=1`):

| Indicador | `total` declarado |
| --- | ---: |
| NY.GDP.PCAP.CD | 17 556 |
| SP.POP.TOTL | 17 556 |
| SP.DYN.LE00.IN | 17 556 |
| SP.URB.TOTL.IN.ZS | 17 556 |
| NY.GDP.MKTP.CD | 17 556 |
| SP.POP.TOTL.FE.IN | 17 556 |

17 556 = 266 entidades × 66 años (1960–2025). Espero que `data/raw/` guarde
exactamente **17 556 registros por indicador** (105 336 en total); si no
cuadra, el extract tiene que fallar. Con `per_page=5000` serán 4 páginas por
indicador, 24 peticiones.

### B. Filas en `data/clean/indicators` (lo decide el transform)

- Criterio de país: 194 independientes del perfil + Kosovo y Palestina = **196**.
- De las 266 entidades del Banco Mundial, ~79 son agregados y otras ~20 son
  territorios que mi criterio rechaza. Espero casar **~193** países (la Ciudad
  del Vaticano, por ejemplo, no está en el Banco Mundial).
- Guardo las filas con valor nulo como nulo explícito, así que por indicador
  espero ~193 × 66 ≈ **12 700 filas**, y ~76 000 para los seis.
- Filas con valor no nulo: bastantes menos, porque los primeros años tienen
  huecos. Mi estimación: población ~95 % con dato, PIB per cápita ~70 %,
  urbana ~95 %, esperanza de vida ~90 %.
- Eurostat `demo_pjan` (sexo T, edad TOTAL): ~50 países europeos × hasta 66
  años, con muchos huecos → **~2 500 filas**.
- OWID esperanza de vida: ~190 países casados × ~75 años (1950–2023), más
  series históricas largas de algunos países → **~15 000 filas**.
- **Total esperado en clean/indicators: ~95 000 filas** (no es 6 × 17 556,
  porque se quitan agregados y territorios, y se suman Eurostat y OWID).
- Huérfanos: espero muy pocos en el Banco Mundial (algún micro-estado sin
  serie) y ~145 por Eurostat (todo lo que no es Europa).

> El apartado 1 está en el commit `89ad2e6`, anterior a la primera ejecución
> completa (`run_id cbef7f57…`, 18/11/2026). Lo que sigue se escribió después
> de ejecutar.

## 2. Resultado y diferencia con la predicción

Salidas completas en `evidencias/terminal/`.

| Cifra | Predicción | Resultado | Diferencia |
| --- | ---: | ---: | --- |
| Bruto Banco Mundial por indicador | 17 556 | 17 556 (×6, 4 páginas) | Exacta: el extract cuadra con `total` |
| Países aceptados | 196 | 196 | — |
| Países casados en el Banco Mundial | ~193 | 195 | Solo faltan Vaticano (VAT, no está en el BM) y, en PIB, Corea del Norte (PRK) |
| Filas BM por indicador (con nulos) | ~12 700 | 12 870 (= 195 × 66) | +1 %: casaron 2 países más de lo previsto |
| Eurostat `demo_pjan` | ~2 500 | 2 577 (49 países) | +3 % |
| OWID esperanza de vida | ~15 000 | 16 492 (196 países, 1543–2023) | +10 %: series históricas más largas de lo previsto |
| **Total clean/indicators** | **~95 000** | **96 289** | +1,4 % |
| Filas con valor no nulo | — | 92 284 | 4 005 nulos explícitos (3 716 de PIB; 3 474 en las décadas de 1960 a 1980) |
| Huérfanos | ~145 Eurostat + pocos BM | 147 Eurostat + 8 BM = 155 | Eurostat: los 196 − 49 países europeos |

Filas con dato por indicador del Banco Mundial (de 12 870): PIB per cápita y PIB
11 012 (86 %), esperanza de vida 12 641 (98 %), población y población femenina
12 840 (99,8 %), población urbana 12 870 (100 %). Fallé por lo bajo en el PIB
per cápita (dije ~70 %): la serie histórica del BM está más completa de lo que
pensaba.

**Por qué el limpio (96 289) no es 6 × 17 556 = 105 336:** de las 266
entidades del Banco Mundial, 50 son agregados (WLD, EUU, AFE…) y 21 son
territorios que el criterio rechaza (Puerto Rico, Hong Kong, Groenlandia…). Esas
71 entidades × 66 años × 6 indicadores son 28 116 filas fuera. A eso se suman
Eurostat y OWID, que no son del Banco Mundial.

Idempotencia, con el pipeline completo dos veces seguidas:

```text
1.ª ejecución: 9 descargas (HTTP 200)              3 min 38 s
2.ª ejecución: "omitido, ya existe" ×9, 0 peticiones   13 s
manifest.csv: 18 filas, 2 run_id distintos (cbef7f57…, f488e4f9…), nada borrado
```

Job Spark (`python spark/job_01.py`, exit=0 en local[*], local[1] y local[4]):

```text
[job_01] particiones del DataFrame leído: 8
[job_01] grupos (indicator_id, década): 98
[job_01] filas con dato contadas: 92284
[job_01] OK: RDD == DataFrame == Spark SQL == SQLite
```

## 3. Decisiones

- **Criterio de país.** Se acepta una entrada del perfil `mledoze` si
  `independent: true` (194). Se añaden por configuración Kosovo (UNK) y Palestina
  (PSE), que el Banco Mundial publica como economías: en total, 196. Quedan
  fuera 54 territorios del perfil (Aruba, Gibraltar, la Antártida, Hong Kong,
  Taiwán…). Por qué: los agregados y dependencias sesgan la mediana regional de
  las fichas. Todo lo descartado está en `excluded_entities.csv` (204 filas) con
  motivo: 54 `not_independent` del perfil, y 50 + 10 + 28 agregados
  `aggregate_or_not_in_profile` del BM, Eurostat y OWID, más 21 + 41 entidades
  del perfil no aceptadas que aparecen en BM y OWID.
- **Equivalencias de código** en `sources.yaml` (`countries.code_equivalences`):
  BM `XKX → UNK`; Eurostat `EL → GRC`, `UK → GBR`, `XK → UNK`; OWID
  `OWID_KOS → UNK`.
- **Los cuatro indicadores elegidos** (además de NY.GDP.PCAP.CD y SP.POP.TOTL):
  - SP.DYN.LE00.IN (esperanza de vida): se puede contrastar con OWID.
  - SP.URB.TOTL.IN.ZS (población urbana, %): una proporción sin unidades
    monetarias, comparable entre países de cualquier tamaño.
  - NY.GDP.MKTP.CD (PIB total): junto con la población, permite comprobar el
    PIB per cápita.
  - SP.POP.TOTL.FE.IN (población femenina): permite ver la proporción
    mujeres/total y su cobertura frente a la población total.
- **Banco Mundial `country/all` con `per_page=5000`:** 4 páginas por indicador
  (24 peticiones). Con el límite de 5 peticiones/s, la extracción no pasa de ~5 s
  de red en el BM. Antes eran ~1500 peticiones país a país.
- **Particionado:** `data/clean/indicators/` por `indicator_id`: 8 carpetas, un
  fichero cada una (25–134 KB). No por `iso3`: ver la pregunta 3.
- **Nulos:** el valor ausente del BM se guarda como nulo, nunca como 0. Spark y
  SQLite solo cuentan filas con `value` no nulo.

## 4. Evidencia

- `evidencias/eventlog_v2_local-1795026100626/`: subdirectorio completo del
  event log (Spark 4.2.1, zstd) de la ejecución `local[*]` citada aquí.
- `evidencias/terminal/`: salida de las dos ejecuciones del pipeline, del job en
  `local[*]`, `local[1]` y `local[4]`, y las mediciones de `data/clean/`.

## 5. Preguntas de teoría (con nuestros datos)

### 5.1 ¿Hay algún job en el event log antes de la primera acción?

Sí. En el event log hay 20 jobs. El **Job 0** (`parquet at …`, 1 tarea, 1 stage)
aparece antes del Job 1, que es el `collect()` de la versión RDD, la primera
acción de nuestro código. Lo lanza `spark.read.parquet(...)`: no es una acción,
pero Spark ejecuta un job para leer los footers de los Parquet y fijar el
esquema. `getNumPartitions()` y `printSchema()` no generan jobs. Los otros
jobs: el 1 (RDD, 2 stages: `reduceByKey` y `collect`), del 2 al 11 (`collect`
del DataFrame y del SQL; AQE parte cada consulta en varios jobs, uno por stage
con shuffle) y del 12 al 19 (`show` y la escritura de `cobertura_decada/`).

### 5.2 Particiones con local[1] y local[4], y dónde está el shuffle

| Master | Particiones al leer | Tareas del stage `reduceByKey` |
| --- | ---: | ---: |
| local[1] | 1 | 1 |
| local[4] | 4 | 4 |
| local[*] (8 núcleos) | 8 | 8 |

Con nuestros datos **sí cambia**, al contrario de lo que se espera con ficheros
grandes. El dataset ocupa solo 832 KB en 8 ficheros, muy por debajo de
`spark.sql.files.maxPartitionBytes` (128 MB). Spark divide el tamaño total
entre el paralelismo por defecto (los núcleos del master) para no dejar núcleos
ociosos, y agrupa ficheros pequeños en una partición. Con local[1] los 8 ficheros
caben en 1 partición; con local[*] hay una por fichero. Con ficheros de cientos
de MB, el número dependería de `maxPartitionBytes` y no del master.

El shuffle está en los operadores **`Exchange`** del plan físico de la versión
DataFrame (`evidencias/terminal/job_01_local_todos.txt`):

- `(5) Exchange hashpartitioning(indicator_id, decade, iso3, 200)`: lo
  necesita el `countDistinct(iso3)`, que primero agrupa por (indicador, década,
  país).
- `(8) Exchange hashpartitioning(indicator_id, decade, 200)`: el `groupBy`
  propiamente dicho, el Shuffle de MapReduce. Antes y después van `HashAggregate`
  parcial (combiner) y final (Reduce).
- `(10) Exchange rangepartitioning(...)`: lo provoca el `orderBy`.

Los 200 son `spark.sql.shuffle.partitions`; AQE los agrupa en 1 tarea porque el
resultado son solo 98 grupos. En el RDD, el shuffle está entre el stage
`reduceByKey` y el stage `collect`.

### 5.3 CSV frente a Parquet, y por qué no particionar por iso3

| Formato | Tamaño de `indicators` (96 289 filas) |
| --- | ---: |
| `indicators.csv` | 7 964 602 B (7,6 MB) |
| Parquet Hive por `indicator_id` (8 ficheros) | 831 856 B (0,8 MB) |
| Parquet Hive por `iso3` (prueba, 196 ficheros) | 2 637 812 B (2,5 MB) |

El Parquet ocupa **9,6 veces menos** que el CSV. En un CSV, cada fila repite como
texto `indicator_label`, `unit` y `source_id`. En Parquet van por columnas, con
codificación por diccionario, compresión, y tipos de verdad (`year` int64,
`value` double). Además, la partición `indicator_id` no se guarda dentro del
fichero. El plan lo aprovecha: `ReadSchema` solo lee `iso3`, `year` y `value`
(poda de columnas), y el filtro `IsNotNull(value)` baja al lector
(`PushedFilters`).

Particionando por `iso3` salen 196 carpetas de ~13 KB de media, y el total pesa
3,2 veces más que por indicador: cada fichero diminuto repite esquema y footer, y
comprime peor. Además, la consulta habitual ("este indicador en todos los
países") tendría que abrir las 196 carpetas, en vez de una.

## 6. Pendientes (honestos)

- Las fichas comparan el último año con dato de cada indicador. No hay gráficos
  ni el informe de tres hallazgos de la Práctica 1 actualizado al universo.
- El criterio de país depende del campo `independent` de `mledoze`. Taiwán
  (`independent: false`) queda fuera, aunque tiene datos en OWID.
- Eurostat solo cubre 49 países europeos: sus 147 huérfanos son esperables,
  pero el informe de huérfanos no distingue "no aplica" de "falta el dato".
- No se mide el coste de Spark frente a pandas (queda para la práctica siguiente).
- El job necesita Java 17/21/25 instalado. En este Mac se instaló OpenJDK 21
  con Homebrew y se ejecutó con `JAVA_HOME` apuntando a él.
