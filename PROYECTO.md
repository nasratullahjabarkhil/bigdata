# Observatorio de países — Hito 1

Proyecto ETL para recopilar, transformar y servir información de países
usando cinco fuentes de datos.

## Objetivo

El proyecto amplía el starter inicial, que trabajaba con cinco países, para
procesar todos los países válidos del perfil global `countries_profile.json`.
La clave común del modelo es `iso3`; también se mantiene el mapeo entre ISO2 e
ISO3 para conectar las distintas APIs.

## Fuentes utilizadas

1. Perfil de países e idiomas: `mledoze/countries`.
2. PIB per cápita: World Bank, `NY.GDP.PCAP.CD`.
3. Población de Eurostat: `demo_pjan`.
4. Esperanza de vida: Our World in Data.
5. Indicadores adicionales del World Bank:
   - `SP.POP.TOTL`
   - `SP.DYN.LE00.IN`
   - `SP.URB.TOTL.IN.ZS`
   - `NY.GDP.MKTP.CD`
   - `SP.POP.TOTL.FE.IN`

La configuración completa se encuentra en [sources.yaml](./sources.yaml).

## Estructura

```text
data/raw/       Respuestas originales y manifest.csv
data/clean/     CSV y Parquet normalizados
data/serving/   SQLite y Parquet de consulta
cards/          Una ficha Markdown por país
src/             Código Extract, Transform, Load y Cards
tests/hito1/    Pruebas automatizadas del Hito 1
.github/        Workflow de GitHub Actions
```

Los datos generados, la caché y el entorno virtual están excluidos mediante
[.gitignore](./.gitignore).

## Instalación

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

`pyarrow` permite escribir y leer los resultados Parquet.

## Ejecución

### Pipeline completo

```bash
python -m src.pipeline --parquet
```

El pipeline ejecuta:

```text
Extract → Transform → Load → Cards
```

### Reutilizar descargas

```bash
python -m src.pipeline --skip-extract --parquet
```

Las descargas válidas se reutilizan automáticamente mediante
`data/raw/manifest.csv`.

### Forzar nuevas descargas

```bash
python -m src.pipeline --force --parquet
```

También se puede ejecutar solo la extracción:

```bash
python -m src.extract
python -m src.extract --force
```

World Bank utiliza paginación y reintentos para errores de red, HTTP 429 y
errores 5xx. Las descargas se escriben primero como `.part` y el manifiesto
registra el `run_id` de cada ejecución.

## Resultados

El proceso genera:

- `countries.csv` y `countries.parquet`.
- `languages.csv` y `languages.parquet`.
- `indicators.csv` y `indicators.parquet`.
- `indicators_hive/indicator_id=*/`.
- `join_orphans.csv`.
- `observatorio.sqlite`.
- Una ficha Markdown por país en `cards/`.

Los valores ausentes se conservan como nulos. No se convierten en ceros. Los
duplicados de la clave de indicadores se detectan antes de guardar las tablas.

## Validación

Ejecutar las pruebas del Hito 1:

```bash
python -m pytest -q tests/hito1
```

Resultado validado:

```text
65 passed
```

El workflow [hito1-tests.yml](./.github/workflows/hito1-tests.yml) ejecuta
estas comprobaciones automáticamente en GitHub Actions.

## Commits locales preparados

El trabajo del Hito 1 está dividido en:

```text
a64dd5c Configura fuentes y dependencias del Hito 1
4f6c883 Amplía el ETL y valida sus salidas
4b9bb58 Añade pruebas y workflow del Hito 1
```

Este documento se añade como documentación pública del proyecto. Los archivos
`info.md` y `ampliacion_todos_los_paises.md` son documentación local y
continúan excluidos por `.gitignore`.
