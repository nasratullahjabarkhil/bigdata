"""Extract: descarga intacta → data/raw/ + manifiesto.

No transforma. Añadir una URL nueva = entrada en sources.yaml.
"""

from __future__ import annotations

import csv
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

import requests

from .config import Config, FocusCountry, Source, load_config
from .paths import MANIFEST, RAW, ensure_dirs

# CAMBIO: identificador de agente de usuario personalizado y descriptivo.
# ANTES: "UCJC-DSBD-practica1/1.2 (educational ETL framework)".
# AHORA: identifica al equipo y proyecto en cabeceras HTTP siguiendo buenas prácticas éticas de scraping.
USER_AGENT = "bigdata2026-equipo-nasratullah (UCJC-DSBD)"
TIMEOUT = 60
# CAMBIO: política de reintentos exponenciales con retroceso (exponential backoff).
# ANTES: sin reintentos ni control de códigos de estado transitorios (si fallaba la red, fallaba el ETL).
# AHORA: se reintenta hasta 4 veces ante errores 429 (rate limit) y 5xx (servidor caído temporalmente),
# permitiendo extraer cientos de archivos de forma robusta y desatendida.
MAX_RETRIES = 4
RETRY_STATUSES = {429, 500, 502, 503, 504}

# CAMBIO (Hito 2): limitación de ritmo real, como máximo 5 peticiones por segundo.
# ANTES: solo había esperas cuando una petición fallaba (reintentos); entre
# peticiones correctas no había pausa, así que se podía superar el límite.
# AHORA: antes de cada petición se espera lo necesario para que entre dos
# peticiones al mismo servidor pasen al menos 0,2 s.
MAX_REQUESTS_PER_SECOND = 5
_MIN_INTERVAL = 1.0 / MAX_REQUESTS_PER_SECOND
_last_request_at: dict[str, float] = {}

# Columnas del manifiesto (una fila por intento de descarga o reutilización).
MANIFEST_FIELDS = [
    "timestamp_utc",
    "run_id",
    "source_id",
    "url",
    "http_status",
    "bytes",
    "filename",
    "iso2",
    "iso3",
    "note",
]


# CAMBIO: función auxiliar de saneamiento y normalización de códigos ISO (ISO-3166).
# POR QUÉ: garantiza que valores no textuales o con espacios se limpien y cumplan la longitud esperada
# (2 para cca2 / iso2, 3 para cca3 / iso3) antes de construir URLs o cruces relacionales.
def _normalise_iso(value: object, length: int) -> str | None:
    if not isinstance(value, str):
        return None
    code = value.strip().upper()
    return code if len(code) == length and code.isalpha() else None


# CAMBIO: nombre del fichero raw según cómo trocea la fuente.
# ANTES: solo "{id}_{iso2}.{ext}" o "{id}.{ext}".
# AHORA:
#   - plantilla por país + indicador → "{id}_{indicator_id}_{iso2}.{ext}"
#   - plantilla por país             → "{id}_{iso2}.{ext}"
#   - plantilla solo de indicador    → "{id}_{indicator_id}.{ext}"   (Hito 2: universo)
#   - url fija                        → "{id}.{ext}"
# POR QUÉ: evita colisiones cuando una fuente descarga varios indicadores.
def _raw_name(
    source: Source, iso2: str | None = None, indicator_id: str | None = None
) -> str:
    if source.url_template and iso2:
        if indicator_id:
            return f"{source.id}_{indicator_id}_{iso2}.{source.format}"
        return f"{source.id}_{iso2}.{source.format}"
    if source.url_template and indicator_id:
        return f"{source.id}_{indicator_id}.{source.format}"
    return f"{source.id}.{source.format}"


# CAMBIO (Hito 2): espera mínima entre peticiones al mismo servidor (rate limit).
def _throttle(url: str) -> None:
    host = urlsplit(url).netloc
    previous = _last_request_at.get(host)
    if previous is not None:
        wait = _MIN_INTERVAL - (time.monotonic() - previous)
        if wait > 0:
            time.sleep(wait)
    _last_request_at[host] = time.monotonic()


# CAMBIO: cliente HTTP resiliente con reintentos y respeto de la cabecera Retry-After.
# ANTES: requests.get() directo sin reintentos.
# AHORA:
# 1. Reintenta con backoff exponencial (esperas de 1s, 2s, 4s y 8s: 5 intentos en total).
# 2. Respeta la cabecera 'Retry-After' enviada por servidores con limitación de tasa (rate limiting 429).
# 3. No reintenta errores 404 porque son definitivos (el recurso no existe).
# 4. (Hito 2) Cada intento pasa antes por _throttle(): ≤ 5 peticiones/s.
def _request_with_retry(url: str) -> requests.Response:
    for attempt in range(MAX_RETRIES + 1):
        _throttle(url)
        try:
            response = requests.get(
                url,
                timeout=TIMEOUT,
                headers={"User-Agent": USER_AGENT},
            )
        except requests.RequestException:
            # Timeouts y errores de conexión: transitorios, se reintentan.
            if attempt == MAX_RETRIES:
                raise
            time.sleep(2**attempt)
            continue

        if response.status_code == 404:
            # HTTP 404 es definitivo: no se reintenta una URL inexistente.
            return response
        if response.status_code not in RETRY_STATUSES:
            return response
        if attempt == MAX_RETRIES:
            return response

        retry_after = response.headers.get("Retry-After")
        wait_seconds = int(retry_after) if retry_after and retry_after.isdigit() else 2**attempt
        time.sleep(wait_seconds)

    raise RuntimeError("No se pudo completar la petición")


# CAMBIO: escritura atómica mediante archivos temporales con sufijo `.part`.
# ANTES: dest.write_bytes(content) directamente sobre el archivo final.
# AHORA: se escribe primero en `.part` y luego se renombra atómicamente con `replace()`.
# POR QUÉ: evita que una caída de red o cancelación del proceso deje archivos corruptos
# a medio escribir que luego el manifiesto o el transformer considerarían válidos.
def _write_response(dest: Path, content: bytes) -> tuple[int, int]:
    temp_dest = dest.with_suffix(dest.suffix + ".part")
    temp_dest.write_bytes(content)
    temp_dest.replace(dest)
    return 200, len(content)


# CAMBIO: descarga con reintento y validación previa antes de escribir en disco.
# ANTES: escribía resp.content incondicionalmente, incluso ante códigos de error HTTP 4xx o 5xx.
# AHORA: solo escribe el archivo en disco si el código es 200 y el contenido no está vacío (> 0 bytes).
def _download(url: str, dest: Path) -> tuple[int, int]:
    response = _request_with_retry(url)
    status = response.status_code
    nbytes = len(response.content)

    if status == 200 and nbytes > 0:
        return _write_response(dest, response.content)

    return status, nbytes


# CAMBIO: función auxiliar para modificar/añadir el parámetro de página en URLs con query string.
# POR QUÉ: permite iterar limpiamente por páginas sucesivas (page=2, page=3, ...) de una API REST.
def _page_url(url: str, page: int) -> str:
    parts = urlsplit(url)
    query = parse_qs(parts.query, keep_blank_values=True)
    query["page"] = [str(page)]
    return urlunsplit(parts._replace(query=urlencode(query, doseq=True)))


# CAMBIO: paginación completa para la API de World Bank.
# ANTES: solo se descargaba la primera respuesta (a menudo incompleta si había más de 1 página).
# AHORA:
# 1. Lee los metadatos de paginación (pages y total) de la primera página.
# 2. Pide el resto de páginas y concatena los registros.
# 3. (Hito 2) Comprueba que los registros guardados suman exactamente el `total`
#    que declara la API. Si no cuadra, lanza ValueError y no escribe nada en raw/
#    (el fallo queda anotado en el manifiesto).
# 4. Guarda el JSON consolidado [metadatos, registros] con todas las páginas.
def _download_worldbank(url: str, dest: Path) -> tuple[int, int]:
    first_response = _request_with_retry(url)
    status = first_response.status_code
    if status != 200 or not first_response.content:
        return status, len(first_response.content)

    try:
        response_json = getattr(first_response, "json", None)
        if not callable(response_json):
            # Compatibilidad con respuestas mínimas usadas en pruebas offline.
            return _write_response(dest, first_response.content)
        first_payload = response_json()
        metadata = first_payload[0]
        records = list(first_payload[1] or [])
        total_pages = int(metadata.get("pages", 1))
        expected_total = int(metadata.get("total", len(records)))
    except (ValueError, KeyError, IndexError, TypeError) as exc:
        raise ValueError(f"Respuesta World Bank inválida: {exc}") from exc

    for page in range(2, total_pages + 1):
        response = _request_with_retry(_page_url(url, page))
        if response.status_code != 200 or not response.content:
            return response.status_code, len(response.content)
        try:
            payload = response.json()
            records.extend(payload[1] or [])
        except (ValueError, KeyError, IndexError, TypeError) as exc:
            raise ValueError(f"Respuesta World Bank inválida en página {page}: {exc}") from exc

    # Contrato de completitud: lo guardado tiene que ser igual al total declarado.
    if len(records) != expected_total:
        raise ValueError(
            f"Paginación incompleta: {len(records)} registros descargados "
            f"frente a total={expected_total} declarado por la API ({url})"
        )

    combined = [metadata, records]
    return _write_response(dest, json.dumps(combined, ensure_ascii=False).encode("utf-8"))


# CAMBIO: lectura del manifiesto para soportar descargas incrementales.
# (Hito 2) El manifiesto es acumulativo y tiene varias filas por fichero (una por
# ejecución). Para decidir si se puede reutilizar un fichero se usa la ÚLTIMA fila
# con HTTP 200 de ese fichero.
def _load_manifest() -> dict[str, dict]:
    if not MANIFEST.exists():
        return {}

    latest_ok: dict[str, dict] = {}
    with MANIFEST.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            if row.get("filename") and str(row.get("http_status")) == "200":
                latest_ok[row["filename"]] = row
    return latest_ok


# CAMBIO (Hito 2): el manifiesto es acumulativo (se abre en modo "a", append).
# ANTES: se reescribía entero con mode="w" en cada ejecución y solo quedaba la
# última fila de cada fichero: se perdía el historial de ejecuciones anteriores.
# AHORA: cada descarga (o reutilización) añade una fila con su run_id; nunca se
# borra lo anterior. Si el fichero existía con otras columnas, se migra una vez.
def _append_manifest(row: dict) -> None:
    if MANIFEST.exists() and MANIFEST.stat().st_size > 0:
        with MANIFEST.open(encoding="utf-8", newline="") as f:
            header = next(csv.reader(f), [])
        if header != MANIFEST_FIELDS:
            _migrate_manifest()
        write_header = False
    else:
        write_header = True

    with MANIFEST.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_FIELDS, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def _migrate_manifest() -> None:
    """Reescribe una sola vez un manifiesto antiguo con las columnas actuales."""
    with MANIFEST.open(encoding="utf-8", newline="") as f:
        old_rows = list(csv.DictReader(f))
    with MANIFEST.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=MANIFEST_FIELDS, extrasaction="ignore", restval=""
        )
        writer.writeheader()
        writer.writerows(old_rows)


# CAMBIO: extracción dinámica del universo completo de países a partir del perfil global.
# ANTES: el universo de países estaba restringido a los 5 países iniciales del enunciado.
# AHORA:
# 1. Lee countries_profile.json para extraer todos los países (~250 países).
# 2. Normaliza y valida códigos cca2 e cca3 descartando registros sin códigos válidos.
# 3. Elimina duplicados para no realizar peticiones repetidas a los endpoints.
# (Hito 2) Solo se usa para fuentes que todavía trocean por país ({iso2}/{iso3});
# el Banco Mundial ya no lo necesita porque pide country/all.
def _load_all_countries() -> list[FocusCountry]:
    profile_path = RAW / "countries_profile.json"

    if not profile_path.exists():
        return []

    with profile_path.open(encoding="utf-8") as f:
        payload = json.load(f)

    countries = []
    seen: set[tuple[str, str]] = set()

    for item in payload:
        iso2 = _normalise_iso(item.get("cca2"), 2)
        iso3 = _normalise_iso(item.get("cca3"), 3)

        # CAMBIO: se descartan códigos ausentes o inválidos antes de crear URLs.
        if not iso2 or not iso3:
            continue

        key = (iso2, iso3)
        # CAMBIO: se evitan duplicados ISO2/ISO3 para no repetir peticiones.
        if key in seen or any(
            country.iso2 == iso2 or country.iso3 == iso3
            for country in countries
        ):
            continue
        seen.add(key)
        countries.append(FocusCountry(iso2=iso2, iso3=iso3))

    return countries


# CAMBIO: orquestación de extracción por fuente.
# ANTES:
# - Solo iteraba sobre los 5 países del foco inicial.
# - No soportaba fuentes con múltiples indicadores.
# - Descargaba todo de nuevo sin comprobar si ya existía en disco / manifiesto.
# - El manifiesto se escribía al final del todo; si se interrumpía, se perdía todo el tracking.
# AHORA:
# 1. Lista de indicadores desde la configuración (Source.indicator_specs()).
# 2. (Hito 2) Si la plantilla no lleva {iso2}/{iso3}, se hace una sola petición
#    de universo por indicador; si los lleva, una por país (formato antiguo).
# 3. Idempotencia: si el manifiesto registra un HTTP 200 para ese fichero y el
#    fichero existe con tamaño > 0, no se vuelve a descargar salvo con force=True.
# 4. Cada descarga o reutilización añade una fila al manifiesto en el momento.
def extract_source(
    source: Source, cfg: Config, *, force: bool = False, run_id: str | None = None
) -> list[dict]:
    """Descarga una fuente: una petición por indicador, o por país si la plantilla lo pide."""
    rows: list[dict] = []
    run_id = run_id or uuid.uuid4().hex
    manifest_rows = _load_manifest()

    indicator_ids: list[str | None] = [
        spec["indicator_id"] for spec in source.indicator_specs()
    ] or [None]

    if source.url_template and source.per_country:
        # Formato antiguo: el universo de países sale del perfil descargado.
        targets: list[FocusCountry | None] = list(_load_all_countries())
        if not targets:
            # Plan B solo para pruebas offline sin perfil en disco.
            targets = list(cfg.focus_countries)
    else:
        targets = [None]

    for country in targets:
        iso2 = country.iso2 if country else ""
        iso3 = country.iso3 if country else ""
        current_indicators = indicator_ids if source.url_template else [None]
        for indicator_id in current_indicators:
            url = source.resolve_url(
                iso2=iso2, iso3=iso3, indicator_id=indicator_id or ""
            )
            filename = _raw_name(source, iso2 or None, indicator_id)
            dest = RAW / filename
            previous = manifest_rows.get(filename)
            now = datetime.now(timezone.utc).isoformat()

            # CAMBIO: idempotencia y reanudación rápida.
            # Si el fichero ya existe en disco con tamaño > 0 y el manifiesto registra
            # un HTTP 200 anterior, no se repite la petición salvo con --force.
            if (
                not force
                and previous
                and str(previous.get("bytes", "0")).isdigit()
                and int(previous["bytes"]) > 0
                and dest.exists()
                and dest.stat().st_size > 0
            ):
                print(f"[extract] omitido, ya existe: {filename}")
                row = {
                    **previous,
                    "timestamp_utc": now,
                    "run_id": run_id,
                    "note": "reutilizado: ya descargado con HTTP 200 (idempotente)",
                }
                _append_manifest(row)
                rows.append(row)
                continue

            # World Bank usa su descargador con paginación; el resto, descarga simple.
            # Un error de red o un JSON inválido no tumba el ETL: se anota en la
            # columna `note` del manifiesto con http_status=0 y se sigue.
            try:
                if source.paginate == "worldbank" or source.id.startswith("worldbank"):
                    status, nbytes = _download_worldbank(url, dest)
                else:
                    status, nbytes = _download(url, dest)
                note = ""
            except (requests.RequestException, ValueError) as exc:
                status, nbytes = 0, 0
                note = str(exc)
            print(f"[extract] {filename}: HTTP {status}, {nbytes} bytes")

            row = {
                "timestamp_utc": now,
                "run_id": run_id,
                "source_id": source.id,
                "url": url,
                "http_status": status,
                "bytes": nbytes,
                "filename": filename,
                "iso2": iso2,
                "iso3": iso3,
                "note": note,
            }
            _append_manifest(row)
            rows.append(row)

    return rows


# CAMBIO: orden de extracción por dependencias, trazabilidad con run_id y flag --force.
# ANTES:
# - Iteraba las fuentes en el orden de definición sin tener en cuenta dependencias.
# - El manifiesto no incluía run_id y se reescribía al final con mode="w".
# AHORA:
# 1. Ejecuta primero 'countries_profile' para que countries_profile.json exista
#    antes de las fuentes que necesitan la lista de países.
# 2. Genera un run_id (UUID) por ejecución, compartido por todas las fuentes.
# 3. Admite `force` (CLI --force) para repetir descargas ya válidas.
# 4. (Hito 2) Ya no reescribe el manifiesto al final: las filas se van añadiendo
#    (append) durante la extracción, así se conserva el historial acumulado.
def run_extract(cfg: Config | None = None, *, force: bool = False) -> Path:
    cfg = cfg or load_config()
    ensure_dirs()
    run_id = uuid.uuid4().hex
    print(f"[extract] run_id={run_id}")

    profile_sources = [s for s in cfg.sources if s.id == "countries_profile"]
    other_sources = [s for s in cfg.sources if s.id != "countries_profile"]

    total = 0
    for source in profile_sources + other_sources:
        print(f"[extract] {source.id} — {source.name}")
        total += len(extract_source(source, cfg, force=force, run_id=run_id))

    print(f"[extract] {total} filas añadidas al manifiesto acumulativo → {MANIFEST}")
    return MANIFEST


if __name__ == "__main__":
    # CAMBIO: soporte para ejecutar extract independientemente con argumentos de línea de comandos.
    # ANTES: simplemente llamaba a run_extract().
    # AHORA: añade argparse para poder pasar --force desde terminal (ej. python -m src.extract --force).
    import argparse

    parser = argparse.ArgumentParser(description="Descarga las fuentes del ETL")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Repite también las descargas válidas registradas en el manifiesto",
    )
    run_extract(force=parser.parse_args().force)
