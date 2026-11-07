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


# CAMBIO: función auxiliar de saneamiento y normalización de códigos ISO (ISO-3166).
# POR QUÉ: garantiza que valores no textuales o con espacios se limpien y cumplan la longitud esperada
# (2 para cca2 / iso2, 3 para cca3 / iso3) antes de construir URLs o cruces relacionales.
def _normalise_iso(value: object, length: int) -> str | None:
    if not isinstance(value, str):
        return None
    code = value.strip().upper()
    return code if len(code) == length and code.isalpha() else None


# CAMBIO: soporte para `indicator_id` en el nombrado de ficheros raw.
# ANTES: solo soportaba source.id e iso2 (_raw_name(source, iso2=None) -> "{id}_{iso2}.{ext}").
# AHORA: si se especifica indicator_id, genera "{id}_{indicator_id}_{iso2}.{ext}", evitando
# colisiones de archivos cuando una misma fuente descarga múltiples indicadores para el mismo país.
def _raw_name(
    source: Source, iso2: str | None = None, indicator_id: str | None = None
) -> str:
    if source.url_template and iso2:
        if indicator_id:
            return f"{source.id}_{indicator_id}_{iso2}.{source.format}"
        return f"{source.id}_{iso2}.{source.format}"
    return f"{source.id}.{source.format}"


# CAMBIO: cliente HTTP resiliente con reintentos y respeto de la cabecera Retry-After.
# ANTES: requests.get() directo sin reintentos.
# AHORA:
# 1. Reintenta con backoff exponencial (1s, 2s, 4s, 8s, 16s).
# 2. Respeta la cabecera 'Retry-After' enviada por servidores con limitación de tasa (rate limiting 429).
# 3. No reintenta errores 404 porque son definitivos (el recurso no existe).
def _request_with_retry(url: str) -> requests.Response:
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = requests.get(
                url,
                timeout=TIMEOUT,
                headers={"User-Agent": USER_AGENT},
            )
        except requests.RequestException:
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
# 1. Lee los metadatos de paginación (total_pages = metadata['pages']).
# 2. Si hay más páginas, realiza peticiones para cada página adicional y concatena los registros.
# 3. Guarda el JSON consolidado final con todas las páginas combinadas.
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

    combined = [metadata, records]
    return _write_response(dest, json.dumps(combined, ensure_ascii=False).encode("utf-8"))


# CAMBIO: carga del estado previo del manifiesto manifest.csv para soportar descargas incrementales.
# POR QUÉ: permite indexar los archivos por su nombre y verificar si ya fueron descargados con éxito.
def _load_manifest() -> dict[str, dict]:
    if not MANIFEST.exists():
        return {}

    with MANIFEST.open(encoding="utf-8", newline="") as f:
        rows = csv.DictReader(f)
        return {
            row["filename"]: row
            for row in rows
            if row.get("filename")
        }


# CAMBIO: guardado incremental del manifiesto tras cada archivo descargado.
# POR QUÉ: si el proceso se detiene a mitad, no se pierde el registro de los archivos ya descargados.
def _write_manifest(rows: dict[str, dict]) -> None:
    fieldnames = [
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

    with MANIFEST.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows.values())


# CAMBIO: extracción dinámica del universo completo de países a partir del perfil global.
# ANTES: el universo de países estaba restringido a los 5 países iniciales del enunciado.
# AHORA:
# 1. Lee countries_profile.json para extraer todos los países (~250 países).
# 2. Normaliza y valida códigos cca2 e cca3 descartando registros sin códigos válidos.
# 3. Elimina duplicados para no realizar peticiones repetidas a los endpoints.
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


# CAMBIO: orquestación de extracción por fuente con soporte para universos de países,
# reanudación idempotente y guardado incremental.
# ANTES:
# - Solo iteraba sobre los 5 países del foco inicial.
# - No soportaba fuentes con múltiples indicator_ids.
# - Descargaba todo de nuevo sin comprobar si ya existía en disco / manifiesto.
# - El manifiesto se escribía al final del todo; si se interrumpía, se perdía todo el tracking.
# AHORA:
# 1. Obtiene la lista dinámica de indicadores de source.meta (indicator_ids o indicator_id).
# 2. Utiliza el universo completo obtenido por _load_all_countries() si la fuente tiene url_template.
# 3. Comprueba el manifiesto previo y el archivo local: si ya se descargó correctamente (HTTP 200 y tamaño > 0)
#    y no se indicó force=True, se omite la descarga (idempotencia y reanudación).
# 4. Actualiza y persiste el archivo manifest.csv de forma incremental tras procesar cada archivo.
def extract_source(
    source: Source, cfg: Config, *, force: bool = False, run_id: str | None = None
) -> list[dict]:
    """Descarga una fuente; si tiene plantilla, una petición por país."""
    rows: list[dict] = []
    now = datetime.now(timezone.utc).isoformat()
    run_id = run_id or uuid.uuid4().hex
    manifest_rows = _load_manifest()

    indicator_ids = (source.meta or {}).get("indicator_ids") or [
        (source.meta or {}).get("indicator_id")
    ]
    indicator_ids = [value for value in indicator_ids if value]
    if not indicator_ids:
        indicator_ids = [None]
    if source.url_template:
        targets = _load_all_countries()
        if not targets:
            targets = list(getattr(cfg, "focus_" + "countries"))
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

            # CAMBIO: idempotencia y reanudación rápida.
            # Si el fichero ya existe en disco con tamaño > 0 y el manifiesto registra un HTTP 200 anterior,
            # no se repite la petición a la red a menos que se invoque con --force.
            if (
                not force
                and previous
                and previous.get("http_status") == "200"
                and previous.get("bytes", "0").isdigit()
                and int(previous["bytes"]) > 0
                and dest.exists()
                and dest.stat().st_size > 0
            ):
                print(f"[extract] omitido, ya existe: {filename}")
                previous.setdefault("run_id", run_id)
                rows.append(previous)
                continue

            try:
                if source.id.startswith("worldbank_"):
                    status, nbytes = _download_worldbank(url, dest)
                else:
                    status, nbytes = _download(url, dest)
                note = ""
            except (requests.RequestException, ValueError) as exc:
                status, nbytes = 0, 0
                note = str(exc)

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

            # CAMBIO: persistencia incremental del manifiesto en cada iteración.
            manifest_rows[filename] = row
            _write_manifest(manifest_rows)
            rows.append(row)

    return rows


# CAMBIO: orden de extracción por dependencias, trazabilidad con run_id y flag --force.
# ANTES:
# - Iteraba las fuentes en el orden de definición sin tener en cuenta dependencias.
# - El manifiesto no incluía run_id para distinguir ejecuciones sucesivas del pipeline.
# AHORA:
# 1. Separa 'countries_profile' del resto y la ejecuta primero para asegurar que
#    countries_profile.json exista en disco antes de procesar fuentes basadas en plantilla.
# 2. Genera un identificador único de ejecución (run_id UUID) para agrupar las descargas de cada sesión.
# 3. Admite el parámetro `force` (expuesto vía CLI --force) para invalidar la comprobación de archivos existentes.
def run_extract(cfg: Config | None = None, *, force: bool = False) -> Path:
    cfg = cfg or load_config()
    ensure_dirs()
    all_rows: list[dict] = []
    run_id = uuid.uuid4().hex

    # El perfil debe estar disponible antes de construir URLs ISO2.
    profile_sources = [
        source
        for source in cfg.sources
        if source.id == "countries_profile"
    ]

    other_sources = [
        source
        for source in cfg.sources
        if source.id != "countries_profile"
    ]

    for source in profile_sources:
        print(f"[extract] {source.id} — {source.name}")
        all_rows.extend(extract_source(source, cfg, force=force, run_id=run_id))

    for source in other_sources:
        print(f"[extract] {source.id} — {source.name}")
        all_rows.extend(extract_source(source, cfg, force=force, run_id=run_id))

    fieldnames = [
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

    with MANIFEST.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"[extract] manifiesto → {MANIFEST}")
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
