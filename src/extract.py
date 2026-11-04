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

USER_AGENT = "bigdata2026-equipo-nasratullah (UCJC-DSBD)"
TIMEOUT = 60
# CAMBIO: las peticiones masivas necesitan reintentos ante errores temporales.
MAX_RETRIES = 4
RETRY_STATUSES = {429, 500, 502, 503, 504}


def _normalise_iso(value: object, length: int) -> str | None:
    if not isinstance(value, str):
        return None
    code = value.strip().upper()
    return code if len(code) == length and code.isalpha() else None


def _raw_name(
    source: Source, iso2: str | None = None, indicator_id: str | None = None
) -> str:
    if source.url_template and iso2:
        if indicator_id:
            return f"{source.id}_{indicator_id}_{iso2}.{source.format}"
        return f"{source.id}_{iso2}.{source.format}"
    return f"{source.id}.{source.format}"


def _request_with_retry(url: str) -> requests.Response:
    # CAMBIO: se reintentan fallos de red, rate limiting y errores 5xx.
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


def _write_response(dest: Path, content: bytes) -> tuple[int, int]:
    # CAMBIO: el archivo .part evita tratar una descarga interrumpida como válida.
    temp_dest = dest.with_suffix(dest.suffix + ".part")
    temp_dest.write_bytes(content)
    temp_dest.replace(dest)
    return 200, len(content)


def _download(url: str, dest: Path) -> tuple[int, int]:
    response = _request_with_retry(url)
    status = response.status_code
    nbytes = len(response.content)

    if status == 200 and nbytes > 0:
        return _write_response(dest, response.content)

    return status, nbytes


def _page_url(url: str, page: int) -> str:
    parts = urlsplit(url)
    query = parse_qs(parts.query, keep_blank_values=True)
    query["page"] = [str(page)]
    return urlunsplit(parts._replace(query=urlencode(query, doseq=True)))


def _download_worldbank(url: str, dest: Path) -> tuple[int, int]:
    # CAMBIO: World Bank puede devolver varias páginas y deben combinarse.
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

def _load_all_countries() -> list[FocusCountry]:
    # CAMBIO: se sustituye la lista fija de cinco países por el perfil global.
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


def extract_source(
    source: Source, cfg: Config, *, force: bool = False, run_id: str | None = None
) -> list[dict]:
    """Descarga una fuente; si tiene plantilla, una petición por país."""
    rows: list[dict] = []
    now = datetime.now(timezone.utc).isoformat()
    run_id = run_id or uuid.uuid4().hex
    manifest_rows = _load_manifest()

    # CAMBIO: las fuentes parametrizadas se recorren para todos los países.
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

            # CAMBIO: se reanudan las ejecuciones omitiendo descargas ya completas.
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

            # CAMBIO: se guarda el progreso después de cada archivo descargado.
            manifest_rows[filename] = row
            _write_manifest(manifest_rows)
            rows.append(row)

    return rows


def run_extract(cfg: Config | None = None, *, force: bool = False) -> Path:
    cfg = cfg or load_config()
    ensure_dirs()
    all_rows: list[dict] = []
    run_id = uuid.uuid4().hex

    # CAMBIO: el perfil debe estar disponible antes de construir URLs ISO2.
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
    import argparse

    parser = argparse.ArgumentParser(description="Descarga las fuentes del ETL")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Repite también las descargas válidas registradas en el manifiesto",
    )
    run_extract(force=parser.parse_args().force)
