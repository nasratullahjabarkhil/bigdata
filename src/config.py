from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import yaml

from .paths import SOURCES_YAML


@dataclass
class Source:
    id: str
    name: str
    format: str = "json"
    role: str = "indicator"
    transformer: str = "generic_passthrough"
    url: str | None = None
    url_template: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)
    # CAMBIO (Hito 2): tipo de paginación de la fuente ("worldbank" o None).
    # POR QUÉ: el extract decide si recorre páginas y contrasta el `total` de la API
    # mirando la configuración, no el nombre de la fuente.
    paginate: str | None = None

    # CAMBIO: se añade el parámetro `indicator_id` para resolver plantillas de URL
    # que parametrizan no solo el país ({iso2}, {iso3}) sino también el indicador ({indicator_id}).
    # ANTES: resolve_url solo aceptaba iso2 e iso3 (def resolve_url(self, iso2="", iso3="")).
    # AHORA: permite soportar fuentes que descargan múltiples indicadores
    # usando una misma plantilla de URL (ej. .../indicator/{indicator_id}?...).
    def resolve_url(
        self, iso2: str = "", iso3: str = "", indicator_id: str = ""
    ) -> str:
        if self.url_template:
            return self.url_template.format(
                iso2=iso2, iso3=iso3, indicator_id=indicator_id
            )
        if self.url:
            return self.url
        raise ValueError(f"Fuente {self.id}: falta url o url_template")

    # CAMBIO (Hito 2): saber si la plantilla trocea por país.
    # Con {iso2}/{iso3} → una petición por país (formato antiguo, se conserva para
    # las pruebas de la Práctica 1). Sin ellos → una petición de universo.
    @property
    def per_country(self) -> bool:
        template = self.url_template or ""
        return "{iso2}" in template or "{iso3}" in template

    # CAMBIO (Hito 2): lista de indicadores de la fuente con su etiqueta y unidad.
    # Acepta, por orden: meta.indicators (lista de dicts), meta.indicator_ids
    # (lista de códigos) o meta.indicator_id (un código). Devuelve [] si no hay.
    def indicator_specs(self) -> list[dict[str, Any]]:
        meta = self.meta or {}
        specs: list[dict[str, Any]] = []
        for item in meta.get("indicators") or []:
            if isinstance(item, dict) and item.get("indicator_id"):
                specs.append(
                    {
                        "indicator_id": str(item["indicator_id"]),
                        "indicator_label": item.get("indicator_label"),
                        "unit": item.get("unit", meta.get("unit")),
                    }
                )
            elif isinstance(item, str) and item.strip():
                specs.append({"indicator_id": item.strip()})
        if not specs:
            codes = meta.get("indicator_ids") or [meta.get("indicator_id")]
            for code in codes:
                if code:
                    specs.append(
                        {
                            "indicator_id": str(code),
                            "indicator_label": meta.get("indicator_label"),
                            "unit": meta.get("unit"),
                        }
                    )
        return specs


@dataclass
class FocusCountry:
    iso2: str
    iso3: str


# CAMBIO (Hito 2): criterio de país y equivalencias de código leídos de sources.yaml.
# ANTES: no existía; el "criterio" era el if iso3 in focus_countries del transform.
# AHORA: el transform aplica estas reglas y anota lo descartado con su motivo.
@dataclass
class CountryRules:
    accept_if_independent: bool = True
    include_iso3: list[str] = field(default_factory=list)
    exclude_iso3: list[str] = field(default_factory=list)
    # {source_id: {código_en_la_fuente: iso3_canónico}}
    code_equivalences: dict[str, dict[str, str]] = field(default_factory=dict)

    def canonical(self, source_id: str, code: object) -> str | None:
        """Código de una fuente → iso3 canónico si hay equivalencia declarada."""
        if not isinstance(code, str):
            return None
        return (self.code_equivalences.get(source_id) or {}).get(code.strip().upper())


@dataclass
class Config:
    sources: list[Source]
    focus_countries: list[FocusCountry]
    # CAMBIO (Hito 2): reglas de país con valor por defecto, para que el código
    # antiguo (y las pruebas) que crean Config(sources, focus_countries) siga valiendo.
    countries: CountryRules = field(default_factory=CountryRules)


# NOTA (Hito 1): sources.yaml ya no declara `focus_countries`, así que aquí se
# devuelve una lista vacía. Se mantiene el campo por compatibilidad: las pruebas
# offline y el código antiguo pueden seguir pasando una lista de países a mano.
def load_config(path=SOURCES_YAML) -> Config:
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    sources = [Source(**s) for s in raw.get("sources", [])]
    focus = [FocusCountry(**c) for c in raw.get("focus_countries", [])]
    rules_raw = raw.get("countries") or {}
    rules = CountryRules(
        accept_if_independent=bool(rules_raw.get("accept_if_independent", True)),
        include_iso3=[str(c).upper() for c in rules_raw.get("include_iso3") or []],
        exclude_iso3=[str(c).upper() for c in rules_raw.get("exclude_iso3") or []],
        code_equivalences={
            str(src): {str(k).upper(): str(v).upper() for k, v in (mapping or {}).items()}
            for src, mapping in (rules_raw.get("code_equivalences") or {}).items()
        },
    )
    return Config(sources=sources, focus_countries=focus, countries=rules)
