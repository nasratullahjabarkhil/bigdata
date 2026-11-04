"""Opción --repo. Tiene que vivir en la raíz: pytest la lee antes de recoger tests."""

from __future__ import annotations


def pytest_addoption(parser):
    parser.addoption(
        "--repo",
        action="store",
        default=None,
        help="Raíz del ETL a comprobar (sources.yaml + src/).",
    )
