from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
RAW = DATA / "raw"
CLEAN = DATA / "clean"
SERVING = DATA / "serving"
CARDS = ROOT / "cards"
SOURCES_YAML = ROOT / "sources.yaml"
MANIFEST = RAW / "manifest.csv"


def ensure_dirs() -> None:
    for d in (RAW, CLEAN, SERVING, CARDS):
        d.mkdir(parents=True, exist_ok=True)
