#!/usr/bin/env bash
# Sesión 19. Paso D del ensayo: publicar eventos de control en sensores.temp
# MIENTRAS el query temp-stream está parado.
#
#   bash streaming/publicar_control.sh            # sensor s01, hora actual (UTC)
#   bash streaming/publicar_control.sh s02 3      # 3 eventos del sensor s02
set -euo pipefail

SENSOR="${1:-s01}"
N="${2:-2}"
BROKER="${BROKER:-127.0.0.1:9092}"
TOPIC="${TOPIC:-sensores.temp}"

for i in $(seq 1 "$N"); do
  ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  valor=$(awk -v i="$i" 'BEGIN { printf "%.1f", 22 + i / 2 }')
  printf '{"sensor":"%s","valor":%s,"ts":"%s"}\n' "$SENSOR" "$valor" "$ts"
  sleep 1
done | tee /dev/stderr | kafka-console-producer.sh \
  --bootstrap-server "$BROKER" \
  --topic "$TOPIC"
