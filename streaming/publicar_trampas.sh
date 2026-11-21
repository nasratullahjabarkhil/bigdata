#!/usr/bin/env bash
# Sesión 19. Reto sorpresa: dos eventos trampa en sensores.temp.
#   1) llega TARDE: ts de hace 20 minutos (watermark = 10 minutos);
#   2) esquema ROTO: campo "temp" en vez de "valor".
# Predecid qué hará cada query ANTES de lanzarlo.
set -euo pipefail
BROKER="${BROKER:-127.0.0.1:9092}"
TOPIC="${TOPIC:-sensores.temp}"
# CAMBIO: `date -d` solo existe en GNU/Linux (DataLab); en macOS se usa `date -v-20M`.
tarde=$(date -u -d '20 minutes ago' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null \
  || date -u -v-20M +%Y-%m-%dT%H:%M:%SZ)
ahora=$(date -u +%Y-%m-%dT%H:%M:%SZ)
printf '%s\n' \
  "{\"sensor\":\"s09\",\"valor\":30.0,\"ts\":\"${tarde}\"}" \
  "{\"sensor\":\"s09\",\"temp\":30.0,\"ts\":\"${ahora}\"}" \
| tee /dev/stderr | kafka-console-producer.sh \
  --bootstrap-server "$BROKER" --topic "$TOPIC"
