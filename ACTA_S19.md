# Acta de la sesión 19 — `temp-stream`

Equipo: Nasratullah Jabarkhil.

> **Dónde se probó.** En local, no en DataLab: un Mac con Kafka 4.4.0
> (Homebrew, modo KRaft) escuchando en `127.0.0.1:9092`, con el topic
> `sensores.temp`, y PySpark 4.2.1 con el conector
> `spark-sql-kafka-0-10_2.13:4.2.1` (en DataLab es `_2.12:3.5.9`). Los
> checkpoints iban en una carpeta fija de pruebas, en vez de
> `~/checkpoints/temp-stream`, porque la ruta se pasa con `--checkpoint`.
> Ctrl+C se envió como SIGINT al proceso. **La prueba en DataLab está pendiente.**
> Salidas completas en `evidencias/s19/`.

**1. ¿Qué hace `spark/job_01.py` y por qué termina?**
Lee el Parquet de `data/clean/indicators` y cuenta las filas con dato por
indicador y década de tres maneras (RDD, DataFrame y SQL). Las compara con el
SQLite y escribe el resultado. Termina porque es *batch*: los datos ya están
cerrados en disco (el álbum de fotos). Cuando ha contado todo, no queda nada más.

**2. ¿Qué hace `temp-stream` y por qué no termina?**
Lee de Kafka sin parar y, en un micro-lote cada 5 s, cuenta los mensajes por
sensor y por minuto (`window` de 1 minuto, watermark de 10 minutos). Lo
imprime por consola en modo `update`. No termina porque es *streaming*: siempre
puede llegar otro mensaje a la cinta. Solo se para con Ctrl+C.

**3. ¿Qué guarda el checkpoint?**
El **progreso**, en `offsets/` y `commits/`: hasta qué mensaje de Kafka ha
leído y qué micro-lotes ha terminado. Al parar ponía `{"sensores.temp":{"0":7}}`.
El **estado**, en `state/`: las cuentas a medias de cada (minuto, sensor), para
no empezar de 0. `temp-nulos` no tiene `state/`, porque no agrega.

**4. ¿Qué pasó en la prueba de parar y arrancar?**
1.er arranque: 7 mensajes en el minuto 14:24 (hora local), con `s01`=3, `s02`=2
y `s03`=2. Ctrl+C y, con el programa parado, `publicar_control.sh s01 2` más un
mensaje de `s01` con hora 14:24:55, para probar el estado. 2.º arranque: siguió
en el micro-lote 2, no en el 0, y contó **solo los 3 nuevos**: `s01`=1 en 14:26,
`s01`=1 en 14:27 y `s01`=**4** en 14:24 (3 de antes + 1). El progreso pasó de
7 a 10. No repitió los 7 primeros y recordó la cuenta a medias, así que el
checkpoint funciona.

**5. Reto: los dos eventos trampa.**
Apuesta, escrita antes de lanzarlos: la tardía la tira la watermark; la rota
la cuenta `temp-stream` y la muestra `temp-nulos`. **Resultado:**
- Trampa 1 (`s09`, `valor` 30.0, 20 minutos tarde): no salió en ningún query.
  `temp-stream` la descartó porque la watermark solo espera 10 minutos, y
  `temp-nulos` no la ve porque trae temperatura.
- Trampa 2 (`s09`, con `temp` en vez de `valor`): `temp-stream` **sí la
  contó** (`s09`=1 en 14:30), porque `count()` cuenta mensajes y no mira la
  temperatura. `temp-nulos` la mostró: `s09 | NULL | 14:30:42`, porque
  `from_json` no falla, sino que deja `valor` vacío.

`temp-nulos` necesita su propio checkpoint porque es otro query: lleva su
propio progreso (acabó con 3 micro-lotes frente a 6 de `temp-stream`) y no tiene
estado. Si compartiera la carpeta, un query leería el progreso del otro y se
saltaría mensajes o los repetiría.

Defecto encontrado: el T5 del starter (`awaitAnyTermination()` + `except
KeyboardInterrupt`) paraba con trazas `py4j: reentrant call`, porque el
manejador de Ctrl+C de PySpark llama a Java en mitad de la espera. Se arregló
con una marca de Ctrl+C y una espera en tramos de 1 s.

## Salida de `pytest -q tests/s19`

Ejecutado en este repositorio, con la versión de `tests/s19` del repo del
profesor del 18/11 (commit `2c5ab71`), el 21/11/2026:

```text
$ python -m pytest -q tests/s19
...........                                                              [100%]
11 passed in 0.02s
```

Este test solo lee el código y el acta; no arranca Spark ni Kafka. La prueba
de parar y arrancar es la del punto 4.
