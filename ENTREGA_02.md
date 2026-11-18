# Entrega 2 — Del ejemplo al universo

Equipo: Nasratullah Jabarkhil (ver `CONTRIBUTORS.md`).

## 1. Predicción (escrita antes de la primera ejecución completa)

Este apartado se escribe y se commitea **antes** de ejecutar el pipeline
completo con la configuración nueva. Solo se han consultado los metadatos de
la API (`per_page=1`), no los datos.

### A. Bruto del Banco Mundial (lo comprueba el extract)

Metadatos consultados el 18/11/2026 (`country/all/indicator/<id>?per_page=1`):

| Indicador | `total` declarado |
| --- | ---: |
| NY.GDP.PCAP.CD | 17 556 |
| SP.POP.TOTL | 17 556 |
| SP.DYN.LE00.IN | 17 556 |
| SP.URB.TOTL.IN.ZS | 17 556 |
| NY.GDP.MKTP.CD | 17 556 |
| SP.POP.TOTL.FE.IN | 17 556 |

17 556 = 266 entidades × 66 años (1960–2025). Espero que `data/raw/` guarde
exactamente **17 556 registros por indicador** (105 336 en total); si no
cuadra, el extract tiene que fallar. Con `per_page=5000` serán 4 páginas por
indicador, 24 peticiones.

### B. Filas en `data/clean/indicators` (lo decide el transform)

- Criterio de país: 194 independientes del perfil + Kosovo y Palestina = **196**.
- De las 266 entidades del Banco Mundial, ~79 son agregados y otras ~20 son
  territorios que mi criterio rechaza. Espero casar **~193** países (la Ciudad
  del Vaticano, por ejemplo, no está en el Banco Mundial).
- Guardo las filas con valor nulo como nulo explícito, así que por indicador
  espero ~193 × 66 ≈ **12 700 filas**, y ~76 000 para los seis.
- Filas con valor no nulo: bastantes menos, porque los primeros años tienen
  huecos. Mi estimación: población ~95 % con dato, PIB per cápita ~70 %,
  urbana ~95 %, esperanza de vida ~90 %.
- Eurostat `demo_pjan` (sexo T, edad TOTAL): ~50 países europeos × hasta 66
  años, con muchos huecos → **~2 500 filas**.
- OWID esperanza de vida: ~190 países casados × ~75 años (1950–2023), más
  series históricas largas de algunos países → **~15 000 filas**.
- **Total esperado en clean/indicators: ~95 000 filas** (no es 6 × 17 556,
  porque se quitan agregados y territorios, y se suman Eurostat y OWID).
- Huérfanos: espero muy pocos en el Banco Mundial (algún micro-estado sin
  serie) y ~145 por Eurostat (todo lo que no es Europa).
