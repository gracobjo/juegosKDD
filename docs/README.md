# Documentación — KDD λ Gaming

Índice de la documentación del proyecto. Cada documento es
autocontenido pero se recomienda leerlos en orden si te acercas al
proyecto por primera vez.

| # | Documento | Contenido |
|---|---|---|
| 1 | [Casos de uso](01_casos_de_uso.md) | 14 casos de uso (jugador, analista, data scientist, devops) con flujo principal y artefactos. |
| 2 | [Requisitos](02_requisitos.md) | Requisitos funcionales, no funcionales, de datos y de entorno. |
| 3 | [Diseño](03_diseno.md) | Arquitectura Lambda, mapeo a las 5 fases KDD, modelo de datos, ADRs y diagramas. |
| 4 | [Manual de usuario](04_manual_usuario.md) | Cómo usar el dashboard y los endpoints REST; FAQ. |
| 5 | [Manual del desarrollador](05_manual_desarrollador.md) | Setup, convenciones, extensión, despliegue, troubleshooting. |
| 6 | [Glosario y acrónimos](06_glosario_acronimos.md) | Diccionario A-Z de todas las siglas y términos técnicos. |

## Lectura sugerida según rol

| Rol | Recorrido recomendado |
|---|---|
| **Profesor / evaluador académico** | 1 → 3 → 6 |
| **Usuario final / analista** | 4 → 6 |
| **Desarrollador nuevo en el equipo** | 5 → 3 → 2 → 6 |
| **DevOps / SRE** | 5 § 5–7 → 2 § 4 → 3 § 8 |
| **Data scientist** | 3 § 3 y § 6 → 5 § 4.6 → 2 § 1.3 |

## Cómo regenerar la documentación

Toda la documentación está en Markdown plano y se versiona con el
código fuente. Para previsualizarla en local con render correcto:

```bash
# Opción 1: VS Code → Ctrl+Shift+V sobre cada .md
# Opción 2: pandoc + navegador
pandoc docs/03_diseno.md -o /tmp/diseno.html && xdg-open /tmp/diseno.html
# Opción 3: mkdocs (no incluido por defecto)
pip install mkdocs && mkdocs serve
```
