# Manual de Usuario — KDD λ Gaming

Este manual está dirigido a **usuarios finales** (analistas, jugadores
de prueba, evaluadores académicos) que quieran consultar las
recomendaciones, la analítica en vivo, los insights del KDD y los
agentes IA. **No** explica cómo instalar el sistema (eso está en
`05_manual_desarrollador.md`); asume que el stack ya está arrancado.

---

## 1. Acceso al sistema

| Recurso | URL |
|---|---|
| Dashboard React | http://localhost:5173 |
| API REST (FastAPI) | http://localhost:8000 |
| Swagger UI (docs interactiva) | http://localhost:8000/docs |
| Healthcheck | http://localhost:8000/health |
| Airflow (DAGs) | http://localhost:8090 |

> Si accedes desde otra máquina de la red, sustituye `localhost` por la
> IP o nombre del host (`nodo1`).

---

## 2. Dashboard — guía visual

El dashboard es una single-page application con cabecera fija + 6
pestañas. La cabecera muestra el **logo** y un **indicador de
conexión** (punto cian = API ok; rojo = API caída).

### 2.1 Pipeline KDD (cabecera)

Bajo el menú aparece una barra con las **5 fases del KDD**
(Selection → Preprocessing → Transformation → Mining → Evaluation),
cada una con su contador de registros procesados en la última
ejecución. Es informativa: ayuda a ubicarte en qué fase del pipeline
proviene cada dato.

### 2.2 Pestañas

| Pestaña | Fuente principal | Para qué sirve |
|---|---|---|
| ⚡ **Tiempo Real** | `gaming_kdd.player_windows` | Ver el número actual de jugadores por juego (Steam) en ventanas de 5 min, con sparklines y tier (`massive / popular / active / niche`). |
| 📊 **Histórico** | `gaming_kdd.game_stats_daily` | Ranking de los últimos 7-30 días por juego (`avg_players`, `growth_rate`, `review_score`). |
| 🎮 **Recomendaciones** | `gaming_recommender.user_recommendations` | Buscar recomendaciones para un `user_id`, ver el top global popular y buscar **juegos similares** por contenido. |
| 🤖 **Agentes IA** | `gaming_recommender.*` + Claude | Resúmenes en lenguaje natural del estado del pipeline y explicaciones por recomendación. |
| 🔬 **KDD Insights** | `kdd_insights` (ambos keyspaces) | Lista filtrable de alertas (`info / warning / alert / critical`) generadas por el speed o el batch. |
| λ **Arquitectura** | (estática) | Mini-diagrama explicativo de la arquitectura Lambda + métricas globales (#tablas, #endpoints…). |

---

## 3. Cómo usar la pestaña Recomendaciones

1. **Buscar por usuario**
   - Introduce un `user_id` en la caja de texto.
   - Pulsa "Buscar".
   - Si el usuario existe en el modelo, verás una lista de hasta 10 juegos con:
     - `rank` (1 = mejor)
     - `hybrid_score` (0-1)
     - `cf` (componente colaborativa) y `cb` (componente de contenido)
     - `source` (`batch` o `speed_layer`)
   - Si **no** hay datos, la API devuelve `cold_start: true` y se mostrarán las recomendaciones populares globales.

2. **Buscar juegos similares**
   - Escribe el título de un juego (en minúsculas, p. ej. `dota 2`).
   - El sistema mostrará los 10 juegos más parecidos por contenido textual (TF-IDF coseno).
   - El score `similarity` va de 0 (nada parecido) a 1 (idéntico).

3. **Métricas del modelo**
   - El panel inferior muestra las últimas 10 ejecuciones del KDD con `RMSE`, `Precision@10`, `Recall@10`, `users_with_recs`, `games_covered`.
   - Sirve para detectar drift entre días (si la precisión cae bruscamente, el agente Monitor lo señalará).

---

## 4. Cómo usar la pestaña Agentes IA

Cada panel se carga al entrar y se puede refrescar con el botón ↻.

| Agente | Qué muestra | Cuándo es útil |
|---|---|---|
| **Explorer** | Tamaño de los CSV, fecha de modificación, conteo aproximado de filas, tablas Hive con `count(*)`. | Verificar si los datos se han ingestado correctamente. |
| **KDD** | Recuentos de cada tabla del recomendador + las 5 últimas métricas + un resumen en lenguaje natural. | Auditoría rápida del estado global. |
| **Recommender** | Top-K para un `user_id` con explicación por ítem (qué juegos del historial llevaron a la recomendación). | Demostrar **explicabilidad** del recomendador. |
| **Monitor** | Detecta drift comparando la última métrica vs la baseline. Muestra recomendación de acción. Con `auto_trigger=true` lanza el DAG de re-entrenamiento. | Operación / mantenimiento. |

> Si **no** hay `ANTHROPIC_API_KEY` configurada, los agentes contestan
> con explicaciones generadas por reglas (mismo formato, sin LLM).

---

## 5. Cómo usar la API REST directamente

### 5.1 Endpoints públicos

```bash
# Healthcheck
curl http://localhost:8000/health

# Recomendaciones para un usuario
curl http://localhost:8000/api/recommendations/151603712 | jq .

# Top global popular (cold-start)
curl http://localhost:8000/api/recommendations/popular?limit=15 | jq .

# Juegos similares (lowercase)
curl http://localhost:8000/api/similar/dota%202?limit=10 | jq .

# Métricas del modelo
curl http://localhost:8000/api/recommender/metrics?limit=5 | jq .

# Insights filtrados por severidad y origen
curl 'http://localhost:8000/api/insights?severity=critical&source=both&limit=20' | jq .

# Resumen del pipeline original
curl http://localhost:8000/api/kdd/summary | jq .

# Real-time del recomendador
curl http://localhost:8000/api/recommender/realtime?limit=10 | jq .
```

### 5.2 Endpoints de agentes

```bash
curl http://localhost:8000/api/agent/explorer | jq .
curl http://localhost:8000/api/agent/kdd | jq .
curl http://localhost:8000/api/agent/recommend/151603712?top_k=5 | jq .
curl 'http://localhost:8000/api/agent/monitor?auto_trigger=false' | jq .
```

### 5.3 Inyectar interacciones (para probar el speed layer)

```bash
curl -X POST http://localhost:8000/api/ingest/interaction \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "151603712",
    "game_title": "dota 2",
    "event_type": "play",
    "hours": 12.5,
    "recommended": true
  }' | jq .
```

A los 30-60 segundos, una nueva consulta a
`/api/recommendations/151603712` debería mostrar entradas con
`source: "speed_layer"` por delante de las de `batch`.

---

## 6. Interpretar los datos

### 6.1 `hybrid_score`

| Rango | Color en UI | Interpretación |
|---|---|---|
| ≥ 0.75 | Cyan | Recomendación muy fuerte (alta señal CF y/o CB). |
| 0.50 – 0.75 | Amarillo | Recomendación razonable. |
| 0.25 – 0.50 | Rosa | Recomendación débil (suele venir de cold-start o de un usuario con poca historia). |
| < 0.25 | Gris | Marginal — útil solo para diversificar. |

### 6.2 `player_tier`

| Tier | Umbral `current_players` |
|---|---|
| `massive` | ≥ 100.000 |
| `popular` | 10.000 – 99.999 |
| `active`  | 1.000 – 9.999 |
| `niche`   | < 1.000 |

### 6.3 `severity` de insights

| Nivel | Significado |
|---|---|
| `info` | Solo informativo (cambios menores). |
| `warning` | Algo raro pero no crítico (caída de jugadores moderada). |
| `alert` | Pico/caída fuerte; revisar pronto. |
| `critical` | Anomalía grave (modelo degradado, drop > 50%, etc.). |

### 6.4 `source` en `user_recommendations`

| `source` | Generada por |
|---|---|
| `batch` | DAG nocturno (`gaming_recommender_batch_pipeline`). |
| `speed_layer` | `streaming_recommender.py` tras evento Kafka del usuario. |
| `popular_fallback` | Cold-start: usuario sin historial. |

---

## 7. Preguntas frecuentes (FAQ)

**P. He inyectado un evento por la API y no veo recomendaciones nuevas.**
R. Hay tres causas posibles:
1. El `user_id` no existe en el modelo ALS (cold-start). Usa un `user_id` que aparezca en `gaming_recommender.user_history_cache`.
2. El speed layer no está corriendo. Comprueba con `jps | grep StreamingRecommender` o el log `/tmp/kdd_recommender_streaming.log`.
3. Han pasado < 30 s (trigger del micro-batch).

**P. La pestaña Tiempo Real está vacía.**
R. El producer Steam (`steam_producer.py`) no está enviando datos.
Arráncalo con `cd 1_ingesta/producer && python steam_producer.py`.
También puede ser que falte `STEAM_API_KEY` en `.env`.

**P. El agente Monitor dice "Anthropic no configurado".**
R. Añade `ANTHROPIC_API_KEY=sk-...` a `.env` y reinicia FastAPI. Sin clave funcionará igual pero con respuestas heurísticas.

**P. ¿Cómo sé qué `user_id` puedo usar en pruebas?**
R. Ejecuta:
```bash
cqlsh -e "SELECT user_id FROM gaming_recommender.user_recommendations LIMIT 10;"
```
o usa los IDs que aparecen en la propia pestaña Histórico → Top usuarios (si está habilitada).

**P. La API responde 503.**
R. Cassandra no está arrancada o el keyspace correspondiente no existe. Comprueba `nodetool status` y vuelve a ejecutar `cqlsh -f 5_serving_layer/recommender_schema.cql`.

**P. ¿Qué hago si quiero forzar un re-entrenamiento ya?**
R. Tres opciones:
1. Lanzar el DAG en Airflow → `gaming_recommender_batch_pipeline` → "Trigger DAG".
2. Llamar al endpoint Monitor con `auto_trigger=true`.
3. Ejecutar manualmente `bash 4_batch_layer/recommender/submit_kdd.sh`.

---

## 8. Atajos de teclado del dashboard

(El dashboard no define atajos custom; usa los del navegador).

- `F5` / `Ctrl+R`: forzar recarga completa (resetea polling).
- `F12`: abrir devtools del navegador para ver llamadas a la API.

---

## 9. Datos de ejemplo

| Categoría | Ejemplo |
|---|---|
| `user_id` real (Kaggle Steam) | `151603712`, `59945701`, `5250`, `76767` |
| `game_title` (lowercase) | `dota 2`, `team fortress 2`, `counter-strike global offensive`, `garry's mod` |
| Severity de insights | `info`, `warning`, `alert`, `critical` |

> Los `user_id` reales solo existen tras correr el pipeline batch
> sobre los CSVs de Kaggle. Si has corrido el modo sintético, los
> `user_id` serán del estilo `synthetic_user_0001`.
