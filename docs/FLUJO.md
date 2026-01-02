# Flujo del pipeline

Este documento describe el flujo real del proyecto según los scripts actuales.

## Resumen (de punta a punta)

1. [yt_discovery.py](../yt_discovery.py)
2. [yt_normalization_validation.py](../yt_normalization_validation.py)
3. [yt_channel_discovery.py](../yt_channel_discovery.py)
4. [yt_channel_analysis.py](../yt_channel_analysis.py)
5. [yt_channel_scoring.py](../yt_channel_scoring.py)
6. [dashboard.py](../dashboard.py)

## 1) Discovery: búsqueda en YouTube (Playwright)

Archivo: [yt_discovery.py](../yt_discovery.py)

- Abre `https://www.youtube.com/results?search_query=...`
- Aplica filtros UI:
  - "Este mes"
  - "Más de 20 minutos"
- Scrollea hasta “No hay más resultados”.
- Extrae por cada resultado:
  - `video_id`
  - `channels` (lista de canales encontrados en el item)
  - `duration`
  - `views_text`
  - `published_text`
  - `video_type` (`video` / `short`)
  - `is_multi_creator`
- Persiste en Postgres vía [db.py](../db.py):
  - `create_search_run()`
  - `insert_videos_raw()`
  - `finish_search_run()`

Salida: filas en `videos_raw`.

## 2) Normalización y validación (video-level)

Archivo: [yt_normalization_validation.py](../yt_normalization_validation.py)

- Lee videos sin normalizar: `db.fetch_unprocessed_videos_raw()`
- Normaliza:
  - `views_text` → `views_estimated`
  - `published_text` → `published_at_estimated` (UTC estimado)
  - `duration_text` → `duration_seconds_estimated`
- Valida (reglas mínimas):
  - duración `>= 1200s` (20 min)
  - publicado dentro de últimos 60 días
  - vistas `>= 1000`
- Inserta en `videos_normalized` con:
  - `validation_passed` y `validation_reason`

Salida: filas en `videos_normalized`.

## 3) Enriquecimiento de canal (yt-dlp, sin análisis)

Archivo: [yt_channel_discovery.py](../yt_channel_discovery.py)

- Selecciona canales candidatos desde `videos_normalized`:
  - `validation_passed = true`
  - excluye canales ya presentes en `channels_processed`
- Para cada canal:
  - Ejecuta `yt-dlp --dump-single-json --flat-playlist --playlist-end N --skip-download`
  - Persiste:
    - `channels_raw` (metadata de canal)
    - `channel_videos_raw` (últimos N videos del canal)
    - `channels_processed` (marca idempotente; `success` o `failed` en fallos permanentes)

Salida: `channels_raw`, `channel_videos_raw`, `channels_processed`.

## 4) Análisis de canal (post-enriquecimiento)

Archivo: [yt_channel_analysis.py](../yt_channel_analysis.py)

- Toma canales pendientes:
  - existen en `channels_raw`
  - aún no existen en `channels_analysis`
- Construye el “ciclo actual” por fechas de subida (detecta gap >= 60 días).
- Calcula métricas del ciclo (solo videos largos):
  - `cycle_long_videos_count`
  - `median_views`, `max_views`
  - `median_views_ratio`, `max_views_ratio` (views/subs)
- Decide `qualified` según reglas:
  - subs >= 100
  - total de long videos >= 3
  - ciclo long videos >= 3
  - al menos 2 videos con ratio >= 0.3
  - mediana ratio >= 0.25
- Inserta (una vez por canal) en `channels_analysis`.

Salida: `channels_analysis`.

## 5) Scoring (ranking)

Archivo: [yt_channel_scoring.py](../yt_channel_scoring.py)

- Lee filas desde `channels_analysis` (no excluye ya-scoreados).
- Si el canal no califica o faltan datos → score 0.
- Si califica → calcula componentes normalizados (0..1):
  - `s_perf` basado en `median_views_ratio`
  - `s_peak` basado en `max_views_ratio`
  - `s_consistency` basado en `cycle_long_videos_count`
  - `s_size` penaliza tamaño (más chicos puntúan más)
- `final_score` = suma ponderada de componentes.
- Upsert en `channels_score`.

Salida: `channels_score`.

## 6) Dashboard (Streamlit, solo lectura)

Archivo: [dashboard.py](../dashboard.py)

- Conecta a Postgres vía `DATABASE_URL` o `POSTGRES_DSN`.
- Consulta `channels_score JOIN channels_analysis`.
- Permite filtrar por score mínimo, subs máximos y mínimo de videos largos.
- Exporta CSV de la tabla visible.

Importante: el dashboard **no** corre scraping ni análisis; solo consulta.
