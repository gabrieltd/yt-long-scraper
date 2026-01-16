# Flujo del pipeline

Este documento describe el flujo actual del proyecto, simplificado para las etapas de descubrimiento y colección de datos.

## Resumen

1. [yt_discovery.py](../yt_discovery.py)
2. [yt_normalization_validation.py](../yt_normalization_validation.py)
3. [yt_channel_discovery.py](../yt_channel_discovery.py)

## 1) Discovery: búsqueda en YouTube (Playwright)

Archivo: [yt_discovery.py](../yt_discovery.py)

- Abre `https://www.youtube.com/results?search_query=...`
- Aplica filtros UI:
  - "Este mes"
  - "Más de 20 minutos"
- Scrollea hasta “No hay más resultados”.
- Extrae por cada resultado:
  - `video_id`
  - `channels`
  - `duration`
  - `views_text`
  - `published_text`
  - `video_type`
  - `is_multi_creator`
- Persiste en SQLite vía [db.py](../db.py):
  - `create_search_run()`
  - `insert_videos_raw()`
  - `finish_search_run()`

Salida: filas en tabla `videos_raw`.

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

Salida: filas en tabla `videos_normalized`.

## 3) Enriquecimiento de canal (yt-dlp)

Archivo: [yt_channel_discovery.py](../yt_channel_discovery.py)

- Selecciona canales candidatos desde `videos_normalized`:
  - `validation_passed = true`
  - excluye canales ya presentes en `channels_processed`
- Para cada canal en paralelo (Workers):
  - Ejecuta `yt-dlp --dump-single-json --flat-playlist --playlist-end N --skip-download`
  - Persiste:
    - `channels_raw` (metadata de canal)
    - `channel_videos_raw` (últimos N videos del canal)
    - `channels_processed` (marca idempotente; `success` o `failed`)

Salida: tablas `channels_raw`, `channel_videos_raw`, `channels_processed`.

