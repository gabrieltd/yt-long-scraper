# Base de datos (Postgres)

El esquema se crea de forma idempotente al ejecutar `db.init_db()` (ver [db.py](../db.py)).

## Conexión

El proyecto usa `asyncpg` y un pool global.

Variables aceptadas:
- `DATABASE_URL` (recomendada)
- `POSTGRES_DSN` (alternativa)

## Tablas

### `search_runs`

Registro de ejecuciones de búsqueda (discovery).

Campos:
- `id` (UUID, PK)
- `query` (TEXT)
- `mode` (TEXT)
- `started_at` (TIMESTAMPTZ)
- `finished_at` (TIMESTAMPTZ)

### `videos_raw`

Resultados “crudos” del scraping de búsqueda.

Campos clave:
- `video_id` (TEXT, PK)
- `search_run_id` (UUID, FK → `search_runs.id`)
- `query` (TEXT)
- `video_url` (TEXT) (derivado de `video_id`)
- `channel_url` (TEXT) (derivado del primer elemento en `channels` cuando existe)
- `duration_text` (TEXT)
- `views_text` (TEXT)
- `published_text` (TEXT)
- `thumbnail_url` (TEXT) (derivado de `video_id`)
- `video_type` (TEXT)
- `is_multi_creator` (BOOLEAN)
- `discovered_at` (TIMESTAMPTZ)

Índices destacados:
- `idx_videos_raw_channel_url`
- `idx_videos_raw_discovered_at`
- `idx_videos_raw_search_run_id`

### `videos_normalized`

Normalización + validación de `videos_raw`.

Campos clave:
- `video_id` (TEXT, PK, FK → `videos_raw.video_id`)
- `channel_url` (TEXT)
- `query` (TEXT)
- `views_estimated` (BIGINT)
- `published_at_estimated` (TIMESTAMPTZ)
- `duration_seconds_estimated` (INTEGER)
- `validation_passed` (BOOLEAN)
- `validation_reason` (TEXT)
- `normalized_at` (TIMESTAMPTZ)

Índices:
- `idx_videos_normalized_validation_passed`
- `idx_videos_normalized_normalized_at`

### `channels_raw`

Metadata de canal obtenida por `yt-dlp`.

Campos:
- `channel_url` (TEXT, PK)
- `channel_id` (TEXT)
- `channel_name` (TEXT)
- `subscriber_count` (BIGINT)
- `is_verified` (BOOLEAN)
- `extracted_at` (TIMESTAMPTZ)

### `channel_videos_raw`

Últimos N videos del canal (sin descargar).

Campos:
- `channel_url` (TEXT)
- `video_id` (TEXT)
- `upload_date` (TEXT) (suele venir como `YYYYMMDD`)
- `duration_seconds` (INTEGER)
- `view_count` (BIGINT)

PK compuesta:
- `(channel_url, video_id)`

Índice:
- `idx_channel_videos_raw_channel_url`

### `channels_processed`

Marca idempotente de canales ya procesados por `yt_channel_discovery.py`.

Campos:
- `channel_url` (TEXT, PK)
- `processed_at` (TIMESTAMPTZ)
- `status` (TEXT, default `success`) (por ejemplo `failed` para fallos permanentes)

Índice:
- `idx_channels_processed_processed_at`

### `channels_analysis`

Resultados del análisis (post-enriquecimiento).

Campos:
- `channel_url` (TEXT, PK)
- `subscriber_count` (INTEGER)
- `cycle_start_date` (DATE)
- `cycle_long_videos_count` (INTEGER)
- `median_views` (INTEGER)
- `max_views` (INTEGER)
- `median_views_ratio` (REAL)
- `max_views_ratio` (REAL)
- `qualified` (BOOLEAN)
- `analysis_reason` (TEXT)
- `analyzed_at` (TIMESTAMPTZ)

### `channels_score`

Score final y componentes para ranking.

Campos:
- `channel_url` (TEXT, PK)
- `final_score` (REAL)
- `s_perf` (REAL)
- `s_peak` (REAL)
- `s_consistency` (REAL)
- `s_size` (REAL)
- `scored_at` (TIMESTAMPTZ)

## Queries importantes (lógicas de selección)

- Candidatos a enriquecer (`yt_channel_discovery.py`):
  - `videos_normalized.validation_passed = true`
  - excluye `channels_processed`
  - nota: el contrato dice que **no agrupa por canal** (puede devolver duplicados)

- Canales pendientes de análisis (`yt_channel_analysis.py`):
  - existen en `channels_raw`
  - no existen en `channels_analysis`

- Canales para scoring (`yt_channel_scoring.py`):
  - lee `channels_analysis` (scoring puede re-ejecutarse)

## Operación idempotente

La mayoría de inserts usan `ON CONFLICT DO NOTHING` o upserts:

- `videos_raw`: `ON CONFLICT DO NOTHING` por `video_id`
- `videos_normalized`: `ON CONFLICT DO NOTHING` por `video_id`
- `channels_raw`: upsert por `channel_url`
- `channel_videos_raw`: upsert/batch idempotente por `(channel_url, video_id)`
- `channels_processed`: upsert por `channel_url`
- `channels_score`: upsert por `channel_url`

Esto permite reintentos sin “romper” el pipeline.
