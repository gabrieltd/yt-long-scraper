# Base de datos (PostgreSQL)

El proyecto utiliza **PostgreSQL** como motor de base de datos.
Se requiere una instancia de PostgreSQL en ejecución y configurar la variable de entorno `DATABASE_URL` en el archivo `.env`.

## Conexión

Se utiliza la librería `asyncpg` para accesos asíncronos de alto rendimiento.
La conexión utiliza un *connection pool* gestionado globalmente.

El script [db.py](../db.py) maneja toda la lógica de conexión y esquema.

## Tablas

### `search_runs`

Registro de ejecuciones de búsqueda (discovery).

Campos:
- `id` (TEXT/UUID, PK)
- `query` (TEXT)
- `mode` (TEXT)
- `started_at` (TIMESTAMPTZ)
- `finished_at` (TIMESTAMPTZ)

### `videos_raw`

Resultados “crudos” del scraping de búsqueda.

Campos clave:
- `video_id` (TEXT, PK)
- `search_run_id` (TEXT, FK → `search_runs.id`)
- `query` (TEXT)
- `video_url` (TEXT)
- `channel_url` (TEXT)
- `duration_text` (TEXT)
- `views_text` (TEXT)
- `published_text` (TEXT)
- `thumbnail_url` (TEXT)
- `video_type` (TEXT)
- `is_multi_creator` (BOOLEAN)
- `discovered_at` (TIMESTAMPTZ)

### `videos_normalized`

Normalización + validación de `videos_raw`.

Campos clave:
- `video_id` (TEXT, PK, FK → `videos_raw.video_id`)
- `channel_url` (TEXT)
- `query` (TEXT)
- `views_estimated` (BIGINT)
- `published_at_estimated` (TIMESTAMPTZ)
- `duration_seconds_estimated` (BIGINT)
- `validation_passed` (BOOLEAN)
- `validation_reason` (TEXT)
- `normalized_at` (TIMESTAMPTZ)

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
- `upload_date` (TEXT)
- `duration_seconds` (BIGINT)
- `view_count` (BIGINT)

PK compuesta: `(channel_url, video_id)`

### `channels_processed`

Marca idempotente de canales ya procesados.

Campos:
- `channel_url` (TEXT, PK)
- `processed_at` (TIMESTAMPTZ)
- `status` (TEXT, default `success`)

### `channels_discovery_claims`

Coordinación de workers.

Campos:
- `channel_url` (TEXT, PK)
- `claimed_at` (TIMESTAMPTZ)

## Operación idempotente

La mayoría de inserts usan `ON CONFLICT DO NOTHING` o `ON CONFLICT DO UPDATE` para permitir re-ejecuciones seguras.
