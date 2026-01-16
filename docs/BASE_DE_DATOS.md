# Base de datos (SQLite)

El proyecto utiliza **SQLite** como motor de base de datos local. El archivo de base de datos se crea automáticamente como `youtube.db` en la raíz del proyecto al ejecutar los scripts.

## Conexión

Se utiliza la librería `aiosqlite` para accesos asíncronos. No se requiere configuración de servidor ni credenciales.

El script [db.py](../db.py) maneja toda la lógica de conexión y esquema.

## Tablas

### `search_runs`

Registro de ejecuciones de búsqueda (discovery).

Campos:
- `id` (TEXT/UUID, PK)
- `query` (TEXT)
- `mode` (TEXT)
- `started_at` (TEXT/ISO8601)
- `finished_at` (TEXT/ISO8601)

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
- `is_multi_creator` (INTEGER/BOOLEAN)
- `discovered_at` (TEXT/ISO8601)

### `videos_normalized`

Normalización + validación de `videos_raw`.

Campos clave:
- `video_id` (TEXT, PK, FK → `videos_raw.video_id`)
- `channel_url` (TEXT)
- `query` (TEXT)
- `views_estimated` (INTEGER)
- `published_at_estimated` (TEXT/ISO8601)
- `duration_seconds_estimated` (INTEGER)
- `validation_passed` (INTEGER/BOOLEAN)
- `validation_reason` (TEXT)
- `normalized_at` (TEXT/ISO8601)

### `channels_raw`

Metadata de canal obtenida por `yt-dlp`.

Campos:
- `channel_url` (TEXT, PK)
- `channel_id` (TEXT)
- `channel_name` (TEXT)
- `subscriber_count` (INTEGER)
- `is_verified` (INTEGER/BOOLEAN)
- `extracted_at` (TEXT/ISO8601)

### `channel_videos_raw`

Últimos N videos del canal (sin descargar).

Campos:
- `channel_url` (TEXT)
- `video_id` (TEXT)
- `upload_date` (TEXT)
- `duration_seconds` (INTEGER)
- `view_count` (INTEGER)

PK compuesta: `(channel_url, video_id)`

### `channels_processed`

Marca idempotente de canales ya procesados.

Campos:
- `channel_url` (TEXT, PK)
- `processed_at` (TEXT/ISO8601)
- `status` (TEXT, default `success`)

### `channels_discovery_claims`

Coordinación de workers.

Campos:
- `channel_url` (TEXT, PK)
- `claimed_at` (TEXT/ISO8601)

## Operación idempotente

La mayoría de inserts usan `INSERT OR IGNORE` o `ON CONFLICT` para permitir re-ejecuciones seguras.
