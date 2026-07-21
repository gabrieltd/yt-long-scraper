# Base de datos (PostgreSQL)

El proyecto usa PostgreSQL mediante `asyncpg`. La conexión se configura con
`DATABASE_URL` y [db.py](../db.py) administra el pool, el esquema y las
operaciones transaccionales.

Cada entidad tiene tablas independientes por idioma, con sufijo `_es` o `_en`.
El esquema actual está diseñado para una base nueva; su creación es idempotente,
pero no convierte estructuras históricas.

## Discovery y staging

### `search_runs_{lang}`

Historial durable de búsquedas. `id` es un UUID nativo y cada ejecución conserva
`query`, `mode`, timestamps, `status`, `result_count` y `last_error`.

Sólo una fila con `status = 'success'` hace que una query cuente como ejecutada.
Una ejecución fallida permanece en el historial y puede volver a intentarse;
`--reprocess-duplicates` ignora el historial de éxitos.

### `discovery_videos_staging_{lang}`

Única tabla física para los resultados crudos y su normalización. Conserva el
texto necesario de YouTube y, en la misma fila, los valores estimados, el
resultado de validación y `normalized_at`.

Las vistas `videos_raw_{lang}` y `videos_normalized_{lang}` mantienen los nombres
y columnas de consulta anteriores. Las URLs de video y miniatura se derivan de
`video_id`; no se almacenan en staging.

### `channel_candidates_{lang}`

Cola durable con una sola fila por `channel_url` validado y su `first_seen`.
Normalización la alimenta de forma idempotente y el descubrimiento de canales
reclama trabajo desde esta tabla, no desde todas las filas de video.

Un resultado terminal elimina el candidato dentro de la misma transacción. Un
fallo transitorio conserva la fila para el siguiente ciclo. La purga del staging
pesado no borra esta cola.

## Datos persistentes de canales

### `channels_raw_{lang}`

Metadata del canal. Usa `id BIGINT GENERATED ALWAYS AS IDENTITY` como clave
primaria interna y mantiene `channel_url` como valor público único. Incluye
metadata de yt-dlp, `last_upload_date` y el estado del primer video.

### `channel_videos_raw_{lang}`

Videos rastreados del canal:

- `video_id TEXT` como clave primaria global.
- `channel_key BIGINT` como FK a `channels_raw.id`.
- `upload_date DATE`, `duration_seconds INTEGER`, `view_count BIGINT` y `title`.

`video_url` y `thumbnail_url` no se almacenan: la API las reconstruye como la
URL canónica de YouTube y `https://i.ytimg.com/vi/{video_id}/hqdefault.jpg`. La
API también convierte `upload_date` al formato `YYYYMMDD` esperado por la UI.

### `channel_stats_{lang}`

Tabla compacta de estadísticas exactas por `channel_key`: total de videos,
promedio, máximo y `view_counts BIGINT[]` ordenado para los filtros por rango.

Se actualiza dentro de la misma transacción que persiste canal, videos, estado y
liberación del claim. Por eso un canal queda visible inmediatamente y no depende
de un refresh al finalizar. `refresh_channel_stats()` queda sólo como operación
explícita de reconstrucción o reparación.

### Tablas auxiliares

- `channels_processed_{lang}`: estado terminal e idempotencia por URL.
- `channels_discovery_claims_{lang}`: claim, propietario y hora de adquisición;
  los claims vencidos son recuperables y una interrupción libera sólo los del
  propietario actual.
- `channel_relevance_{lang}`: relevancia, notas y etiquetas por `channel_key`.

## Finalización y almacenamiento

[yt_channel_finalize.py](../yt_channel_finalize.py) sólo trunca
`discovery_videos_staging_{lang}`, limpia claims que ya tienen estado procesado y
reporta cuántos candidatos durables quedan. No recalcula estadísticas ni borra
`search_runs` o `channel_candidates`.

Antes del truncado, cualquier búsqueda exitosa que aún conserve filas sin
normalizar pasa a `failed`. Así deja de contar como ejecutada y se vuelve a
intentar en el ciclo siguiente, sin conservar el staging pesado.

Para inspeccionar filas, heap, índices, TOAST y tamaño total sin modificar la
base:

```bash
python scripts/report_db_storage.py
python scripts/report_db_storage.py --ES --exact-rows
```
