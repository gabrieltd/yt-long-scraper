# Flujo del pipeline

El pipeline conserva tres etapas de procesamiento y una finalización liviana.
ES y EN usan el mismo flujo sobre tablas PostgreSQL separadas.

## 1. Discovery

[yt_discovery.py](../yt_discovery.py) busca videos con Playwright, aplica los
filtros solicitados y extrae ID, canal, duración, vistas y fecha publicada.

Cada intento crea una fila UUID en `search_runs_{lang}`. Los resultados se
insertan en `discovery_videos_staging_{lang}` y el run termina como `success` o
`failed`, con su cantidad y error. Sólo los runs exitosos evitan repetir una
query en ejecuciones posteriores.

## 2. Normalización y candidatos

[yt_normalization_validation.py](../yt_normalization_validation.py) completa las
columnas normalizadas de la misma fila física de staging:

- vistas estimadas;
- fecha estimada;
- duración en segundos;
- resultado y motivo de validación.

Por compatibilidad, `videos_raw_{lang}` y `videos_normalized_{lang}` son vistas
de esa tabla unificada. Cada canal que supera la validación se agrega una sola
vez a `channel_candidates_{lang}`, salvo que ya tenga estado terminal.

## 3. Descubrimiento y enriquecimiento de canales

[yt_channel_discovery.py](../yt_channel_discovery.py) reclama candidatos por
lotes mediante una operación atómica. Cada claim tiene propietario y puede ser
recuperado cuando vence.

El procesamiento por canal:

- extrae metadata y videos con el pool persistente de procesos de yt-dlp;
- permite `--ytdlp-mode subprocess` como modo de rollback;
- consulta RSS en paralelo con el primer video para corregir fechas recientes;
- obtiene y registra el primer video público;
- conserva fallos transitorios como pendientes para otro ciclo.

En un éxito, una sola transacción persiste `channels_raw`, hace el upsert masivo
de `channel_videos_raw`, actualiza `channel_stats`, marca el canal procesado,
libera su claim y elimina el candidato. Las claves internas entre estas tablas
son `BIGINT`; `channel_url` continúa siendo la identidad pública.

La tabla de estadísticas queda lista en esa transacción, así que la API y la UI
pueden mostrar el canal sin esperar una finalización global.

## 4. Finalización

La ejecución local de `yt_channel_discovery.py` inicializa el esquema y purga el
staging pesado de forma predeterminada. En GitHub Actions el esquema se crea una
sola vez; los workers usan `--skip-schema --skip-finalize` y un job final ejecuta:

```bash
python yt_channel_finalize.py --skip-schema --ES
```

Ese job corre incluso si falla un worker, trunca únicamente
`discovery_videos_staging_{lang}` y reporta la cola pendiente. Conserva el
historial de búsquedas y los candidatos que deben reintentarse; no refresca
estadísticas. Si encuentra resultados todavía sin normalizar, marca primero su
ejecución de búsqueda como fallida para que la query vuelva a ser elegible.

El enriquecimiento pendiente del primer video puede ejecutarse por separado:

```bash
python yt_first_video_enrichment.py --workers 5 --batch-size 50 --ES
```
