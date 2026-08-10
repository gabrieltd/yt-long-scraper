# Configuración y ejecución

El proyecto usa PostgreSQL. Configure `DATABASE_URL` en `.env`; por ejemplo:

```dotenv
DATABASE_URL=postgresql://yt_user:yt_password@localhost:5432/yt_archive
LOCAL_DATABASE_URL=postgresql://yt_user:yt_password@localhost:5432/yt_archive
SUPABASE_DATABASE_URL=postgresql://postgres.PROJECT_REF:PASSWORD@HOST:5432/postgres
```

## Instalación en Windows

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m pip install -e "./yt-dlp/yt-dlp[default]"
python -m playwright install
```

## Ejecución local

Las ejecuciones directas crean el esquema idempotente. Use `--EN` o `--ES` para
seleccionar el conjunto de tablas; ES es el valor predeterminado.

### 1. Discovery

```bash
python yt_discovery.py --query "documental" --headless --ES
python yt_discovery.py --query "documental" --limit 50 --headless --ES
python yt_discovery.py --query "documental" --debug-artifacts --headless --ES
```

Para ejecutar el archivo de queries y conservar el historial durable:

```bash
python run_discovery.py --ES
python run_discovery.py --batch-size 50 --batch-index 0 --EN
python run_discovery.py --ES --reprocess-duplicates
```

Una query sólo se omite si tiene al menos un `search_run` exitoso.
`--reprocess-duplicates` fuerza su ejecución aun cuando ya exista ese éxito.

### 2. Normalización y validación

```bash
python yt_normalization_validation.py --ES
```

La etapa actualiza el staging físico unificado y genera
`channel_candidates_{lang}`. Los nombres `videos_raw_{lang}` y
`videos_normalized_{lang}` son vistas de compatibilidad, no tablas duplicadas.

### 3. Descubrimiento de canales

```bash
python yt_channel_discovery.py --ES
python yt_channel_discovery.py --max-workers 6 --claim-batch-size 12 --claim-stale-minutes 60 --ES
python yt_channel_discovery.py --ytdlp-mode subprocess --ES
```

El modo predeterminado de yt-dlp es `process-pool`. El lote de claims por defecto
es `2 x --max-workers`. La persistencia actualiza canal, videos y estadísticas
atómicamente, por lo que no se necesita un refresh al terminar.

La ejecución local purga automáticamente el staging pesado. Para conservarlo
temporalmente puede usarse `--skip-finalize`.

GitHub Actions añade `--skip-first-video`; así los workers remotos no consultan
Innertube y el dato queda pendiente para `yt_first_video_enrichment.py` local.

### Operaciones independientes

```bash
# Reintentar fechas pendientes del primer video
python yt_first_video_enrichment.py --workers 5 --batch-size 50 --ES

# Truncar staging pesado y reportar candidatos pendientes
python yt_channel_finalize.py --ES

# Reporte de almacenamiento de ES y EN (sólo lectura)
python scripts/report_db_storage.py

# Diagnóstico del archivo remoto -> local (sin escrituras)
python scripts/archive_supabase_to_local.py --all

# Copia verificable sin borrar el origen
python scripts/archive_supabase_to_local.py --all --copy

# Drenar las tablas pesadas después de verificar
python scripts/archive_supabase_to_local.py --all --copy --truncate-after-verify

# Conteos exactos sólo para ES; puede ser más costoso
python scripts/report_db_storage.py --ES --exact-rows
```

La URL de Supabase para `--copy` debe ser directa o de pooler de sesión en el
puerto 5432. El puerto 6543 sólo se admite en el diagnóstico porque una copia
completa mantiene una transacción consistente durante más tiempo.

## GitHub Actions y runners preparados

Los workflows crean el esquema una sola vez. Los jobs paralelos usan
`--skip-schema` para evitar DDL concurrente y los workers de canal añaden
`--skip-finalize`. Un único job final, ejecutado aunque falle un worker, llama a
`yt_channel_finalize.py --skip-schema` para purgar staging; las estadísticas ya
fueron actualizadas por cada transacción de canal.

No use `--skip-schema` en una ejecución directa si las tablas del idioma todavía
no existen.

## Buenas prácticas

- Empiece con límites pequeños para validar conectividad y selectores.
- Use `scripts/report_db_storage.py` antes de atribuir tamaño a una tabla: separa
  heap, índices y TOAST.
- No trunque `channel_candidates` para liberar staging; esa tabla es la cola
  durable de reintentos.
- Si YouTube cambia HTML o selectores, consulte [Troubleshooting](TROUBLESHOOTING.md).
