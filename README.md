# YouTube Long Niche Scrapper

Proyecto en Python para descubrir y rankear canales de YouTube “prometedores” dentro de un nicho de videos largos.
**✨ Nuevo:** Ahora con soporte bilingüe (Inglés/Español) y filtros configurables de búsqueda.
El pipeline (simplificado) es:

1. **Discovery (Playwright)**: scrapea resultados de búsqueda de YouTube y guarda videos en Postgres.
2. **Normalización/validación**: convierte textos (vistas, duración, publicado) a campos numéricos/fechas y filtra por reglas mínimas.
3. **Enriquecimiento de canal (yt-dlp)**: obtiene metadata real del canal + últimos N videos (sin descargar).
4. **Análisis de canal**: determina si el canal califica según desempeño reciente en videos largos.
5. **Scoring**: asigna un score determinístico para rankear canales.
6. **Dashboard (Streamlit, solo lectura)**: visualiza el ranking desde Postgres.

## Requisitos

- Python 3.10+ recomendado
- Docker (opcional pero recomendado) para levantar Postgres
- Acceso a internet (YouTube)

Dependencias principales (ver [requirements.txt](requirements.txt)):
- `playwright`
- `yt-dlp` desde el source local vendorizado en `yt-dlp/yt-dlp`
- `asyncpg`
- `python-dotenv`
- `requests`
- `streamlit`
- `pandas`

## Quickstart

### 1) Levantar Postgres (Docker)

```bash
docker compose up -d
```

Esto usa [docker-compose.yml](docker-compose.yml) y expone Postgres en `localhost:5432`.

### 2) Configurar variables de entorno

Crea un `.env` (puedes copiar [`.env.example`](.env.example)). Variable clave:

- `DATABASE_URL=postgresql://yt_user:yt_password@localhost:5432/yt_archive`
- `LOCAL_DATABASE_URL=postgresql://yt_user:yt_password@localhost:5432/yt_archive`
- `SUPABASE_DATABASE_URL=postgresql://...` (origen temporal de GitHub Actions)

Si cambias `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` o `POSTGRES_PORT`,
mantén `DATABASE_URL` sincronizada con esos valores.

### 3) Instalar dependencias

```bash
python -m venv .venv
# activar venv (PowerShell)
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m pip install -e "./yt-dlp/yt-dlp[default]"
python -m playwright install
```

### 4) Ejecutar pipeline

**Discovery (scrape de búsqueda):**

Básico (Español - por defecto):
```bash
python yt_discovery.py --query "documental" --headless
```

Con idioma y filtros (Inglés):
```bash
python yt_discovery.py --query "documentary" --EN --upload-date this_month --duration over_20 --features hd subtitles --headless
```

Con idioma y filtros (Español):
```bash
python yt_discovery.py --query "documental" --ES --upload-date this_month --duration over_20 --headless
```

Para acotar tanto la salida como el trabajo de red, usa `--limit`. Los artefactos
HTML y las capturas de diagnóstico son opcionales:
```bash
python yt_discovery.py --query "documental" --limit 50 --headless
python yt_discovery.py --query "documental" --debug-artifacts --headless
```

`--skip-schema` es una opción interna para runners que ya inicializaron la base.
Las ejecuciones directas no deben usarla salvo que el esquema esté preparado.

**Parallel Discovery (múltiples queries):**
```bash
# Español (usa queries.txt por defecto)
python run_parallel_discovery.py --instances 5 --batch-size 10 --ES

# Inglés (usa queries_en.txt por defecto)
python run_parallel_discovery.py --instances 5 --batch-size 10 --EN --upload-date this_week --duration over_20
```

**Batch Discovery (secuencial con batches):**
```bash
# Español - procesar todas las queries
python run_discovery.py --ES

# Inglés - procesar batch específico con filtros
python run_discovery.py --batch-size 50 --batch-index 0 --EN --duration over_20 --upload-date this_month

# Verificar batches pendientes
python run_discovery.py --batch-size 50 --check-batches --EN
```

**Normalización/validación:**
```bash
python yt_normalization_validation.py
```

**Enriquecimiento (yt-dlp a nivel canal):**
```bash
# usa el yt-dlp local vendorizado en yt-dlp/yt-dlp
python yt_channel_discovery.py

# concurrencia configurable; el lote de claims predeterminado es 2 x workers
python yt_channel_discovery.py --max-workers 6 --claim-stale-minutes 60

# process-pool persistente es el predeterminado; rollback disponible
python yt_channel_discovery.py --ytdlp-mode subprocess

# reintenta sólo canales cuya fecha del primer video quedó pendiente
python yt_first_video_enrichment.py --workers 5 --batch-size 50
```

**Uso de almacenamiento PostgreSQL (solo lectura):**
```bash
# ES y EN, con estimaciones rápidas de filas
python scripts/report_db_storage.py

# sólo ES y con conteos exactos (más costoso)
python scripts/report_db_storage.py --ES --exact-rows
```

**Archivar Supabase en PostgreSQL local:**
```bash
# Diagnóstico: no modifica ninguna base
python scripts/archive_supabase_to_local.py --all

# Copiar y verificar, conservando todavía los datos remotos
python scripts/archive_supabase_to_local.py --all --copy

# Copiar, verificar y liberar las tablas pesadas remotas
python scripts/archive_supabase_to_local.py --all --copy --truncate-after-verify
```

El último comando debe ejecutarse cuando GitHub Actions esté inactivo. Bloquea
las tablas persistentes con `NOWAIT`, rechaza claims activos y sólo ejecuta el
`TRUNCATE` después de verificar la copia. `channels_processed`, `search_runs`,
staging, candidatos y claims no se eliminan de Supabase. Para copiar debe usarse
la conexión directa o el pooler de sesión de Supabase (`:5432`), no el pooler
transaccional (`:6543`).

**Análisis de canal:**
```bash
python yt_channel_analysis.py
```

**Scoring:**
```bash
python yt_channel_scoring.py
```

**Dashboard (solo lectura):**
```bash
streamlit run dashboard.py
```

## Características Nuevas 🆕

### Soporte Bilingüe
- **`--EN`**: Interfaz en inglés (locale en-US, queries_en.txt)
- **`--ES`**: Interfaz en español (locale es-MX, queries.txt) - **Por defecto**

### Filtros de Búsqueda Configurables
- **`--upload-date`**: `last_hour`, `today`, `this_week`, `this_month`, `this_year`
- **`--duration`**: `under_4`, `4_20`, `over_20`
- **`--features`**: `live`, `4k`, `hd`, `subtitles`, `creative_commons`, `360`, `vr180`, `3d`, `hdr`, `location`, `purchased`
- **`--sort-by`**: `relevance`, `upload_date`, `view_count`, `rating`

Ver [BILINGUAL_USAGE.md](BILINGUAL_USAGE.md) para guía completa de uso.

## Documentación

- **[Uso Bilingüe y Filtros](BILINGUAL_USAGE.md)** ⭐ Nuevo
- [PostgreSQL local via ngrok TCP for GitHub Actions](docs/NGROK_POSTGRES_GITHUB_ACTIONS.md)
- [Configuración y ejecución](docs/CONFIGURACION.md)
- [Flujo del pipeline](docs/FLUJO.md)
- [Base de datos (tablas)](docs/BASE_DE_DATOS.md)
- [Troubleshooting](docs/TROUBLESHOOTING.md)

## Notas importantes

- El dashboard **no** ejecuta scraping ni recomputa métricas; solo lee `channels_score` + `channels_analysis`.
- El módulo de base de datos centraliza el esquema y las operaciones asyncpg: ver [db.py](db.py).
- Los parsers de normalización soportan automáticamente ambos idiomas (español e inglés).
