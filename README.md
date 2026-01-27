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
- `yt-dlp`
- `asyncpg`
- `python-dotenv`
- `streamlit`
- `pandas`

## Quickstart

### 1) Levantar Postgres (Docker)

```bash
docker compose up -d
```

Esto usa [docker-compose.yaml](docker-compose.yaml) y expone Postgres en `localhost:5432`.

### 2) Configurar variables de entorno

Crea un `.env` (puedes copiar [`.env.example`](.env.example)). Variable clave:

- `DATABASE_URL=postgresql://yt_user:yt_password@localhost:5432/yt_discovery`

### 3) Instalar dependencias

```bash
python -m venv .venv
# activar venv (PowerShell)
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
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
python yt_channel_discovery.py
```

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
- [Configuración y ejecución](docs/CONFIGURACION.md)
- [Flujo del pipeline](docs/FLUJO.md)
- [Base de datos (tablas)](docs/BASE_DE_DATOS.md)
- [Troubleshooting](docs/TROUBLESHOOTING.md)

## Notas importantes

- El dashboard **no** ejecuta scraping ni recomputa métricas; solo lee `channels_score` + `channels_analysis`.
- El módulo de base de datos centraliza el esquema y las operaciones asyncpg: ver [db.py](db.py).
- Los parsers de normalización soportan automáticamente ambos idiomas (español e inglés).
