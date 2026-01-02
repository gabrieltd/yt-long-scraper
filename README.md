# YouTube Long Niche Scrapper

Proyecto en Python para descubrir y rankear canales de YouTube “prometedores” dentro de un nicho de videos largos.

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
```bash
python yt_discovery.py --query "documental" --headless
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

## Documentación

- [Configuración y ejecución](docs/CONFIGURACION.md)
- [Flujo del pipeline](docs/FLUJO.md)
- [Base de datos (tablas)](docs/BASE_DE_DATOS.md)
- [Troubleshooting](docs/TROUBLESHOOTING.md)

## Notas importantes

- El dashboard **no** ejecuta scraping ni recomputa métricas; solo lee `channels_score` + `channels_analysis`.
- El módulo de base de datos centraliza el esquema y las operaciones asyncpg: ver [db.py](db.py).
