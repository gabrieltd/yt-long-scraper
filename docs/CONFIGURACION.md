# Configuración y ejecución

Este proyecto usa Postgres como almacén central y scripts separados para cada etapa del pipeline.

## Variables de entorno

Se cargan desde `.env` mediante `python-dotenv` en la mayoría de scripts.

Variables aceptadas para conectar a Postgres:

- `DATABASE_URL` (preferida)
- `POSTGRES_DSN` (alternativa; varios módulos también la aceptan)

Ejemplo (ver [`.env.example`](../.env.example)):

```dotenv
DATABASE_URL=postgresql://yt_user:yt_password@localhost:5432/yt_discovery
```

## Postgres con Docker

El archivo [docker-compose.yaml](../docker-compose.yaml) levanta un Postgres 16 con:

- DB: `yt_discovery`
- User: `yt_user`
- Password: `yt_password`
- Puerto: `5432:5432`

Comandos:

```bash
docker compose up -d
docker compose ps
```

Para reiniciar limpio (borra datos):

```bash
docker compose down -v
```

## Instalación (Windows)

1) Crear y activar entorno virtual:

```bash
python -m venv .venv
# PowerShell
.\.venv\Scripts\Activate.ps1
```

2) Instalar dependencias:

```bash
pip install -r requirements.txt
```

3) Instalar navegadores de Playwright:

```bash
python -m playwright install
```

## Ejecución por etapas

### 1) Discovery (Playwright)

Guarda resultados de búsqueda en `videos_raw`.

```bash
python yt_discovery.py --query "documental" --headless
# opcional: limitar cantidad
python yt_discovery.py --query "documental" --limit 50 --headless
```

Notas:
- Aplica filtros UI ("Este mes" y "Más de 20 minutos") en la interfaz de YouTube.

### 2) Normalización/validación

Convierte texto → campos estimados y marca `validation_passed`.

```bash
python yt_normalization_validation.py
```

### 3) Enriquecimiento de canales (yt-dlp)

Lee canales candidatos (desde `videos_normalized`) y guarda:
- `channels_raw`
- `channel_videos_raw`
- `channels_processed`

```bash
python yt_channel_discovery.py
# opcional: limitar canales y cantidad de videos por canal
python yt_channel_discovery.py --limit-channels 50 --max-videos 25
```

### 4) Análisis de canal

Escribe una fila por canal en `channels_analysis`.

```bash
python yt_channel_analysis.py
# opcional
python yt_channel_analysis.py --limit 100
```

### 5) Scoring

Lee `channels_analysis` y upsertea `channels_score`.

```bash
python yt_channel_scoring.py
# opcional
python yt_channel_scoring.py --limit 100
```

### 6) Dashboard (Streamlit, solo lectura)

```bash
streamlit run dashboard.py
```

## Ejecución en lote de queries

El script [run_sequential_queries.py](../run_sequential_queries.py) ejecuta muchas queries (una por una) llamando a `yt_discovery.py`.

```bash
python run_sequential_queries.py
```

## Buenas prácticas

- Empieza con límites pequeños (p. ej. `--limit 50`) hasta validar estabilidad.
- Si YouTube cambia el HTML/roles, Playwright puede fallar: ver [Troubleshooting](TROUBLESHOOTING.md).
