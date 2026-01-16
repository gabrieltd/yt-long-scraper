# Configuración y ejecución

Este proyecto utiliza **SQLite** para el almacenamiento de datos, lo que simplifica la configuración (no requiere servidor de base de datos externo).

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

El pipeline consta de 3 scripts principales que deben ejecutarse en orden:

### 1) Discovery (Playwright)

Busca videos en YouTube y guarda los resultados crudos en la tabla `videos_raw`.

```bash
python yt_discovery.py --query "documental" --headless
# opcional: limitar cantidad de resultados
python yt_discovery.py --query "documental" --limit 50 --headless
```

### 2) Normalización/Validación

Procesa los datos crudos, normaliza formatos (vistas, fechas, duración) y aplica validaciones. Guarda en `videos_normalized`.

```bash
python yt_normalization_validation.py
```

### 3) Enriquecimiento de canales (yt-dlp)

Lee los canales de videos validados y extrae información detallada usando yt-dlp. Guarda en `channels_raw` y `channel_videos_raw`.

```bash
python yt_channel_discovery.py
# opcional: limitar canales y videos
python yt_channel_discovery.py --limit-channels 50 --max-videos 25
```

## Buenas prácticas

- Empieza con límites pequeños (p. ej. `--limit 50`) hasta validar estabilidad.
- Si YouTube cambia el HTML o los selectores, Playwright puede fallar: ver [Troubleshooting](TROUBLESHOOTING.md).

