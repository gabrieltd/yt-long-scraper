# Troubleshooting

## 1) Problemas con SQLite / youtube.db

Síntoma típico:
- `sqlite3.OperationalError: database is locked`

Solución:
- Asegúrate de no tener el archivo `youtube.db` abierto en otro programa (como un visor SQL) mientras corren los scripts.
- El sistema usa `WAL mode` para permitir concurrencia, pero bloqueos largos externos pueden afectar.

## 2) Playwright falla al hacer click en filtros / YouTube cambió UI

Síntomas:
- `TimeoutError` esperando selector
- No encuentra el botón “Filtros de búsqueda”

Causas comunes:
- YouTube cambió roles/nombres en el HTML.
- Diferente idioma en UI (el script fuerza locale es-MX).
- Te muestra un interstitial (consentimiento/cookies).

Acciones recomendadas:
- Ejecuta en modo con UI para observar:

```bash
python yt_discovery.py --query "documental" --headed
```

- Ajusta los selectores en [yt_discovery.py](../yt_discovery.py).

## 3) yt-dlp falla / timeouts / 429 / bloqueos

Síntomas:
- `yt-dlp timeout for ...`
- errores HTTP (incluyendo 429)

Mitigaciones:
- Reduce concurrencia (`MAX_WORKERS`) en [yt_channel_discovery.py](../yt_channel_discovery.py).
- Baja `--max-videos` y/o `--limit-channels`.
- Reintenta luego.

## 4) Normalización “rechaza todo” (muchos `validation_passed=false`)

Reglas actuales (mínimas):
- Duración >= 20 minutos
- Publicado dentro de últimos 60 días
- Vistas >= 1000

Si tu nicho requiere otras reglas, cambia la lógica en:
- [yt_normalization_validation.py](../yt_normalization_validation.py)

## 5) Problemas con entorno en Windows

- Activa venv en PowerShell:
  - `.\.venv\Scripts\Activate.ps1`
- Si PowerShell bloquea scripts:
  - `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`

## 6) Limpieza / reset

Si deseas comenzar de cero (borrar todos los datos):

1. Detén cualquier script en ejecución.
2. Elimina el archivo `youtube.db` del directorio raíz.
3. Vuelve a ejecutar cualquier script (la DB se recreará automáticamente).

