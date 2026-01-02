# Troubleshooting

## 1) Error: no se pudo conectar a Postgres / DSN no configurado

Síntoma típico:
- `RuntimeError: PostgreSQL DSN not configured. Set DATABASE_URL (or POSTGRES_DSN).`

Solución:
- Crea `.env` (puedes copiar [`.env.example`](../.env.example)).
- Asegura `DATABASE_URL` correcto.
- Si usas Docker: verifica que `docker compose ps` muestre Postgres healthy.

## 2) Playwright falla al hacer click en filtros / YouTube cambió UI

Síntomas:
- `TimeoutError` esperando selector
- No encuentra el botón “Filtros de búsqueda” o “Este mes” / “Más de 20 minutos”

Causas comunes:
- YouTube cambió roles/nombres en el HTML.
- Diferente idioma en UI.
- Te muestra un interstitial (consentimiento/cookies).

Acciones recomendadas:
- Ejecuta en modo con UI para observar:

```bash
python yt_discovery.py --query "documental" --headed
```

- Ajusta los nombres/roles en [yt_discovery.py](../yt_discovery.py) si la UI cambió.

## 3) yt-dlp falla / timeouts / 429 / bloqueos

Síntomas:
- `yt-dlp timeout for ...`
- errores HTTP (incluyendo 429)

Mitigaciones:
- Reduce concurrencia (`MAX_WORKERS`) en [yt_channel_discovery.py](../yt_channel_discovery.py).
- Baja `--max-videos` y/o `--limit-channels`.
- Reintenta luego (fallos transitorios no se marcan como processed, salvo fallos permanentes detectados).

## 4) El dashboard no muestra resultados

El dashboard solo lee de:
- `channels_score`
- `channels_analysis`

Checklist:
- ¿Corriste `yt_channel_analysis.py` y `yt_channel_scoring.py`?
- ¿Hay datos válidos en `videos_normalized` (con `validation_passed=true`)?
- ¿La conexión a Postgres apunta al DB correcto?

## 5) Normalización “rechaza todo” (muchos `validation_passed=false`)

Reglas actuales (mínimas):
- Duración >= 20 minutos
- Publicado dentro de últimos 60 días
- Vistas >= 1000

Si tu nicho requiere otras reglas, cambia la lógica en:
- [yt_normalization_validation.py](../yt_normalization_validation.py)

## 6) Problemas con entorno en Windows

- Activa venv en PowerShell:
  - `.\.venv\Scripts\Activate.ps1`
- Si PowerShell bloquea scripts:
  - `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`

## 7) Limpieza / reset

Reset total de BD (Docker):

```bash
docker compose down -v
docker compose up -d
```

Esto borra las tablas/datos porque elimina el volumen.
