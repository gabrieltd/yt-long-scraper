# PostgreSQL local con Docker y Tailscale

Esta carpeta levanta la base de datos PostgreSQL local que puede usar la aplicacion y, de forma controlada, el workflow `Full Pipeline (Local PostgreSQL via Tailscale)`.

Si es tu primera vez con Tailscale, sigue primero la [guia paso a paso de Tailscale](TAILSCALE_SETUP.md). Explica desde la cuenta hasta la prueba desde GitHub Actions.

La base de datos permanece en el volumen Docker `youtube-niche-postgres-data`. Para el conjunto actual de datos, reserva al menos **15 GB libres** para datos, indices, vistas materializadas y backups. El PC debe permanecer encendido, con Docker Desktop y Tailscale conectados durante una ejecucion de GitHub Actions.

## 1. Levantar PostgreSQL local

Requisitos:

- Docker Desktop instalado y en ejecucion.
- Tailscale instalado e iniciado en el PC si usas GitHub Actions.
- Cliente de PostgreSQL (`pg_dump`, `pg_restore`, `psql`) para una migracion desde Supabase.

Desde esta carpeta:

```powershell
Copy-Item .env.example .env
# Edita .env y cambia POSTGRES_PASSWORD por una contrasena larga y unica.
docker compose up -d
docker compose ps
```

El estado debe aparecer como `healthy`. La aplicacion usa una sola variable de conexion:

```text
DATABASE_URL=postgresql://yt_user:<URL_ENCODED_PASSWORD>@localhost:5432/yt_discovery
```

Si la contrasena contiene caracteres reservados de URL (`@`, `:`, `/`, `?`, `#`), codificalos antes de colocarla en `DATABASE_URL`.

Verifica la conexion sin revelar el DSN:

```powershell
$env:DATABASE_URL='postgresql://yt_user:<URL_ENCODED_PASSWORD>@localhost:5432/yt_discovery'
python scripts/check_db_connection.py
```

Para detener la base sin borrar datos:

```powershell
docker compose down
```

No ejecutes `docker compose down -v` salvo que quieras eliminar permanentemente la base local.

## 2. Seguridad de red

El compose publica `5432` para que un runner de GitHub Actions pueda alcanzarlo a traves de la IP de Tailscale. No necesitas, ni debes crear, un port-forwarding en el router.

### Windows Firewall

Abre solo TCP 5432 desde la red CGNAT usada por Tailscale:

```powershell
New-NetFirewallRule -DisplayName "PostgreSQL from Tailscale" `
  -Direction Inbound -Action Allow -Protocol TCP -LocalPort 5432 `
  -RemoteAddress 100.64.0.0/10
```

Comprueba la regla con:

```powershell
Get-NetFirewallRule -DisplayName "PostgreSQL from Tailscale"
```

### ACL de Tailscale

En la consola de administracion de tu tailnet, etiqueta el PC que aloja PostgreSQL, por ejemplo con `tag:local-db`, y limita el auth key usado por GitHub Actions a `tag:github-actions`. La ACL debe permitir unicamente:

```json
{
  "action": "accept",
  "src": ["tag:github-actions"],
  "dst": ["tag:local-db:5432"]
}
```

Usa un auth key **ephemeral**, preautorizado y restringido a `tag:github-actions`. Ajusta los nombres de tags a los que uses en tu tailnet. La ACL es la barrera principal: el firewall restringe la red, mientras que la ACL restringe que nodos pueden abrir la conexion.

Obtiene la IP o hostname Tailscale del PC con `tailscale ip -4` o desde la consola de Tailscale. Ese valor sera `TAILSCALE_DB_HOST`.

## 3. Secrets de GitHub Actions

En el repositorio de GitHub crea exclusivamente estos secrets para el workflow local:

| Secret | Uso |
| --- | --- |
| `TAILSCALE_AUTHKEY` | Auth key efimera y preautorizada, restringida al tag del runner. |
| `TAILSCALE_DB_HOST` | IP Tailscale o hostname MagicDNS del PC que ejecuta Docker. |
| `DATABASE_URL_LOCAL` | DSN hacia `TAILSCALE_DB_HOST:5432`, con la contrasena URL-encoded. |

Ejemplo de formato, sin usar valores reales:

```text
postgresql://yt_user:<URL_ENCODED_PASSWORD>@<TAILSCALE_DB_HOST>:5432/yt_discovery
```

El workflow `full-pipeline-local-db.yml` instala Tailscale antes de cada fase que accede a la base y ejecuta `scripts/check_db_connection.py` antes y despues del pipeline. Conserva los workflows actuales de Supabase como ruta de respaldo hasta completar una corrida local satisfactoria.

## 4. Migrar datos desde Supabase

Usa la conexion directa de Supabase en el puerto `5432`; no uses el pooler de `:6543` para `pg_dump` o `pg_restore`.

1. Deten los pipelines que puedan escribir en Supabase.
2. Exporta desde tu PC, sustituyendo los placeholders y guardando el dump fuera del repositorio:

```powershell
pg_dump "postgresql://postgres.<PROJECT_REF>:<PASSWORD>@db.<PROJECT_REF>.supabase.co:5432/postgres?sslmode=require" `
  -Fc --no-owner --no-privileges -f supabase_backup.dump
```

3. Levanta PostgreSQL local y restaura el dump:

```powershell
pg_restore --clean --if-exists --no-owner --no-privileges `
  -d "postgresql://yt_user:<URL_ENCODED_PASSWORD>@localhost:5432/yt_discovery" `
  supabase_backup.dump
```

4. Asegura las extensiones, indices y materialized views de la version actual del proyecto:

```powershell
$env:DATABASE_URL='postgresql://yt_user:<URL_ENCODED_PASSWORD>@localhost:5432/yt_discovery'
python setup_language_tables.py
python scripts/check_db_connection.py
```

5. Verifica los conteos y el tamano antes de apuntar un workflow a la nueva base:

```powershell
psql "$env:DATABASE_URL" -c "SELECT 'es' AS language, (SELECT COUNT(*) FROM channels_raw_es) AS channels, (SELECT COUNT(*) FROM channel_videos_raw_es) AS videos UNION ALL SELECT 'en', (SELECT COUNT(*) FROM channels_raw_en), (SELECT COUNT(*) FROM channel_videos_raw_en);"
psql "$env:DATABASE_URL" -c "SELECT pg_size_pretty(pg_database_size(current_database())) AS database_size;"
```

## 5. Recuperacion y troubleshooting

- Si el runner no conecta, confirma que el PC, Docker Desktop y Tailscale estan activos; luego revisa la ACL, el firewall y que `TAILSCALE_DB_HOST` sea la IP/hostname del PC correcto.
- Si Docker esta sano pero la aplicacion no conecta localmente, comprueba el DSN con `python scripts/check_db_connection.py` y `docker compose logs postgres`.
- Si quieres restaurar un backup distinto, detiene los workflows, restaura el dump y vuelve a ejecutar `python setup_language_tables.py`.
- No borres el volumen Docker al recuperar una base: usa una restauracion sobre la instancia existente. El borrado de volumen destruye todos los datos locales.
- Empieza la primera corrida local con pocos trabajos paralelos. El workflow local usa `3` por defecto para no saturar una conexion residencial ni el PC anfitrion.
