# PostgreSQL local via ngrok TCP for GitHub Actions

This setup lets GitHub-hosted runners use their compute while connecting to
PostgreSQL running on your PC. Your PC must stay on, Docker/Postgres must be
running, and the ngrok TCP tunnel must stay active for the whole workflow.

## 1) Start local PostgreSQL

Start the repo's Docker Compose PostgreSQL service:

```powershell
docker compose up -d
```

The service is defined in `docker-compose.yml`. By default it uses:

```text
USER=yt_user
PASSWORD=yt_password
DB=yt_discovery
PORT=5432
```

You can override those values in your local `.env`; keep `DATABASE_URL`
synced with whatever values you choose.

Confirm Postgres is listening locally:

```powershell
Test-NetConnection 127.0.0.1 -Port 5432
```

## 2) Start ngrok TCP

Run this on your PC:

```powershell
ngrok tcp 5432
```

ngrok will print a forwarding address like:

```text
Forwarding  tcp://0.tcp.ngrok.io:12345 -> localhost:5432
```

Keep this terminal open. If ngrok stops, GitHub Actions loses DB access.

## 3) Update the GitHub secret

Because this project uses a random ngrok TCP address, update the repository
secret every time you restart ngrok.

Use the ngrok host and port in `DATABASE_URL`:

```text
postgresql://USER:PASSWORD@0.tcp.ngrok.io:12345/DB_NAME
```

With the default Docker Compose values, that looks like:

```text
postgresql://yt_user:yt_password@0.tcp.ngrok.io:12345/yt_discovery
```

In GitHub:

1. Open your repository.
2. Go to `Settings` -> `Secrets and variables` -> `Actions`.
3. Edit or create the `DATABASE_URL` repository secret.
4. Paste the ngrok-based PostgreSQL URL.

Do not use `localhost` in GitHub secrets. On GitHub-hosted runners,
`localhost` means the runner machine, not your PC.

## 4) Run the workflow

Run the workflow from the Actions tab. The workflows include a database
preflight step:

```bash
python scripts/check_database_connection.py --expect-remote --timeout-seconds 20
```

If the tunnel is down, the DB is offline, or the secret still points to
`localhost`, the workflow fails early with a clear message.

## Checklist before running Actions

- Your PC is on and connected to the internet.
- Docker/Postgres is running locally.
- `ngrok tcp 5432` is running and still shows the forwarding address.
- GitHub secret `DATABASE_URL` uses the current ngrok host and port.
- The DB user/password in `DATABASE_URL` is valid.

## Safety notes

- Do not expose Postgres directly through your router/firewall.
- Prefer a dedicated DB user for CI instead of a superuser.
- Never commit real credentials to `.env`, docs, or workflow files.
- ngrok TCP endpoints can expose databases such as Postgres:
  <https://ngrok.com/docs/gateway/tcp>
- The `ngrok tcp` CLI forwards public TCP traffic to a local port:
  <https://ngrok.com/docs/agent/cli>
