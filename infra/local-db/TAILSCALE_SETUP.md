# Guia desde cero: Tailscale para la base local

Esta guia conecta una ejecucion de GitHub Actions con PostgreSQL que corre en tu PC. Tailscale crea una red privada entre ambos. No abre tu router, no requiere IP publica y no cambia como funciona la aplicacion.

Al terminar tendras esta ruta:

```text
GitHub Actions -> red privada Tailscale -> tu PC Windows -> Docker -> PostgreSQL:5432
```

Haz los pasos en este orden. No avances al siguiente hasta que la comprobacion del actual funcione.

## Antes de empezar

Necesitas:

- Una cuenta de GitHub con acceso al repositorio.
- Tu PC Windows encendido cuando vaya a correr el pipeline.
- Docker Desktop instalado y funcionando.
- Acceso de administrador a la cuenta de Tailscale que vas a crear.

Nunca pegues una contrasena de PostgreSQL ni un auth key en un archivo del repositorio, un chat, un commit o una captura de pantalla.

## 1. Crear tu red privada de Tailscale

1. Abre [login.tailscale.com](https://login.tailscale.com/).
2. Inicia sesion. Puedes usar tu cuenta de GitHub, Google, Microsoft o correo.
3. Acepta la creacion de tu red privada, llamada *tailnet*.
4. Se abrira el Admin Console. Dejalo abierto en otra pestana: lo usaras para registrar equipos y permisos.

Una tailnet es solo una red privada. Las IP que veras, normalmente `100.x.x.x`, no son IP publicas de Internet.

## 2. Instalar Tailscale en el PC que tiene Docker

1. En tu PC Windows abre [tailscale.com/download/windows](https://tailscale.com/download/windows).
2. Descarga e instala Tailscale. Acepta el adaptador de red cuando Windows lo pida.
3. Abre Tailscale desde el menu Inicio y presiona **Log in**.
4. El navegador pedira autorizar el PC en la misma cuenta que usaste en el paso 1. Acepta.
5. Abre PowerShell y ejecuta:

```powershell
tailscale status
tailscale ip -4
```

El primer comando debe mostrar tu PC conectado. El segundo imprime una IP parecida a `100.75.12.34`.

Guarda esa IP temporalmente: sera el valor de `TAILSCALE_DB_HOST`. Mas adelante puedes reemplazarla por el nombre MagicDNS si prefieres, pero la IP es mas facil para la primera configuracion.

Si PowerShell dice que `tailscale` no existe, cierra y vuelve a abrir PowerShell. Si sigue igual, reinicia Windows despues de instalar la aplicacion.

## 3. Darle una etiqueta al PC de la base

La etiqueta permite escribir una regla que diga: "solo los runners de GitHub pueden hablar con este PC por el puerto 5432".

1. En el Admin Console abre **Access controls**.
2. Busca el bloque `tagOwners` de la configuracion existente. Si no existe, agrega este bloque al objeto principal:

```json
"tagOwners": {
  "tag:local-db": ["autogroup:admin"],
  "tag:github-actions": ["autogroup:admin"]
}
```

3. Guarda la configuracion. Tailscale valida el formato antes de aplicarlo.
4. Abre **Machines**, busca tu PC Windows, abre el menu de tres puntos y selecciona la opcion para editar sus ACL tags.
5. Agrega `tag:local-db` y guarda.

La alternativa por PowerShell, una vez creado `tagOwners`, es:

```powershell
tailscale set --advertise-tags=tag:local-db
```

Vuelve a **Machines** y confirma que el PC aparece con `tag:local-db`.

## 4. Crear la regla de acceso

En **Access controls**, agrega esta regla dentro del arreglo `acls` existente:

```json
{
  "action": "accept",
  "src": ["tag:github-actions"],
  "dst": ["tag:local-db:5432"]
}
```

Importante: las ACL de Tailscale son reglas de permiso acumulativas. Si tu configuracion ya tiene una regla amplia como esta:

```json
{ "action": "accept", "src": ["*"], "dst": ["*:*" ] }
```

esa regla sigue permitiendo a todos los dispositivos llegar al PC. Para que el aislamiento sea real, reemplaza el permiso amplio por reglas especificas para los dispositivos que realmente necesites conservar. No hay una regla `deny` que pueda anular un `accept` amplio.

Un ejemplo minimo para una tailnet usada solo en este proyecto es:

```json
{
  "tagOwners": {
    "tag:local-db": ["autogroup:admin"],
    "tag:github-actions": ["autogroup:admin"]
  },
  "acls": [
    {
      "action": "accept",
      "src": ["tag:github-actions"],
      "dst": ["tag:local-db:5432"]
    }
  ]
}
```

Guarda y espera a que el Admin Console muestre que la politica se aplico sin errores.

## 5. Crear el auth key para GitHub Actions

El auth key permite que un runner temporal de GitHub se una a tu tailnet durante un workflow. El runner recibira `tag:github-actions` automaticamente.

1. En el Admin Console abre **Settings** y luego **Keys** o **Auth keys**.
2. Crea un auth key nuevo con estas opciones:
   - **Reusable**: activado. Es necesario porque el pipeline usa varios jobs y puede ejecutar varios runners.
   - **Ephemeral**: activado. El runner desaparece de Tailscale cuando el workflow termina.
   - **Pre-authorized**: activado. Asi no tendras que aprobar cada runner manualmente.
   - **Tags**: agrega `tag:github-actions`.
   - **Expiration**: usa una fecha razonable y anota cuando debes renovarla.
3. Crea la key y copiala inmediatamente. Tailscale solo la muestra una vez.

No la pongas aun en un archivo local. Va como secret de GitHub en el paso 8.

## 6. Levantar PostgreSQL y limitar Windows Firewall

Sigue primero [README.md](README.md) para crear `infra/local-db/.env` y levantar Docker:

```powershell
cd infra/local-db
Copy-Item .env.example .env
# Edita .env, define una contrasena larga y unica.
docker compose up -d
docker compose ps
```

El estado de PostgreSQL debe ser `healthy`. Luego, desde una ventana de PowerShell **como administrador**, permite solamente conexiones Tailscale al puerto de PostgreSQL:

```powershell
New-NetFirewallRule -DisplayName "PostgreSQL from Tailscale" `
  -Direction Inbound -Action Allow -Protocol TCP -LocalPort 5432 `
  -RemoteAddress 100.64.0.0/10
```

Comprueba primero que PostgreSQL responde desde tu propio PC:

```powershell
Test-NetConnection -ComputerName 127.0.0.1 -Port 5432
```

El resultado debe contener `TcpTestSucceeded : True`.

No crees reglas en el router. No configures port forwarding. Tailscale usa conexiones salientes y su red privada.

## 7. Crear los tres secrets en GitHub

En GitHub abre tu repositorio y entra a **Settings -> Secrets and variables -> Actions -> New repository secret**. Crea exactamente estos tres:

| Nombre | Valor |
| --- | --- |
| `TAILSCALE_AUTHKEY` | El auth key creado en el paso 5. |
| `TAILSCALE_DB_HOST` | La IP obtenida con `tailscale ip -4`, por ejemplo `100.75.12.34`. |
| `DATABASE_URL_LOCAL` | `postgresql://yt_user:<PASSWORD_URL_ENCODED>@<TAILSCALE_DB_HOST>:5432/yt_discovery` |

Para `DATABASE_URL_LOCAL`, reemplaza ambos placeholders. Si tu contrasena contiene `@`, `:`, `/`, `?` o `#`, debes codificarla para URL antes de usarla. Por ejemplo, `mi@clave` se convierte en `mi%40clave`.

Los tres nombres coinciden con [full-pipeline-local-db.yml](../../.github/workflows/full-pipeline-local-db.yml). No cambies los nombres salvo que tambien cambies el workflow.

## 8. Probar desde GitHub sin lanzar scraping

En GitHub abre la pestana **Actions**, elige **Test Local PostgreSQL Connection** y presiona **Run workflow**. Este workflow no descubre ni modifica datos; solo se conecta a Tailscale y ejecuta la comprobacion de PostgreSQL.

Revisa estos pasos:

1. **Connect to Tailscale** debe finalizar correctamente y hacer ping al host.
2. **Check local PostgreSQL connection** debe mostrar `Database connection succeeded`.

Si ambos terminan bien, ejecuta **Full Pipeline (Local PostgreSQL via Tailscale)**. Empieza con `language: ES`, `max_jobs: 1` y filtros vacios; luego puedes aumentar el paralelismo gradualmente.

## Problemas comunes

### GitHub no puede hacer ping al host

- Confirma que tu PC aparece como conectado en **Machines**.
- Confirma que `TAILSCALE_DB_HOST` es la IP de `tailscale ip -4` del PC correcto.
- Revisa que el auth key tenga `tag:github-actions` y que la ACL permita ese tag hacia `tag:local-db:5432`.
- Verifica que el PC mantiene Tailscale abierto y conectado durante el workflow.

### El ping funciona, pero falla la conexion a la base

- Ejecuta `docker compose ps`: PostgreSQL debe estar `healthy`.
- Ejecuta `Test-NetConnection -ComputerName 127.0.0.1 -Port 5432` en el PC.
- Comprueba la regla de firewall con `Get-NetFirewallRule -DisplayName "PostgreSQL from Tailscale"`.
- Revisa que `DATABASE_URL_LOCAL` tenga la misma contrasena que `infra/local-db/.env` y que apunte al puerto `5432`.

### El auth key dejo de funcionar

Normalmente expiro. Crea uno nuevo con las mismas opciones y actualiza solo `TAILSCALE_AUTHKEY` en GitHub Secrets. No necesitas cambiar Docker, PostgreSQL ni la ACL.

## Documentacion oficial

- [Tailscale Quickstart](https://tailscale.com/kb/1017/install)
- [Auth keys](https://tailscale.com/kb/1085/auth-keys)
- [ACLs](https://tailscale.com/kb/1018/acls)
- [ACL tags](https://tailscale.com/kb/1068/acl-tags)
