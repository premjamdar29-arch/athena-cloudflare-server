# ATHENA Cloud Market Data Service

Cloudflare Worker + SQLite Durable Objects for the ATHENA market feed and up to 365 IST-session history. It does not use R2 or require an R2 bucket. After a user logs in to Angel One in ATHENA, the app can send the short-lived SmartAPI session to `POST /v1/collector/start`. The Durable Object then opens its own Angel One Snap Quote WebSocket, stores normalized one-minute bars, broadcasts live bars to connected apps, and reconnects after interruptions. The Android APK and Windows app can close after the collector reports `CONNECTED`.

## Authentication boundary

- PIN and TOTP are used only by the app to log in to Angel One. **Never send them to Cloudflare.**
- The app sends the SmartAPI API key, client code, JWT, feed token, and instrument subscription list over HTTPS after successful login. The server encrypts that session material with AES-GCM before storing it in Durable Object SQLite.
- Cloudflare uses the feed session only for market data. This service exposes **no order-placement endpoint**; live orders remain direct from the apps to Angel One.
- Feed tokens expire. When Angel One rejects/ends a session, the collector reports `AUTH_REQUIRED`; log in again in ATHENA and call `/v1/collector/start` with the new session.
- A WebSocket Snap Quote contains quote/depth snapshots, not historical aggressor executions. `LIVE_SIDE_ESTIMATE` bars use quote matching plus the tick rule and must be labeled as inferred. OHLCV history remains separately labeled as `BROKER_OHLCV` or `OHLCV_ESTIMATE`.

## Upload this folder to GitHub and connect Cloudflare

This folder is a standalone Worker project. Create an empty GitHub repository (preferably private), then open PowerShell in this folder and run the following, replacing the URL with your GitHub repository URL:

```powershell
git init -b main
git add .
git commit -m "Add ATHENA Cloudflare market data service"
git remote add origin https://github.com/YOUR-ACCOUNT/YOUR-REPOSITORY.git
git push -u origin main
```

Keep `wrangler.jsonc` at the repository root. Do not upload `.dev.vars`, `.env`, `node_modules`, or `.wrangler` folders; `.gitignore` excludes them.

In Cloudflare:

1. Open **Workers & Pages → Create application → GitHub**, authorize GitHub, and select the repository.
2. Set the root directory to `.` (or leave it blank), build command blank, and deploy command to `npm run deploy`. Your build log says Cloudflare created the Worker as `worker-repo`, so `wrangler.jsonc` uses that same name.
3. Add runtime secrets `ATHENA_API_TOKEN` and `ATHENA_SESSION_ENCRYPTION_KEY` in the Worker’s **Settings → Variables & Secrets**. Do not put secret values in GitHub or this folder.
4. Save and deploy. No R2 bucket is required. The Durable Object migration is declared in `wrangler.jsonc` and is applied by Wrangler during deployment.

Cloudflare’s Git integration can build and deploy a Worker on pushes to the selected branch. See [Cloudflare Workers Builds](https://developers.cloudflare.com/workers/ci-cd/builds/) and [build configuration](https://developers.cloudflare.com/workers/ci-cd/builds/configuration/).

## Deploy from a local terminal instead

Requirements: Node.js 20+ and a Cloudflare account with Workers and SQLite Durable Objects enabled.

From this directory:

```powershell
npm install
npx wrangler login
npx wrangler secret put ATHENA_API_TOKEN
npx wrangler secret put ATHENA_SESSION_ENCRYPTION_KEY
npm run deploy
```

For `ATHENA_API_TOKEN`, enter a long random bearer secret and keep it in a password manager. For `ATHENA_SESSION_ENCRYPTION_KEY`, enter a random 32-byte key encoded as Base64. Generate one in PowerShell:

```powershell
$bytes = New-Object byte[] 32
$rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
$rng.GetBytes($bytes)
[Convert]::ToBase64String($bytes)
$rng.Dispose()
```

Set `ALLOWED_ORIGINS` in `wrangler.jsonc` to the exact Windows/Web origins only if browser clients call the Worker directly. Do not place `ATHENA_API_TOKEN` or Angel One session values in bundled app code; clients need protected local configuration (Android encrypted storage and Windows user-protected storage).

Wrangler prints the deployed `https://worker-repo.<your-subdomain>.workers.dev` URL. The first deployment applies the SQLite Durable Object migration. Cron invokes the collector watchdog every five minutes and compresses, archives, and prunes history after the IST session.

## API

Except `GET /health`, requests require `Authorization: Bearer <ATHENA_API_TOKEN>`.

- `GET /health` — public service check.
- `GET /v1/time` — server UTC time for client offset measurement; this is not a broker clock synchronization guarantee.
- `GET /v1/collector/status` — feed state, freshness, and event-age diagnostics.
- `POST /v1/collector/start` — after Angel One login, submit `{apiKey, clientCode, jwtToken, feedToken, instruments:[{exchangeType,token,priceStep}]}`. Returns whether the app may close.
- `PUT /v1/collector/subscriptions` — replace the active feed's instrument list.
- `POST /v1/collector/stop` — disconnect and erase the encrypted session from cloud storage.
- `POST /v1/market/bars` — import 1–500 historical or locally recorded bars. Accepted sources are `BROKER_OHLCV`, `OHLCV_ESTIMATE`, and `LIVE_SIDE_ESTIMATE`.
- `GET /v1/market/history?exchangeType=2&token=123&from=...&to=...` — return archived and active bars; `from` and `to` are UTC epoch seconds.
- `POST /v1/stream-ticket`, then connect to `/v1/stream?ticket=...` using the one-use 30-second ticket. Apps receive `MARKET_BARS` messages.

`POST /v1/collector/start` example:

```json
{
  "apiKey": "<SmartAPI key>",
  "clientCode": "<Angel One client code>",
  "jwtToken": "<short-lived JWT from successful app login>",
  "feedToken": "<feed token from successful app login>",
  "instruments": [
    { "exchangeType": 2, "token": "12345", "priceStep": 0.05 }
  ]
}
```

The API key and session tokens are sensitive. Submit them only to the HTTPS endpoint; never put them in a URL or logs. The server has one collector session for this ATHENA account and allows up to 180 instruments to keep free-tier daily storage writes within a practical limit.

## Retention and costs

The daily job stores gzip-compressed contract-day archives across ten SQLite Durable Object shards and prunes dates older than the newest **365 distinct IST sessions**. This avoids R2 subscription and card setup. The Workers Free plan caps Durable Object storage at 5 GB per account and 1 GB per object; all ATHENA data shares those limits. If the retained footprint exceeds the cap, writes will fail and collection will be incomplete. The collector accepts at most 180 subscribed instruments because free-tier daily row-write quotas are also limited. Measure actual compressed session data before relying on 365 days. See [Durable Object pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/), [Durable Object limits](https://developers.cloudflare.com/durable-objects/platform/limits/), and [Workers limits](https://developers.cloudflare.com/workers/platform/limits/).

## Status

This repository contains the Worker service only. It has not been deployed to the user's Cloudflare account. The existing APK/Windows app still needs a secure login handoff to call `/v1/collector/start` and read bars/history. Until that app integration is added, Cloudflare will not receive a login session automatically. The collector persists its encrypted session and continues independently after a successful handoff, until Angel One expires/rejects the daily session or the collector is stopped. It does not bypass Angel One PIN/TOTP authentication.
