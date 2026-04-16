# Routing Monitor

Standalone service that polls Chili Piper concierge routing logs and posts alerts to a dedicated Slack channel when it detects issues — so RevOps knows before reps do.

## What it catches

| Severity | Alert | Example |
|----------|-------|---------|
| 🚨 Critical | **Action Failures** | HubSpot upsert/ownership/engagement chain broke — meeting won't be tracked |
| ⚠️ Warning | **Segment Mismatch** | Form says "Mid-Market" but company is 26 FTE |
| ⚠️ Warning | **Missing Employee Count** | `numberofemployees` is null, routing flew blind on segment label |
| ℹ️ Info | **No CRM Owner** | All ownership rules skipped, fell to segment catch-all |
| ℹ️ Info | **Fall-Through** | Lead fell through all named rules to backup routing |

## Setup

### 1. Create a Slack App

1. Go to [api.slack.com/apps](https://api.slack.com/apps) → **Create New App**
2. Name it **Routing Monitor** (or whatever you like)
3. Add Bot Token Scopes: `chat:write`, `channels:read`
4. Install to workspace → copy the **Bot User OAuth Token** (`xoxb-...`)
5. Create a `#routing-monitor` channel and invite the bot

### 2. Configure

Copy `.env.example` to `.env` and fill in:

```bash
CHILI_PIPER_API_KEY=api_...           # Already set
SLACK_BOT_TOKEN=xoxb-your-token       # From step 1
SLACK_ROUTING_MONITOR_CHANNEL=routing-monitor
ROUTING_MONITOR_INTERVAL_MINUTES=10   # Poll every 10 min
```

### 3. Run locally

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8100
```

### 4. Deploy to Render

Push to GitHub, connect to Render, and use the included `render.yaml` blueprint.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Service info |
| GET | `/health` | Health check (Chili Piper + Slack status) |
| GET | `/status` | Monitor status + last poll results |
| POST | `/poll?lookback_minutes=15` | Manually trigger a poll cycle |
| GET | `/routers` | List all Chili Piper Concierge routers |
| GET | `/logs?workspace_id=...&router_id=...` | Fetch raw concierge logs |
| GET | `/docs` | Swagger UI |

## Architecture

```
┌──────────────┐     poll every 10m     ┌──────────────────┐
│  Chili Piper │ ◄───────────────────── │  Routing Monitor │
│  Edge API    │  concierge logs        │  (FastAPI)       │
└──────────────┘                        └────────┬─────────┘
                                                 │ analyze
                                                 ▼
                                        ┌──────────────────┐
                                        │  #routing-monitor │
                                        │  (Slack channel)  │
                                        └──────────────────┘
```
