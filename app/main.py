"""
Routing Monitor - Main FastAPI Application
===========================================

Standalone service that polls Chili Piper concierge routing logs
and posts alerts to Slack when it detects misroutes, action failures,
or data quality issues. Runs on Render as a separate service from
the Deal Desk Bot.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.config import settings
from app.routes.monitor import router as monitor_router, start_monitor, stop_monitor
from app.services.chilipiper_service import chilipiper_service
from app.services.hubspot_service import hubspot_service


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("🔍 Routing Monitor starting...")
    print(f"   Environment: {settings.environment}")
    print(f"   Poll interval: {settings.routing_monitor_interval_minutes} min")
    print(f"   Slack channel: #{settings.slack_routing_monitor_channel}")

    chilipiper_ok = await chilipiper_service.test_connection()
    print(f"   Chili Piper: {'✅ Connected' if chilipiper_ok else '❌ Not connected'}")

    hubspot_ok = await hubspot_service.test_connection()
    print(f"   HubSpot: {'✅ Connected' if hubspot_ok else '⚠️ Not connected (CP-only mode)'}")

    slack_configured = bool(settings.slack_bot_token)
    print(f"   Slack: {'✅ Configured' if slack_configured else '⚠️ No token — alerts will log only'}")

    # Start background polling
    start_monitor()

    yield

    # Shutdown
    stop_monitor()
    print("👋 Routing Monitor shutting down...")


app = FastAPI(
    title="Routing Monitor",
    description="Chili Piper routing monitor — detects misroutes and action failures, alerts via Slack",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routes
app.include_router(monitor_router)


@app.get("/")
async def root():
    return {
        "service": "Routing Monitor",
        "version": "1.0.0",
        "description": "Chili Piper routing monitor with Slack alerts",
        "docs": "/docs",
    }


@app.get("/health")
async def health_check():
    chilipiper_ok = await chilipiper_service.test_connection()
    hubspot_ok = await hubspot_service.test_connection()
    slack_configured = bool(settings.slack_bot_token)

    return {
        "status": "healthy" if chilipiper_ok else "degraded",
        "version": "1.1.0",
        "integrations": {
            "chili_piper": "connected" if chilipiper_ok else "disconnected",
            "hubspot": "connected" if hubspot_ok else "not configured",
            "slack": "configured" if slack_configured else "not configured",
        },
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=settings.port)
