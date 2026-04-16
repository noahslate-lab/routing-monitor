"""
Routing Monitor Routes
======================

Endpoints:
- GET  /status       — current monitor status + last poll results
- POST /poll         — manually trigger a poll cycle
- GET  /routers      — list all Chili Piper routers
- GET  /logs         — fetch raw concierge logs for a router

Background task polls every ROUTING_MONITOR_INTERVAL_MINUTES (default 10).
"""
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Query

from app.config import settings
from app.services.chilipiper_service import chilipiper_service
from app.services.routing_monitor_service import poll_and_analyze

router = APIRouter(tags=["routing-monitor"])

# ── State ─────────────────────────────────────────────────────────────

_monitor_task: Optional[asyncio.Task] = None
_last_poll_result: dict = {}
_poll_count: int = 0
_is_running: bool = False


# ── Background polling loop ──────────────────────────────────────────

async def _polling_loop():
    """Runs forever, polling Chili Piper on the configured interval."""
    global _last_poll_result, _poll_count, _is_running
    interval = settings.routing_monitor_interval_minutes
    _is_running = True
    print(f"[ROUTING_MONITOR] Background polling started (every {interval} min)")

    while True:
        try:
            _last_poll_result = await poll_and_analyze(
                lookback_minutes=interval + 5  # overlap to avoid gaps
            )
            _poll_count += 1
            events = _last_poll_result.get("new_events", 0)
            alerts = _last_poll_result.get("alerts_posted", 0)
            print(
                f"[ROUTING_MONITOR] Poll #{_poll_count}: "
                f"{events} new events, {alerts} alerts posted"
            )
        except Exception as e:
            print(f"[ROUTING_MONITOR] Poll error: {e}")
            _last_poll_result = {"error": str(e)}

        await asyncio.sleep(interval * 60)


def start_monitor():
    """Start the background polling task (call from app lifespan)."""
    global _monitor_task
    if not settings.chili_piper_api_key:
        print("[ROUTING_MONITOR] No CHILI_PIPER_API_KEY — monitor disabled")
        return
    if not settings.slack_routing_monitor_channel:
        print("[ROUTING_MONITOR] No SLACK_ROUTING_MONITOR_CHANNEL — monitor disabled")
        return
    if _monitor_task is None or _monitor_task.done():
        _monitor_task = asyncio.create_task(_polling_loop())


def stop_monitor():
    """Stop the background polling task (call from app lifespan shutdown)."""
    global _monitor_task, _is_running
    if _monitor_task and not _monitor_task.done():
        _monitor_task.cancel()
        _is_running = False
        print("[ROUTING_MONITOR] Background polling stopped")


# ── Endpoints ─────────────────────────────────────────────────────────

@router.get("/status")
async def monitor_status():
    """Current monitor status and last poll results."""
    cp_ok = await chilipiper_service.test_connection()
    return {
        "monitor_running": _is_running,
        "chili_piper_connected": cp_ok,
        "poll_count": _poll_count,
        "interval_minutes": settings.routing_monitor_interval_minutes,
        "slack_channel": settings.slack_routing_monitor_channel,
        "last_poll": _last_poll_result,
    }


@router.post("/poll")
async def manual_poll(
    lookback_minutes: int = Query(
        default=15, ge=1, le=1440,
        description="How many minutes back to check"
    ),
):
    """Manually trigger a routing monitor poll cycle."""
    global _last_poll_result, _poll_count
    result = await poll_and_analyze(lookback_minutes=lookback_minutes)
    _last_poll_result = result
    _poll_count += 1
    return result


@router.get("/routers")
async def list_routers():
    """List all Chili Piper Concierge routers."""
    routers = await chilipiper_service.list_routers()
    return {"count": len(routers), "routers": routers}


@router.get("/logs")
async def get_logs(
    workspace_id: str = Query(..., description="Workspace ID"),
    router_id: str = Query(..., description="Router ID (UUID)"),
    lookback_minutes: int = Query(
        default=60, ge=1, le=43200,
        description="How many minutes back to fetch"
    ),
):
    """Fetch raw concierge routing logs for a specific router."""
    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=lookback_minutes)
    logs = await chilipiper_service.get_concierge_logs(
        workspace_id=workspace_id,
        router_id=router_id,
        start=start,
        end=now,
    )
    return {
        "workspace_id": workspace_id,
        "router_id": router_id,
        "period_start": start.isoformat(),
        "period_end": now.isoformat(),
        "count": len(logs),
        "logs": logs,
    }
