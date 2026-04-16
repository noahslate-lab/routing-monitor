"""
Chili Piper API Client
======================

Wraps the Chili Piper Edge Org API (fire-edge).
Primary use: pull concierge routing logs and meeting data
for the routing monitor.

API Docs: https://fire.chilipiper.com/api/fire-edge/public/org/docs/swagger/
"""
import httpx
from datetime import datetime
from typing import Any, Dict, List, Optional
from app.config import settings


class ChiliPiperService:
    def __init__(self):
        self.base_url = settings.chili_piper_base_url.rstrip("/")
        self.api_key = settings.chili_piper_api_key
        self._client: Optional[httpx.AsyncClient] = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                timeout=30.0,
            )
        return self._client

    # ── Health ────────────────────────────────────────────────────────

    async def test_connection(self) -> bool:
        """Ping the API to verify the key is valid."""
        if not self.api_key:
            return False
        try:
            resp = await self.client.get("/v1/org/health/ping")
            return resp.status_code == 200
        except Exception as e:
            print(f"[CHILIPIPER] Connection test failed: {e}")
            return False

    # ── Tenant ────────────────────────────────────────────────────────

    async def get_tenant(self) -> Optional[Dict[str, Any]]:
        """Fetch org-level config and metadata."""
        try:
            resp = await self.client.get("/v1/org/tenant/get")
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"[CHILIPIPER] get_tenant failed: {e}")
            return None

    # ── Concierge Routers ─────────────────────────────────────────────

    async def list_routers(self) -> List[Dict[str, Any]]:
        """List all Concierge routers in the org.
        
        The API returns: { "routers": [ { "router": {...}, "workspaceId": "..." }, ... ] }
        We normalize this to a flat list with workspaceId on each router dict.
        """
        try:
            resp = await self.client.post(
                "/v1/org/concierge/routers/concierge/list"
            )
            resp.raise_for_status()
            data = resp.json()

            # Handle nested structure
            raw_list = []
            if isinstance(data, dict):
                raw_list = data.get("routers", data.get("results", []))
            elif isinstance(data, list):
                raw_list = data

            # Flatten: pull inner "router" dict and attach workspaceId
            normalized = []
            for item in raw_list:
                if isinstance(item, dict) and "router" in item:
                    router = item["router"]
                    router["workspaceId"] = item.get("workspaceId", "")
                    normalized.append(router)
                else:
                    normalized.append(item)
            return normalized
        except Exception as e:
            print(f"[CHILIPIPER] list_routers failed: {e}")
            return []

    # ── Concierge Logs (the gold mine) ────────────────────────────────

    async def get_concierge_logs(
        self,
        workspace_id: str,
        router_id: str,
        start: datetime,
        end: datetime,
    ) -> List[Dict[str, Any]]:
        """
        Fetch routing decision logs for a router in a time window.
        Max window: 30 days.

        Returns list of log entries with:
        - timestamp
        - inputLeadData
        - rulesEvaluated
        - finalDecision (assignedUser/team)
        """
        try:
            resp = await self.client.post(
                "/v1/org/concierge/logs",
                params={
                    "workspaceId": workspace_id,
                    "routerId": router_id,
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                },
            )
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            return data.get("results", data.get("logs", []))
        except Exception as e:
            print(f"[CHILIPIPER] get_concierge_logs failed: {e}")
            return []

    # ── Meetings ──────────────────────────────────────────────────────

    async def list_meetings(
        self,
        start: datetime,
        end: datetime,
        workspace_ids: Optional[List[str]] = None,
        page: int = 0,
        page_size: int = 50,
    ) -> Dict[str, Any]:
        """List meetings in a time range. Paginated."""
        params: Dict[str, Any] = {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "page": page,
            "pageSize": page_size,
        }
        if workspace_ids:
            params["workspaceIds"] = workspace_ids
        try:
            resp = await self.client.get(
                "/v1/org/meetings/meetings", params=params
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"[CHILIPIPER] list_meetings failed: {e}")
            return {}

    async def get_meeting(self, meeting_id: str) -> Optional[Dict[str, Any]]:
        """Fetch full record for a single meeting."""
        try:
            resp = await self.client.get(
                f"/v1/org/meetings/get/{meeting_id}"
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"[CHILIPIPER] get_meeting failed: {e}")
            return None

    # ── Users ─────────────────────────────────────────────────────────

    async def find_user(self, query: str) -> List[Dict[str, Any]]:
        """Search users by email or name."""
        try:
            resp = await self.client.get(
                "/v1/org/user/find", params={"query": query}
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("results", [])
        except Exception as e:
            print(f"[CHILIPIPER] find_user failed: {e}")
            return []

    # ── Distributions ─────────────────────────────────────────────────

    async def list_distributions(
        self, page: int = 0, page_size: int = 50
    ) -> List[Dict[str, Any]]:
        """List all published distributions."""
        try:
            resp = await self.client.get(
                "/v1/org/distribution",
                params={"page": page, "pageSize": page_size},
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("results", [])
        except Exception as e:
            print(f"[CHILIPIPER] list_distributions failed: {e}")
            return []

    # ── Rules ─────────────────────────────────────────────────────────

    async def list_rules(self) -> List[Dict[str, Any]]:
        """List all routing rules."""
        try:
            resp = await self.client.get("/v1/org/rule/list")
            resp.raise_for_status()
            data = resp.json()
            return data.get("results", [])
        except Exception as e:
            print(f"[CHILIPIPER] list_rules failed: {e}")
            return []

    # ── Workspaces ────────────────────────────────────────────────────

    async def list_workspaces(
        self, page: int = 0, page_size: int = 50
    ) -> List[Dict[str, Any]]:
        """List all workspaces in the org."""
        try:
            resp = await self.client.get(
                "/v1/org/workspace",
                params={"page": page, "pageSize": page_size},
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("results", [])
        except Exception as e:
            print(f"[CHILIPIPER] list_workspaces failed: {e}")
            return []


chilipiper_service = ChiliPiperService()
