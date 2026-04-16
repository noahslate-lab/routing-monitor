"""
HubSpot API Service for Routing Monitor
========================================

Focused on post-routing verification:
- Contact/company lookups by email
- Property change history (ownership flip-flops, field overwrites)
- Meeting engagement verification
- Owner lookups and team membership
"""
import httpx
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from app.config import settings


# Contact properties we care about for routing verification
CONTACT_PROPERTIES = [
    "email",
    "firstname",
    "lastname",
    "company",
    "jobtitle",
    "hubspot_owner_id",
    "hs_lead_status",
    "lifecyclestage",
    "numberofemployees",
    "associatedcompanyid",
    "hs_analytics_source",
    "createdate",
    "lastmodifieddate",
    "sdr_owner",
    "employee_size__segments_",
]

# Properties to track change history on
AUDIT_PROPERTIES = [
    "hubspot_owner_id",
    "hs_lead_status",
    "lifecyclestage",
    "sdr_owner",
]


class HubSpotService:
    def __init__(self):
        self.api_key = settings.hubspot_api_key
        self.base_url = settings.hubspot_base_url
        self.portal_id = settings.hubspot_portal_id
        self._headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    async def test_connection(self) -> bool:
        if not self.api_key:
            return False
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{self.base_url}/crm/v3/objects/contacts",
                    headers=self._headers,
                    params={"limit": 1},
                    timeout=10,
                )
                return resp.status_code == 200
        except Exception as e:
            print(f"[HUBSPOT] Connection test failed: {e}")
            return False

    # ── Contact lookup ────────────────────────────────────────────────

    async def get_contact_by_email(
        self, email: str
    ) -> Optional[Dict[str, Any]]:
        """Look up a contact by email, returning properties we care about."""
        if not self.api_key or not email:
            return None
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    f"{self.base_url}/crm/v3/objects/contacts/search",
                    headers=self._headers,
                    json={
                        "filterGroups": [
                            {
                                "filters": [
                                    {
                                        "propertyName": "email",
                                        "operator": "EQ",
                                        "value": email,
                                    }
                                ]
                            }
                        ],
                        "properties": CONTACT_PROPERTIES,
                        "limit": 1,
                    },
                    timeout=15,
                )
                if resp.status_code == 200:
                    results = resp.json().get("results", [])
                    return results[0] if results else None
                print(f"[HUBSPOT] contact search failed: {resp.status_code}")
                return None
        except Exception as e:
            print(f"[HUBSPOT] get_contact_by_email error: {e}")
            return None

    # ── Property change history ───────────────────────────────────────

    async def get_property_history(
        self,
        contact_id: str,
        properties: Optional[List[str]] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Fetch property change history for a contact.
        Returns: { "hubspot_owner_id": [ {value, timestamp, source, sourceId}, ... ], ... }
        """
        if not self.api_key or not contact_id:
            return {}

        props = properties or AUDIT_PROPERTIES
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{self.base_url}/crm/v3/objects/contacts/{contact_id}",
                    headers=self._headers,
                    params={
                        "properties": ",".join(props),
                        "propertiesWithHistory": ",".join(props),
                    },
                    timeout=15,
                )
                if resp.status_code != 200:
                    print(f"[HUBSPOT] property history failed: {resp.status_code}")
                    return {}

                data = resp.json()
                history = data.get("propertiesWithHistory", {})
                return history
        except Exception as e:
            print(f"[HUBSPOT] get_property_history error: {e}")
            return {}

    # ── Owner lookups ─────────────────────────────────────────────────

    async def get_owner(self, owner_id: str) -> Optional[Dict[str, Any]]:
        """Get owner details by ID (email, name, team)."""
        if not self.api_key or not owner_id:
            return None
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{self.base_url}/crm/v3/owners/{owner_id}",
                    headers=self._headers,
                    timeout=10,
                )
                if resp.status_code == 200:
                    return resp.json()
                return None
        except Exception as e:
            print(f"[HUBSPOT] get_owner error: {e}")
            return None

    async def list_owners(self) -> List[Dict[str, Any]]:
        """List all owners (for building a lookup cache)."""
        if not self.api_key:
            return []
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{self.base_url}/crm/v3/owners",
                    headers=self._headers,
                    params={"limit": 500},
                    timeout=15,
                )
                if resp.status_code == 200:
                    return resp.json().get("results", [])
                return []
        except Exception as e:
            print(f"[HUBSPOT] list_owners error: {e}")
            return []

    # ── Meeting engagements ───────────────────────────────────────────

    async def get_contact_meetings(
        self,
        contact_id: str,
        after_timestamp: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get meeting engagements associated with a contact.
        Optionally filter to meetings created after a given timestamp.
        """
        if not self.api_key or not contact_id:
            return []
        try:
            async with httpx.AsyncClient() as client:
                # Get associated meetings
                resp = await client.get(
                    f"{self.base_url}/crm/v3/objects/contacts/{contact_id}/associations/meetings",
                    headers=self._headers,
                    timeout=15,
                )
                if resp.status_code != 200:
                    return []

                meeting_ids = [
                    r["id"] for r in resp.json().get("results", [])
                ]
                if not meeting_ids:
                    return []

                # Batch-read meeting details
                resp = await client.post(
                    f"{self.base_url}/crm/v3/objects/meetings/batch/read",
                    headers=self._headers,
                    json={
                        "inputs": [{"id": mid} for mid in meeting_ids[:100]],
                        "properties": [
                            "hs_meeting_title",
                            "hs_meeting_start_time",
                            "hs_meeting_end_time",
                            "hs_meeting_outcome",
                            "hubspot_owner_id",
                            "hs_timestamp",
                            "hs_createdate",
                            "hs_meeting_external_url",
                        ],
                    },
                    timeout=15,
                )
                if resp.status_code != 200:
                    return []

                meetings = resp.json().get("results", [])

                # Filter by timestamp if given
                if after_timestamp:
                    meetings = [
                        m
                        for m in meetings
                        if (
                            m.get("properties", {}).get("hs_createdate", "")
                            >= after_timestamp
                        )
                    ]
                return meetings
        except Exception as e:
            print(f"[HUBSPOT] get_contact_meetings error: {e}")
            return []

    # ── Company lookup ────────────────────────────────────────────────

    async def get_company(self, company_id: str) -> Optional[Dict[str, Any]]:
        """Get company details."""
        if not self.api_key or not company_id:
            return None
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{self.base_url}/crm/v3/objects/companies/{company_id}",
                    headers=self._headers,
                    params={
                        "properties": "name,domain,industry,numberofemployees,hubspot_owner_id"
                    },
                    timeout=15,
                )
                if resp.status_code == 200:
                    return resp.json()
                return None
        except Exception as e:
            print(f"[HUBSPOT] get_company error: {e}")
            return None

    # ── Helpers ───────────────────────────────────────────────────────

    def contact_link(self, contact_id: str) -> str:
        return f"https://app.hubspot.com/contacts/{self.portal_id}/contact/{contact_id}"

    def company_link(self, company_id: str) -> str:
        return f"https://app.hubspot.com/contacts/{self.portal_id}/company/{company_id}"


hubspot_service = HubSpotService()
