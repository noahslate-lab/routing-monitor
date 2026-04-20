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

    # ── RevOps Help Desk ticket creation ────────────────────────────

    async def create_routing_ticket(
        self,
        subject: str,
        description: str,
        priority: str = "HIGH",
        lead_email: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Create a ticket in the RevOps Requests Help Desk pipeline
        for a critical routing alert.
        """
        if not self.api_key:
            return None

        pipeline_id = settings.revops_ticket_pipeline_id
        stage_id = settings.revops_ticket_stage_new
        owner_id = settings.revops_ticket_default_owner_id

        if not pipeline_id or not stage_id:
            print("[HUBSPOT] RevOps ticket pipeline not configured, skipping")
            return None

        properties = {
            "subject": subject[:200],
            "content": description,
            "hs_pipeline": pipeline_id,
            "hs_pipeline_stage": stage_id,
            "hs_ticket_priority": priority,
            "hubspot_owner_id": owner_id,
        }

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    f"{self.base_url}/crm/v3/objects/tickets",
                    headers=self._headers,
                    json={"properties": properties},
                    timeout=15,
                )
                if resp.status_code in (200, 201):
                    ticket = resp.json()
                    ticket_id = ticket.get("id")
                    print(f"[HUBSPOT] Created RevOps ticket #{ticket_id}: \"{subject[:60]}\"")

                    # Create a note on the ticket with the full description
                    if ticket_id:
                        await self._create_ticket_note(
                            ticket_id, subject, description, owner_id
                        )

                    # Associate with contact if we have their email
                    if lead_email and ticket_id:
                        await self._associate_ticket_with_contact(ticket_id, lead_email)

                    return ticket
                else:
                    print(
                        f"[HUBSPOT] RevOps ticket creation failed: "
                        f"{resp.status_code} {resp.text[:200]}"
                    )
                    return None
        except Exception as e:
            print(f"[HUBSPOT] create_routing_ticket error: {e}")
            return None

    async def _create_ticket_note(
        self,
        ticket_id: str,
        subject: str,
        description: str,
        owner_id: Optional[str] = None,
    ) -> None:
        """Create a note on the ticket so the description appears in the conversation timeline."""
        # Convert plain text description to HTML
        html_lines = []
        html_lines.append(f"<h3>{subject}</h3>")
        for line in description.split("\n"):
            if not line.strip():
                html_lines.append("<br/>")
            elif line.startswith("•"):
                html_lines.append(f"<p>{line}</p>")
            elif ": " in line and not line.startswith("http"):
                # Field lines like "Lead Email: foo@bar.com"
                key, _, val = line.partition(": ")
                html_lines.append(f"<p><strong>{key}:</strong> {val}</p>")
            elif line.startswith("http"):
                html_lines.append(f'<p><a href="{line}">{line}</a></p>')
            else:
                html_lines.append(f"<p>{line}</p>")

        html_body = "\n".join(html_lines)

        properties = {
            "hs_note_body": html_body,
            "hs_timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if owner_id:
            properties["hubspot_owner_id"] = owner_id

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    f"{self.base_url}/crm/v3/objects/notes",
                    headers=self._headers,
                    json={
                        "properties": properties,
                        "associations": [
                            {
                                "to": {"id": ticket_id},
                                "types": [
                                    {
                                        "associationCategory": "HUBSPOT_DEFINED",
                                        "associationTypeId": 18,
                                    }
                                ],
                            }
                        ],
                    },
                    timeout=15,
                )
                if resp.status_code in (200, 201):
                    note_id = resp.json().get("id")
                    print(f"[HUBSPOT] Note {note_id} created on ticket #{ticket_id}")
                else:
                    print(
                        f"[HUBSPOT] Note creation failed: "
                        f"{resp.status_code} {resp.text[:200]}"
                    )
        except Exception as e:
            print(f"[HUBSPOT] _create_ticket_note error: {e}")

    async def _associate_ticket_with_contact(
        self, ticket_id: str, email: str
    ) -> None:
        """Look up a contact by email and associate with the ticket."""
        try:
            contact = await self.get_contact_by_email(email)
            if not contact:
                return

            contact_id = contact.get("id")
            if not contact_id:
                return

            async with httpx.AsyncClient() as client:
                resp = await client.put(
                    f"{self.base_url}/crm/v4/objects/tickets/{ticket_id}"
                    f"/associations/contacts/{contact_id}",
                    headers=self._headers,
                    json=[
                        {
                            "associationCategory": "HUBSPOT_DEFINED",
                            "associationTypeId": 16,
                        }
                    ],
                    timeout=15,
                )
                if resp.status_code in (200, 201):
                    print(
                        f"[HUBSPOT] Associated ticket #{ticket_id} "
                        f"with contact {contact_id} ({email})"
                    )
                else:
                    print(
                        f"[HUBSPOT] Ticket-contact association failed: "
                        f"{resp.status_code}"
                    )
        except Exception as e:
            print(f"[HUBSPOT] _associate_ticket_with_contact error: {e}")

    # ── Helpers ───────────────────────────────────────────────────────

    def contact_link(self, contact_id: str) -> str:
        return f"https://app.hubspot.com/contacts/{self.portal_id}/contact/{contact_id}"

    def company_link(self, company_id: str) -> str:
        return f"https://app.hubspot.com/contacts/{self.portal_id}/company/{company_id}"

    def ticket_link(self, ticket_id: str) -> str:
        return f"https://app.hubspot.com/contacts/{self.portal_id}/ticket/{ticket_id}"


hubspot_service = HubSpotService()
