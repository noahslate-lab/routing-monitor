"""
Routing Monitor Service
=======================

Polls Chili Piper concierge logs on a schedule and analyzes each
routing event for red flags:

1. Segment mismatch — form/enrichment segment doesn't match actual
   company size (e.g. "Mid-Market" but 26 FTE)
2. Action failures — post-routing HubSpot actions (upsert, ownership,
   engagement, notify) that failed in a cascade
3. Missing data — null employee count, no CRM owner, missing fields
4. Fall-through routing — lead fell through all named rules to a
   catch-all / backup segment

Posts structured alerts to a dedicated Slack channel so RevOps
knows before the rep does.
"""
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set
from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.errors import SlackApiError

from app.config import settings
from app.services.chilipiper_service import chilipiper_service
from app.services.hubspot_service import hubspot_service

_slack_client: Optional[AsyncWebClient] = None

# Cache: CP userId → resolved display name (populated from CP user list)
_cp_user_name_cache: Dict[str, str] = {}
# Cache: CP userId → email (for meeting owner verification by email)
_cp_user_email_cache: Dict[str, str] = {}
_cp_user_cache_loaded: bool = False


def _get_slack_client() -> Optional[AsyncWebClient]:
    global _slack_client
    if _slack_client is None and settings.slack_bot_token:
        _slack_client = AsyncWebClient(token=settings.slack_bot_token)
    return _slack_client


async def _ensure_cp_user_cache():
    """Load all CP users into the name and HS owner caches on first use."""
    global _cp_user_name_cache, _cp_user_email_cache, _cp_user_cache_loaded
    if _cp_user_cache_loaded:
        return

    users = await chilipiper_service.list_all_users()
    for user in users:
        user_id = user.get("id", "")
        name = user.get("name", "")
        email = user.get("email", "")
        if user_id and name:
            _cp_user_name_cache[user_id] = name
        if user_id and email:
            _cp_user_email_cache[user_id] = email.lower()
    _cp_user_cache_loaded = True
    print(
        f"[ROUTING_MONITOR] CP user cache loaded: "
        f"{len(_cp_user_name_cache)} names, "
        f"{len(_cp_user_email_cache)} emails"
    )


# ── Alert types ───────────────────────────────────────────────────────

class AlertSeverity:
    CRITICAL = "critical"   # action failure chain, meeting won't be tracked
    WARNING = "warning"     # segment mismatch, possible misroute
    INFO = "info"           # fall-through to catch-all, missing enrichment


class RoutingAlert:
    """A single flagged issue from a routing event."""

    def __init__(
        self,
        severity: str,
        title: str,
        details: str,
        lead_name: Optional[str] = None,
        lead_email: Optional[str] = None,
        lead_company: Optional[str] = None,
        assigned_rep: Optional[str] = None,
        router_name: Optional[str] = None,
        matched_rule: Optional[str] = None,
        timestamp: Optional[str] = None,
        raw_log: Optional[Dict[str, Any]] = None,
    ):
        self.severity = severity
        self.title = title
        self.details = details
        self.lead_name = lead_name
        self.lead_email = lead_email
        self.lead_company = lead_company
        self.assigned_rep = assigned_rep
        self.router_name = router_name
        self.matched_rule = matched_rule
        self.timestamp = timestamp
        self.raw_log = raw_log


# ── Helpers to extract from CP log structure ─────────────────────────

def _extract_form_values(log_entry: Dict[str, Any]) -> Dict[str, str]:
    """
    CP form data is: { fieldId: { "name": "...", "value": "...", "type": "..." }, ... }
    Flatten to: { name: value }
    """
    form_raw = log_entry.get("form", {})
    flat: Dict[str, str] = {}
    for _field_id, field_obj in form_raw.items():
        if isinstance(field_obj, dict):
            name = field_obj.get("name", "")
            value = field_obj.get("value", "")
            if name:
                flat[name] = str(value) if value is not None else ""
    return flat


# ── Segment classification ────────────────────────────────────────────
#
# Segment criteria (SDR count = number of SDRs at the company):
#   Enterprise:  ≥1,000 employees AND ≥16 SDRs
#   Mid-Market:  80–999 employees OR (≥1,000 employees with <16 SDRs)
#   Commercial:  1–79 employees
#
# Clay enrichment handles the SDR nuance for inbound forms, but form
# submissions may not always have SDR data — so we flag mismatches on
# employee count and note when SDR data is missing.


def _classify_segment(emp_count: int, sdr_count: Optional[int] = None) -> str:
    """Determine the correct segment based on employee + SDR count."""
    if emp_count < 1:
        return "Unknown"
    if emp_count <= 79:
        return "Commercial"
    if emp_count <= 999:
        return "Mid-Market"
    # ≥1,000 employees — need SDR count to distinguish
    if sdr_count is not None and sdr_count >= 16:
        return "Enterprise"
    if sdr_count is not None:
        return "Mid-Market"  # ≥1,000 but <16 SDRs
    # No SDR data — could be either
    return "Enterprise or Mid-Market"


# Map form segment labels to our canonical names for comparison
SEGMENT_ALIASES = {
    "commercial": "Commercial",
    "smb": "Commercial",
    "mid-market": "Mid-Market",
    "mid-market - low employee count": "Mid-Market",
    "mid-market - high employee count": "Mid-Market",
    "midmarket": "Mid-Market",
    "enterprise": "Enterprise",
}


# ── Analysis checks (sync — CP data only) ────────────────────────────

def _check_segment_mismatch(
    form_values: Dict[str, str],
) -> Optional[RoutingAlert]:
    """
    Flag when the routed segment doesn't match the actual company data.
    Uses employee count + SDR count (when available) to determine
    what the correct segment should be.
    """
    segment_raw = form_values.get("employee_size__segments_", "")
    emp_count_raw = (
        form_values.get("numberofemployees")
        or form_values.get("number_of_employees")
    )

    if not segment_raw or not emp_count_raw:
        return None

    try:
        emp_count = int(emp_count_raw)
    except (ValueError, TypeError):
        return None

    # Try to get SDR count from form data
    sdr_count = None
    sdr_raw = (
        form_values.get("sdr_count")
        or form_values.get("num_sdrs")
        or form_values.get("number_of_sdrs")
        or form_values.get("sdrcount")
    )
    if sdr_raw:
        try:
            sdr_count = int(sdr_raw)
        except (ValueError, TypeError):
            pass

    # Normalize the form segment to our canonical names
    form_segment = SEGMENT_ALIASES.get(segment_raw.lower().strip())
    if not form_segment:
        # Unknown segment label — can't compare
        return None

    correct_segment = _classify_segment(emp_count, sdr_count)

    # If we can't determine definitively (no SDR data for 1000+), be lenient
    if correct_segment == "Enterprise or Mid-Market":
        if form_segment in ("Enterprise", "Mid-Market"):
            return None  # Either could be right without SDR data
        # But if they said Commercial, that's definitely wrong
        if form_segment == "Commercial":
            return RoutingAlert(
                severity=AlertSeverity.WARNING,
                title="Segment Mismatch",
                details=(
                    f"Routed as *{segment_raw}* but company has "
                    f"*{emp_count} employees* — should be Enterprise or "
                    f"Mid-Market (SDR count not available to determine which).\n"
                    f"Check Clay enrichment or form dropdown."
                ),
            )
        return None

    if form_segment != correct_segment:
        sdr_note = ""
        if sdr_count is not None:
            sdr_note = f" with *{sdr_count} SDRs*"
        elif emp_count >= 1000:
            sdr_note = " (SDR count not available — check Clay enrichment)"

        return RoutingAlert(
            severity=AlertSeverity.WARNING,
            title="Segment Mismatch",
            details=(
                f"Routed as *{segment_raw}* but should be "
                f"*{correct_segment}* based on *{emp_count} employees*"
                f"{sdr_note}.\n"
                f"This lead was sent to the wrong team."
            ),
        )
    return None


def _check_action_failures(log_entry: Dict[str, Any]) -> Optional[RoutingAlert]:
    """
    Check actionsV2 for failed actions.
    Each action has: { type, result: { type: "ActionSucceededLog" | "ActionFailedLog", ... } }
    Also check top-level actionsStatus.type.
    """
    # Top-level quick check
    actions_status = log_entry.get("actionsStatus", {})
    if isinstance(actions_status, dict) and actions_status.get("type") == "Failed":
        return RoutingAlert(
            severity=AlertSeverity.CRITICAL,
            title="Post-Routing Actions Failed (Top-Level)",
            details=(
                "The overall `actionsStatus` is *Failed*. "
                "One or more HubSpot post-routing actions did not complete.\n"
                "Manual HubSpot cleanup is likely required."
            ),
        )

    # Detailed action-level check
    actions = log_entry.get("actionsV2", [])
    if not actions:
        return None

    failed = []
    for action in actions:
        result = action.get("result", {})
        result_type = result.get("type", "") if isinstance(result, dict) else ""
        action_type = action.get("type", "Unknown")

        if "Failed" in result_type or "Error" in result_type:
            error_msg = result.get("error", "") or result.get("message", "")
            failed.append(f"• *{action_type}*: {error_msg}" if error_msg else f"• *{action_type}*")
        # Also check for retry (indicates at least one failure)
        retry = action.get("retry")
        if retry and not failed:  # only flag if we haven't already flagged
            failed.append(f"• *{action_type}*: action required retry")

    if failed:
        return RoutingAlert(
            severity=AlertSeverity.CRITICAL,
            title="Post-Routing Action Failures",
            details=(
                "The following post-routing actions failed:\n"
                + "\n".join(failed)
                + "\n\nThis may mean ownership wasn't set, the meeting "
                "engagement wasn't created, and Slack notifications "
                "never fired. Manual HubSpot cleanup required."
            ),
        )
    return None


def _check_routing_errors(log_entry: Dict[str, Any]) -> Optional[RoutingAlert]:
    """Check the explicit routingErrors array."""
    errors = log_entry.get("routingErrors", [])
    if not errors:
        return None

    error_lines = []
    for err in errors:
        if isinstance(err, dict):
            error_lines.append(f"• {err.get('message', err.get('type', str(err)))}")
        else:
            error_lines.append(f"• {err}")

    return RoutingAlert(
        severity=AlertSeverity.CRITICAL,
        title="Routing Errors",
        details=(
            "Chili Piper reported explicit routing errors:\n"
            + "\n".join(error_lines)
        ),
    )


def _check_fallthrough(
    log_entry: Dict[str, Any],
) -> Optional[RoutingAlert]:
    """Flag when the lead fell through all named rules to the catch-all."""
    rules_evaluated = log_entry.get("rulesEvaluated", [])
    matched_path = log_entry.get("matchedPath", {})

    if not rules_evaluated:
        return None

    total_rules = len(rules_evaluated)
    passed_rules = sum(
        1 for r in rules_evaluated
        if r.get("passed") is True or r.get("result") == "matched"
    )

    if total_rules > 3 and passed_rules <= 1:
        # Try to get matched rule type
        route = matched_path.get("route", {}) if isinstance(matched_path, dict) else {}
        rule_type = route.get("ruleType", "catch-all")
        return RoutingAlert(
            severity=AlertSeverity.INFO,
            title="Fall-Through Routing",
            details=(
                f"Lead fell through {total_rules - 1} rules before matching "
                f"a *{rule_type}* rule. This is expected for brand-new contacts "
                f"with no CRM history, but worth checking if the final segment is correct."
            ),
        )
    return None


# ── HubSpot post-routing verification ─────────────────────────────────

def _cp_ownership_action_succeeded(log_entry: Dict[str, Any]) -> Optional[str]:
    """
    Check if CP's HubspotUpdateOwnershipLog action succeeded.
    Returns the succeededAt timestamp if so, None otherwise.
    """
    for action in log_entry.get("actionsV2", []):
        if action.get("type") == "HubspotUpdateOwnershipLog":
            result = action.get("result", {})
            if isinstance(result, dict) and "Succeeded" in result.get("type", ""):
                return result.get("succeededAt", "")
    return None


async def _resolve_assigned_rep_name(
    cp_user_id: str = "",
) -> str:
    """
    Resolve the assigned rep's name from the CP user cache.
    The cache is populated on first use from CP's user list API,
    which maps userId → display name directly.
    """
    await _ensure_cp_user_cache()

    if cp_user_id and cp_user_id in _cp_user_name_cache:
        return _cp_user_name_cache[cp_user_id]

    return ""


async def _check_hubspot_ownership_sync(
    lead_email: str,
    cp_assigned_rep: str,
    log_entry: Dict[str, Any],
) -> Optional[RoutingAlert]:
    """
    After CP routes a lead:
    1. Check if the ownership action succeeded
    2. Verify the HubSpot contact has an owner
    3. Check property history — if ownership changed AFTER CP set it,
       something overwrote the routing decision
    """
    if not lead_email or not settings.hubspot_api_key:
        return None

    # Did CP's ownership action succeed?
    ownership_set_at = _cp_ownership_action_succeeded(log_entry)

    contact = await hubspot_service.get_contact_by_email(lead_email)
    if not contact:
        return None

    contact_id = contact.get("id", "")
    hs_owner_id = contact.get("properties", {}).get("hubspot_owner_id")
    link = hubspot_service.contact_link(contact_id)

    if not hs_owner_id:
        # CP tried to set owner but contact still has none
        if ownership_set_at:
            return RoutingAlert(
                severity=AlertSeverity.WARNING,
                title="HubSpot Owner Not Set After Routing",
                details=(
                    f"Chili Piper's ownership action reports success at "
                    f"`{ownership_set_at}`, but the HubSpot contact has no "
                    f"`hubspot_owner_id`. Something cleared it after routing.\n"
                    f"<{link}|View Contact in HubSpot>"
                ),
            )
        return None

    # If ownership action succeeded, check if someone changed it afterward
    if ownership_set_at:
        history = await hubspot_service.get_property_history(
            contact_id, ["hubspot_owner_id"]
        )
        owner_changes = history.get("hubspot_owner_id", [])

        # Find changes that happened AFTER CP set ownership
        post_routing_changes = []
        for change in owner_changes:
            ts_str = change.get("timestamp", "")
            if not ts_str:
                continue
            # If this change happened after CP's action, it's a post-routing overwrite
            if ts_str > ownership_set_at:
                post_routing_changes.append(change)

        if post_routing_changes:
            # Resolve current owner name
            owner = await hubspot_service.get_owner(hs_owner_id)
            current_name = ""
            if owner:
                current_name = (
                    f"{owner.get('firstName', '')} {owner.get('lastName', '')}".strip()
                    or owner.get("email", "")
                )

            change_lines = []
            for c in post_routing_changes[:5]:
                source = c.get("source", "unknown")
                source_id = c.get("sourceId", "")
                val = c.get("value", "?")
                ts = c.get("timestamp", "")
                label = source
                if source_id:
                    label += f" ({source_id})"
                change_lines.append(f"• `{ts}` → owner *{val}* via {label}")

            return RoutingAlert(
                severity=AlertSeverity.WARNING,
                title="Ownership Changed After Routing",
                details=(
                    f"Chili Piper set ownership at `{ownership_set_at}`, but "
                    f"it was changed afterward. Current owner: *{current_name}*.\n\n"
                    + "\n".join(change_lines)
                    + f"\n\n<{link}|View Contact in HubSpot>"
                ),
            )

    return None


async def _check_ownership_flipflop(
    lead_email: str,
) -> Optional[RoutingAlert]:
    """
    Check the property change history on hubspot_owner_id.
    Flag if ownership changed 3+ times in 24 hours — that's a
    flip-flop from competing workflows or manual overrides.
    """
    if not lead_email or not settings.hubspot_api_key:
        return None

    contact = await hubspot_service.get_contact_by_email(lead_email)
    if not contact:
        return None

    contact_id = contact.get("id", "")
    history = await hubspot_service.get_property_history(
        contact_id, ["hubspot_owner_id"]
    )

    owner_changes = history.get("hubspot_owner_id", [])
    if len(owner_changes) < 3:
        return None

    # Check if 3+ changes happened within 24 hours
    now = datetime.now(timezone.utc)
    recent_changes = []
    for change in owner_changes:
        ts_str = change.get("timestamp", "")
        if not ts_str:
            continue
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            if (now - ts).total_seconds() < 86400:  # 24 hours
                recent_changes.append(change)
        except (ValueError, TypeError):
            continue

    if len(recent_changes) >= 3:
        # Build a readable timeline
        timeline_lines = []
        for c in recent_changes[:8]:  # cap at 8 to avoid wall of text
            source = c.get("source", "unknown")
            source_id = c.get("sourceId", "")
            val = c.get("value", "(empty)")
            ts = c.get("timestamp", "")
            label = f"{source}"
            if source_id:
                label += f" ({source_id})"
            timeline_lines.append(f"• `{ts}` → *{val}* via {label}")

        link = hubspot_service.contact_link(contact_id)
        return RoutingAlert(
            severity=AlertSeverity.CRITICAL,
            title="Ownership Flip-Flop Detected",
            details=(
                f"Contact owner changed *{len(recent_changes)} times* in "
                f"the last 24 hours. Likely competing workflows or manual "
                f"overrides fighting each other.\n\n"
                + "\n".join(timeline_lines)
                + f"\n\n<{link}|View Contact in HubSpot>"
            ),
        )
    return None


def _meeting_was_booked(log_entry: Dict[str, Any]) -> bool:
    """
    Check if Chili Piper confirms a meeting was actually booked.
    Returns True only when the lead completed the booking flow:
    - meetingId is present in the log, OR
    - MeetingOfferedLog action has MeetingOfferScheduledLogResult

    Returns False for timeouts (lead didn't book) or when no meeting
    offer was part of the routing flow.
    """
    # Direct signal: CP recorded a meeting ID
    if log_entry.get("meetingId"):
        return True

    # Check the MeetingOfferedLog action result
    for action in log_entry.get("actionsV2", []):
        if action.get("type") == "MeetingOfferedLog":
            result = action.get("result", {})
            result_type = result.get("type", "") if isinstance(result, dict) else ""
            if result_type == "MeetingOfferScheduledLogResult":
                return True
            # Explicit timeout or other non-scheduled result = not booked
            return False

    # No MeetingOfferedLog action at all = no meeting flow
    return False


async def _check_meeting_created(
    lead_email: str,
    routing_timestamp: str,
    log_entry: Dict[str, Any],
) -> Optional[RoutingAlert]:
    """
    After CP routes a lead AND confirms a meeting was booked:
    1. Verify a meeting engagement exists on the HubSpot contact
    2. Verify the meeting is owned by the rep CP assigned

    Only runs when CP confirms a booking (meetingId or ScheduledLogResult).
    Skips timeouts and non-booking flows entirely.

    Waits at least 5 minutes after routing before checking, since
    HubSpot engagement creation is async and can take a few minutes.
    """
    if not lead_email or not settings.hubspot_api_key:
        return None

    # Don't check meetings that were routed less than 5 minutes ago —
    # HubSpot engagement creation is async and needs time to sync
    try:
        routed_at = datetime.fromisoformat(
            routing_timestamp.replace("Z", "+00:00")
        )
        if (datetime.now(timezone.utc) - routed_at).total_seconds() < 300:
            return None
    except (ValueError, TypeError):
        pass

    # Only check if CP confirms a meeting was actually booked
    if not _meeting_was_booked(log_entry):
        return None

    await _ensure_cp_user_cache()

    contact = await hubspot_service.get_contact_by_email(lead_email)
    if not contact:
        return None

    contact_id = contact.get("id", "")
    link = hubspot_service.contact_link(contact_id)

    meetings = await hubspot_service.get_contact_meetings(
        contact_id, after_timestamp=routing_timestamp
    )

    # Who did CP assign?
    cp_user_id = ""
    assignments = log_entry.get("assignments", [])
    if assignments and isinstance(assignments[0], dict):
        cp_user_id = assignments[0].get("userId", "")
    cp_rep_name = _cp_user_name_cache.get(cp_user_id, cp_user_id)
    cp_rep_email = _cp_user_email_cache.get(cp_user_id, "")

    if not meetings:
        return RoutingAlert(
            severity=AlertSeverity.CRITICAL,
            title="No Meeting Created in HubSpot",
            details=(
                f"Chili Piper confirmed a meeting was booked with "
                f"*{cp_rep_name}* (meetingId: "
                f"`{log_entry.get('meetingId', 'N/A')}`) but no meeting "
                f"engagement was found on the HubSpot contact.\n"
                f"The HubSpot engagement creation likely failed — "
                f"the rep may not know about this meeting.\n"
                f"<{link}|View Contact in HubSpot>"
            ),
        )

    # Meeting exists — verify it's with the right rep (compare by email,
    # since CP↔HubSpot owner ID mappings can be stale/wrong)
    meeting = meetings[0]  # Most recent meeting
    meeting_props = meeting.get("properties", {})
    meeting_owner_id = meeting_props.get("hubspot_owner_id", "")
    meeting_title = meeting_props.get("hs_meeting_title", "")
    meeting_start = meeting_props.get("hs_meeting_start_time", "")

    if meeting_owner_id and cp_rep_email:
        actual_owner = await hubspot_service.get_owner(meeting_owner_id)
        if actual_owner:
            actual_email = (actual_owner.get("email") or "").lower()
            actual_name = (
                f"{actual_owner.get('firstName', '')} "
                f"{actual_owner.get('lastName', '')}".strip()
                or actual_email
            )

            # Compare by email — the only reliable shared key
            if actual_email and actual_email != cp_rep_email:
                return RoutingAlert(
                    severity=AlertSeverity.WARNING,
                    title="Meeting Owner Mismatch",
                    details=(
                        f"Chili Piper assigned this lead to *{cp_rep_name}* "
                        f"(`{cp_rep_email}`), but the HubSpot meeting is "
                        f"owned by *{actual_name}* (`{actual_email}`).\n"
                        f"*Meeting:* {meeting_title}\n"
                        f"*Scheduled:* {meeting_start}\n"
                        f"The meeting may have been reassigned after routing.\n"
                        f"<{link}|View Contact in HubSpot>"
                    ),
                )

    return None


# ── Main analysis function ────────────────────────────────────────────

async def analyze_routing_event(log_entry: Dict[str, Any]) -> List[RoutingAlert]:
    """
    Run all checks on a single concierge log entry.
    Returns list of alerts (may be empty if everything looks clean).
    """
    alerts: List[RoutingAlert] = []

    # Extract form values from CP's nested structure
    form_values = _extract_form_values(log_entry)

    # Extract identifiers from the real CP log structure
    lead_email = log_entry.get("guestEmail", "")
    lead_name = (
        f"{form_values.get('firstname', '')} {form_values.get('lastname', '')}"
    ).strip() or lead_email
    lead_company = (
        form_values.get("company", "")
        or log_entry.get("accountName", "")
        or ""
    )
    router_name = log_entry.get("routerName", "")
    timestamp = log_entry.get("triggeredAt", "")
    crm_url = log_entry.get("guestCrmUrl", "")

    # Extract CP userId from assignments
    cp_user_id = ""
    assignments = log_entry.get("assignments", [])
    if assignments and isinstance(assignments[0], dict):
        cp_user_id = assignments[0].get("userId", "")

    # Resolve the assigned rep name from CP user cache
    assigned_rep_name = await _resolve_assigned_rep_name(cp_user_id)
    # Fallback to CP userId if cache miss
    if not assigned_rep_name:
        assigned_rep_name = cp_user_id

    # Extract matched rule info — prefer name over type
    matched_rule_name = ""
    matched_path = log_entry.get("matchedPath", {})
    if isinstance(matched_path, dict):
        route = matched_path.get("route", {})
        if isinstance(route, dict):
            matched_rule_name = (
                route.get("name", "")
                or route.get("ruleType", "")
            )
    # Also show assignment type (Ownership, StrictRoundRobin, etc.)
    if assignments and isinstance(assignments[0], dict):
        assign_type = assignments[0].get("type", "")
        if assign_type and assign_type != matched_rule_name:
            if matched_rule_name:
                matched_rule_name = f"{matched_rule_name} ({assign_type})"
            else:
                matched_rule_name = assign_type

    # Run CP-data checks
    checks: List[Optional[RoutingAlert]] = [
        _check_action_failures(log_entry),
        _check_routing_errors(log_entry),
        _check_segment_mismatch(form_values),
        _check_fallthrough(log_entry),
    ]

    # Run async HubSpot checks (only if HubSpot is configured)
    if settings.hubspot_api_key and lead_email:
        hs_checks = await asyncio.gather(
            _check_hubspot_ownership_sync(lead_email, assigned_rep_name, log_entry),
            _check_ownership_flipflop(lead_email),
            _check_meeting_created(lead_email, timestamp, log_entry),
            return_exceptions=True,
        )
        for result in hs_checks:
            if isinstance(result, RoutingAlert):
                checks.append(result)
            elif isinstance(result, Exception):
                print(f"[ROUTING_MONITOR] HubSpot check error: {result}")

    for alert in checks:
        if alert is not None:
            alert.lead_name = alert.lead_name or lead_name
            alert.lead_email = alert.lead_email or lead_email
            alert.lead_company = alert.lead_company or lead_company
            alert.assigned_rep = alert.assigned_rep or assigned_rep_name
            alert.router_name = alert.router_name or router_name
            alert.matched_rule = alert.matched_rule or matched_rule_name
            alert.timestamp = alert.timestamp or timestamp
            alert.raw_log = log_entry
            alerts.append(alert)

    return alerts


# ── Slack notification ────────────────────────────────────────────────

SEVERITY_EMOJI = {
    AlertSeverity.CRITICAL: "🚨",
    AlertSeverity.WARNING: "⚠️",
    AlertSeverity.INFO: "ℹ️",
}


def _build_alert_blocks(alert: RoutingAlert) -> List[Dict[str, Any]]:
    """Build Slack Block Kit message for a single routing alert."""
    emoji = SEVERITY_EMOJI.get(alert.severity, "❓")
    severity_label = alert.severity.upper()

    return [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{emoji} Routing Alert: {alert.title}",
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Severity:*\n{severity_label}"},
                {"type": "mrkdwn", "text": f"*Router:*\n{alert.router_name or 'Unknown'}"},
                {"type": "mrkdwn", "text": f"*Lead:*\n{alert.lead_name or 'Unknown'}"},
                {"type": "mrkdwn", "text": f"*Email:*\n{alert.lead_email or 'Unknown'}"},
            ],
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Company:*\n{alert.lead_company or 'Unknown'}"},
                {"type": "mrkdwn", "text": f"*Assigned To:*\n{alert.assigned_rep or 'Unknown'}"},
                {"type": "mrkdwn", "text": f"*Matched Rule:*\n{alert.matched_rule or 'N/A'}"},
                {"type": "mrkdwn", "text": f"*Time:*\n{alert.timestamp or 'Unknown'}"},
            ],
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": alert.details},
        },
    ]


def _build_summary_blocks(
    alerts: List[RoutingAlert],
    period_start: str,
    period_end: str,
    total_events: int,
) -> List[Dict[str, Any]]:
    """Build a periodic summary message when there are no critical/warning alerts."""
    critical = sum(1 for a in alerts if a.severity == AlertSeverity.CRITICAL)
    warnings = sum(1 for a in alerts if a.severity == AlertSeverity.WARNING)
    infos = sum(1 for a in alerts if a.severity == AlertSeverity.INFO)

    status_emoji = "✅" if critical == 0 and warnings == 0 else "⚠️"

    return [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{status_emoji} Routing Monitor Summary",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Period:* {period_start} → {period_end}\n"
                    f"*Total routing events:* {total_events}\n"
                    f"*🚨 Critical:* {critical}  |  *⚠️ Warning:* {warnings}  |  *ℹ️ Info:* {infos}"
                ),
            },
        },
    ]


async def post_routing_alert(alert: RoutingAlert) -> bool:
    """Post a single routing alert to the routing monitor Slack channel."""
    client = _get_slack_client()
    channel = settings.slack_routing_monitor_channel
    if not client or not channel:
        print(f"[ROUTING_MONITOR] Slack not configured — alert: {alert.title}")
        return False

    blocks = _build_alert_blocks(alert)
    try:
        await client.chat_postMessage(
            channel=channel,
            text=f"{SEVERITY_EMOJI.get(alert.severity, '')} Routing Alert: {alert.title} — {alert.lead_name}",
            blocks=blocks,
        )
        print(f"[ROUTING_MONITOR] Posted alert: {alert.title} for {alert.lead_name}")
        return True
    except SlackApiError as e:
        print(f"[ROUTING_MONITOR] Failed to post alert: {e}")
        return False


async def post_summary(
    alerts: List[RoutingAlert],
    period_start: str,
    period_end: str,
    total_events: int,
) -> bool:
    """Post a periodic summary to the routing monitor channel."""
    client = _get_slack_client()
    channel = settings.slack_routing_monitor_channel
    if not client or not channel:
        return False

    blocks = _build_summary_blocks(alerts, period_start, period_end, total_events)
    try:
        await client.chat_postMessage(
            channel=channel,
            text=f"Routing Monitor Summary: {total_events} events",
            blocks=blocks,
        )
        return True
    except SlackApiError as e:
        print(f"[ROUTING_MONITOR] Failed to post summary: {e}")
        return False


# ── Poll + Analyze orchestrator ───────────────────────────────────────

# Track which log entries we've already processed
_seen_log_ids: Set[str] = set()
_MAX_SEEN = 10_000


async def poll_and_analyze(
    lookback_minutes: int = 15,
) -> Dict[str, Any]:
    """
    Main polling loop iteration:
    1. List all routers
    2. For each router, pull concierge logs for the last N minutes
    3. Analyze each new log entry
    4. Post alerts for critical/warning findings
    5. Return summary stats
    """
    global _seen_log_ids

    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=lookback_minutes)

    stats: Dict[str, Any] = {
        "period_start": start.isoformat(),
        "period_end": now.isoformat(),
        "routers_checked": 0,
        "total_events": 0,
        "new_events": 0,
        "alerts_posted": 0,
        "alerts": [],
    }

    # 1. Get routers
    routers = await chilipiper_service.list_routers()
    if not routers:
        print("[ROUTING_MONITOR] No routers found or API call failed")
        return stats

    stats["routers_checked"] = len(routers)

    all_alerts: List[RoutingAlert] = []

    # 2. For each router, pull logs
    for router in routers:
        router_id = router.get("id") or router.get("routerId", "")
        workspace_id = router.get("workspaceId", "")
        router_name = router.get("name") or router.get("slug", "")

        if not router_id or not workspace_id:
            continue

        logs = await chilipiper_service.get_concierge_logs(
            workspace_id=workspace_id,
            router_id=router_id,
            start=start,
            end=now,
        )

        stats["total_events"] += len(logs)

        for log_entry in logs:
            # Deduplicate
            log_id = (
                log_entry.get("id")
                or log_entry.get("logId")
                or log_entry.get("timestamp", "")
                + str(log_entry.get("inputLeadData", {}).get("email", ""))
            )
            if log_id in _seen_log_ids:
                continue
            _seen_log_ids.add(log_id)
            if len(_seen_log_ids) > _MAX_SEEN:
                _seen_log_ids = set(list(_seen_log_ids)[-(_MAX_SEEN // 2):])

            stats["new_events"] += 1
            log_entry.setdefault("routerName", router_name)

            # 3. Analyze
            alerts = await analyze_routing_event(log_entry)
            all_alerts.extend(alerts)

    # 4. Post alerts (critical and warning only — info is summary-only)
    for alert in all_alerts:
        if alert.severity in (AlertSeverity.CRITICAL, AlertSeverity.WARNING):
            posted = await post_routing_alert(alert)
            if posted:
                stats["alerts_posted"] += 1

    stats["alerts"] = [
        {
            "severity": a.severity,
            "title": a.title,
            "lead": a.lead_name,
            "company": a.lead_company,
            "rep": a.assigned_rep,
        }
        for a in all_alerts
    ]

    # 5. Always post a summary when there were events
    if stats["total_events"] > 0:
        await post_summary(
            all_alerts,
            start.strftime("%Y-%m-%d %H:%M UTC"),
            now.strftime("%Y-%m-%d %H:%M UTC"),
            stats["total_events"],
        )

    return stats
