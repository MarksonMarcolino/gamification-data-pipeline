"""Helper functions for the Gamification Pipeline POC notebook."""

import json
from datetime import datetime, timedelta, UTC
from decimal import Decimal
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd
from botocore.exceptions import ClientError


# ── Exception classes ─────────────────────────────────────────────────────

class ValidationError(Exception):
    pass

class DuplicateEventError(Exception):
    pass

class RateLimitError(Exception):
    pass

class TimezoneAbuseError(Exception):
    pass


# ── Constants ─────────────────────────────────────────────────────────────

POINTS_MAP = {
    "CONVERSATION_START": 10,
    "TOOL_USE": 5,
    "DAILY_OPEN": 1,
    "SHARE_RESULT": 15,
}
VALID_EVENT_TYPES = set(POINTS_MAP.keys())


# ── Shared utilities ─────────────────────────────────────────────────────

def decimal_to_native(obj):
    """Convert DynamoDB Decimal types to Python int/float for pandas compatibility."""
    if isinstance(obj, Decimal):
        return int(obj) if obj == int(obj) else float(obj)
    if isinstance(obj, dict):
        return {k: decimal_to_native(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [decimal_to_native(i) for i in obj]
    return obj


def scan_table_to_df(table) -> pd.DataFrame:
    """Scan a DynamoDB table and return a pandas DataFrame with native types."""
    items = table.scan()["Items"]
    if not items:
        return pd.DataFrame()
    return pd.DataFrame([decimal_to_native(item) for item in items])


# ── Data generation ──────────────────────────────────────────────────────

def make_event(user_id, event_type, ts, timezone, source=None, idemp_key=None, rng=None):
    """Create an event payload in the exact API format."""
    sources = ["mobile_app", "web", "whatsapp"]
    local_ts = ts.astimezone(ZoneInfo(timezone))
    if idemp_key is None:
        idemp_key = f"{user_id}:{event_type}:{local_ts.isoformat()}"
    return {
        "user_id": user_id,
        "event_type": event_type,
        "client_timestamp": local_ts.isoformat(),
        "timezone": timezone,
        "idempotency_key": idemp_key,
        "source": source or (rng.choice(sources) if rng else "mobile_app"),
    }


# ── Validation ───────────────────────────────────────────────────────────

def validate_event(body: dict) -> dict:
    """Validate required fields, types, and timestamp sanity."""
    required_fields = ["user_id", "event_type", "client_timestamp", "timezone", "idempotency_key"]
    for field in required_fields:
        if field not in body:
            raise ValidationError(f"Missing required field: {field}")

    if body["event_type"] not in VALID_EVENT_TYPES:
        raise ValidationError(f"Invalid event_type: {body['event_type']}")

    try:
        ZoneInfo(body["timezone"])
    except (KeyError, Exception):
        raise ValidationError(f"Invalid timezone: {body['timezone']}")

    try:
        ts = datetime.fromisoformat(body["client_timestamp"])
    except ValueError:
        raise ValidationError("Invalid client_timestamp format (expected ISO 8601)")

    now = datetime.now(UTC)
    if ts > now + timedelta(hours=24):
        raise ValidationError("client_timestamp is too far in the future")
    if ts < now - timedelta(days=7):
        raise ValidationError("client_timestamp is too far in the past")

    return body


# ── Anomaly detection ────────────────────────────────────────────────────

def check_rate_limit(user_id: str, redis_conn) -> bool:
    """Token bucket rate limiter using Redis. Returns False if rate-limited."""
    key = f"rate:{user_id}"
    current = redis_conn.get(key)
    if current and int(current) >= 100:
        return False
    pipe = redis_conn.pipeline()
    pipe.incr(key)
    pipe.expire(key, 3600)
    pipe.execute()
    return True


def detect_timezone_abuse(user_id: str, new_tz: str, streaks_table) -> bool:
    """Detect if a user is changing timezones to game the streak system.
    Returns True if suspicious (>2 timezone changes in 24h)."""
    resp = streaks_table.get_item(Key={"user_id": user_id})
    user = resp.get("Item", {})
    old_tz = user.get("timezone", new_tz)
    if old_tz == new_tz:
        return False
    tz_changes = user.get("recent_tz_changes", [])
    recent = [
        c for c in tz_changes
        if datetime.fromisoformat(c["timestamp"]) > datetime.now(UTC) - timedelta(hours=24)
    ]
    return len(recent) >= 2


# ── Core event processing ───────────────────────────────────────────────

def process_event(event: dict, bp_tbl, daily_tbl, streaks_tbl, redis_conn) -> dict:
    """Write the event and update streak/BP state."""
    user_id = event["user_id"]
    idempotency_key = event["idempotency_key"]
    event_type = event["event_type"]
    timezone = event["timezone"]

    client_ts = datetime.fromisoformat(event["client_timestamp"])
    utc_ts = client_ts.astimezone(ZoneInfo("UTC"))
    local_date = client_ts.astimezone(ZoneInfo(timezone)).strftime("%Y-%m-%d")

    points = POINTS_MAP[event_type]
    event_id = f"{utc_ts.strftime('%Y%m%dT%H%M%S')}#{uuid4().hex[:8]}"

    if not check_rate_limit(user_id, redis_conn):
        raise RateLimitError()
    if detect_timezone_abuse(user_id, timezone, streaks_tbl):
        raise TimezoneAbuseError()

    # Layer 2 (Bug Fix #1): Query IdempotencyIndex GSI before writing.
    gsi_resp = bp_tbl.query(
        IndexName="IdempotencyIndex",
        KeyConditionExpression="idempotency_key = :ik",
        ExpressionAttributeValues={":ik": idempotency_key},
        Limit=1,
    )
    if gsi_resp.get("Items"):
        raise DuplicateEventError()

    # Layer 3: Conditional write — final guard against GSI eventual consistency
    try:
        bp_tbl.put_item(
            Item={
                "user_id": user_id,
                "event_id": event_id,
                "event_type": event_type,
                "points_awarded": points,
                "source": event.get("source", "unknown"),
                "idempotency_key": idempotency_key,
                "client_timestamp": event["client_timestamp"],
                "created_at": utc_ts.isoformat(),
                "ttl": int((utc_ts + timedelta(days=90)).timestamp()),
            },
            ConditionExpression="attribute_not_exists(idempotency_key)",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            raise DuplicateEventError()
        raise

    # Upsert DailyActivity
    daily_tbl.update_item(
        Key={"user_id": user_id, "activity_date": local_date},
        UpdateExpression=(
            "SET first_event_id = if_not_exists(first_event_id, :eid), "
            "created_at = if_not_exists(created_at, :now) "
            "ADD events_count :one, points_earned :pts"
        ),
        ExpressionAttributeValues={
            ":eid": event_id,
            ":now": utc_ts.isoformat(),
            ":one": 1,
            ":pts": points,
        },
    )

    # Read old timezone for tracking changes
    old_user = streaks_tbl.get_item(Key={"user_id": user_id}).get("Item", {})
    old_tz = old_user.get("timezone")

    # Atomically increment Bestie Points and update timezone
    streaks_tbl.update_item(
        Key={"user_id": user_id},
        UpdateExpression=(
            "ADD total_bestie_points :pts "
            "SET #tz = :tz, streak_updated_at = :now"
        ),
        ExpressionAttributeNames={"#tz": "timezone"},
        ExpressionAttributeValues={
            ":pts": points,
            ":tz": timezone,
            ":now": utc_ts.isoformat(),
        },
    )

    # Bug Fix #4a: Track timezone changes for abuse detection
    if old_tz and old_tz != timezone:
        streaks_tbl.update_item(
            Key={"user_id": user_id},
            UpdateExpression="SET recent_tz_changes = list_append(if_not_exists(recent_tz_changes, :empty), :change)",
            ExpressionAttributeValues={
                ":empty": [],
                ":change": [{"timezone": timezone, "timestamp": utc_ts.isoformat()}],
            },
        )

    return {
        "message": "Event processed",
        "event_id": event_id,
        "points_awarded": points,
        "activity_date": local_date,
    }


# ── Streak processor ────────────────────────────────────────────────────

def get_previous_date(date_str: str, timezone: str) -> str:
    """Return the calendar date before the given date in the user's timezone.

    Bug Fix #3: Uses timezone-aware arithmetic so DST transitions are handled
    correctly. Anchored at noon to avoid midnight edge cases.
    """
    tz = ZoneInfo(timezone)
    local_date = datetime.strptime(date_str, "%Y-%m-%d").replace(hour=12, tzinfo=tz)
    previous = local_date - timedelta(days=1)
    return previous.strftime("%Y-%m-%d")


def update_cache(user_id: str, streak: int, longest: int, bp, redis_conn):
    """Write-through cache update to Redis."""
    bp_val = int(bp) if isinstance(bp, Decimal) else (bp or 0)
    redis_conn.setex(
        f"streak:{user_id}",
        300,  # 5-minute TTL
        json.dumps({
            "current_streak": streak,
            "longest_streak": longest,
            "total_bestie_points": bp_val,
        }),
    )


def process_streak_update(user_id: str, activity_date: str, streaks_tbl, redis_conn):
    """Core streak logic: compare dates and update the streak counter."""
    resp = streaks_tbl.get_item(Key={"user_id": user_id})
    user = resp.get("Item")

    if not user:
        streaks_tbl.put_item(
            Item={
                "user_id": user_id,
                "current_streak": 1,
                "longest_streak": 1,
                "last_activity_date": activity_date,
                "timezone": "UTC",
                "total_bestie_points": 0,
                "streak_updated_at": datetime.now(ZoneInfo("UTC")).isoformat(),
                "version": 1,
            },
            ConditionExpression="attribute_not_exists(user_id)",
        )
        update_cache(user_id, 1, 1, 0, redis_conn)
        return

    last_activity = user.get("last_activity_date")
    current_streak = int(user.get("current_streak", 0))
    longest_streak = int(user.get("longest_streak", 0))
    version = int(user.get("version", 0))
    timezone = user.get("timezone", "UTC")

    if last_activity == activity_date:
        return  # Already updated today

    yesterday = get_previous_date(activity_date, timezone)

    if last_activity == yesterday:
        new_streak = current_streak + 1
    else:
        new_streak = 1  # Streak broken

    new_longest = max(new_streak, longest_streak)

    streaks_tbl.update_item(
        Key={"user_id": user_id},
        UpdateExpression=(
            "SET current_streak = :cs, "
            "longest_streak = :ls, "
            "last_activity_date = :lad, "
            "streak_updated_at = :now, "
            "version = :new_v"
        ),
        ExpressionAttributeValues={
            ":cs": new_streak,
            ":ls": new_longest,
            ":lad": activity_date,
            ":now": datetime.now(ZoneInfo("UTC")).isoformat(),
            ":new_v": version + 1,
            ":expected_v": version,
        },
        ConditionExpression="version = :expected_v OR attribute_not_exists(version)",
    )

    update_cache(user_id, new_streak, new_longest, user.get("total_bestie_points", 0), redis_conn)
    print(f"  {user_id}: streak {current_streak} -> {new_streak} (date: {activity_date})")


# ── Cache read path ──────────────────────────────────────────────────────

def get_user_streak(user_id, streaks_tbl, redis_conn):
    """Read path: check Redis cache first, fall back to DynamoDB."""
    cached = redis_conn.get(f"streak:{user_id}")
    if cached:
        return json.loads(cached), "CACHE_HIT"
    resp = streaks_tbl.get_item(Key={"user_id": user_id})
    item = decimal_to_native(resp.get("Item", {}))
    return item, "CACHE_MISS"
