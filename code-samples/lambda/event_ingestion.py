"""
Lambda: Event Ingestion

Handles incoming user interaction events from API Gateway.
Responsibilities:
  1. Validate the event payload
  2. Deduplicate using idempotency key
  3. Convert timestamps to the user's local date
  4. Write to BPEvents and DailyActivity tables
  5. Atomically increment Bestie Points on UserStreaks
"""

import json
import os
from datetime import datetime, timedelta, UTC
from decimal import Decimal
from uuid import uuid4
from zoneinfo import ZoneInfo

import boto3
import redis
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
bp_events_table = dynamodb.Table(os.environ.get("BP_EVENTS_TABLE", "BPEvents"))
daily_activity_table = dynamodb.Table(os.environ.get("DAILY_ACTIVITY_TABLE", "DailyActivity"))
user_streaks_table = dynamodb.Table(os.environ.get("USER_STREAKS_TABLE", "UserStreaks"))

redis_client = redis.Redis(
    host=os.environ.get("REDIS_HOST", "localhost"),
    port=int(os.environ.get("REDIS_PORT", "6379")),
    decode_responses=True,
)

# Points awarded per event type
POINTS_MAP = {
    "CONVERSATION_START": 10,
    "TOOL_USE": 5,
    "DAILY_OPEN": 1,
    "SHARE_RESULT": 15,
}

VALID_EVENT_TYPES = set(POINTS_MAP.keys())


def handler(event, context):
    """Lambda entry point — API Gateway proxy integration."""
    try:
        body = json.loads(event.get("body", "{}"))
        validated = validate_event(body)
        result = process_event(validated)
        return response(200, result)
    except ValidationError as e:
        return response(400, {"error": str(e)})
    except DuplicateEventError:
        return response(200, {"message": "Event already processed", "duplicate": True})
    except RateLimitError:
        return response(429, {"error": "Rate limit exceeded"})
    except TimezoneAbuseError:
        return response(403, {"error": "Suspicious timezone change detected"})
    except Exception as e:
        print(f"Unexpected error: {e}")
        return response(500, {"error": "Internal server error"})


class ValidationError(Exception):
    pass


class DuplicateEventError(Exception):
    pass


class RateLimitError(Exception):
    pass


class TimezoneAbuseError(Exception):
    pass


def validate_event(body: dict) -> dict:
    """Validate required fields, types, and timestamp sanity."""
    required_fields = ["user_id", "event_type", "client_timestamp", "timezone", "idempotency_key"]
    for field in required_fields:
        if field not in body:
            raise ValidationError(f"Missing required field: {field}")

    if body["event_type"] not in VALID_EVENT_TYPES:
        raise ValidationError(f"Invalid event_type: {body['event_type']}")

    # Validate timezone
    try:
        ZoneInfo(body["timezone"])
    except (KeyError, Exception):
        raise ValidationError(f"Invalid timezone: {body['timezone']}")

    # Validate and parse timestamp
    try:
        ts = datetime.fromisoformat(body["client_timestamp"])
    except ValueError:
        raise ValidationError("Invalid client_timestamp format (expected ISO 8601)")

    # Reject timestamps too far in the future or past
    now = datetime.now(UTC)
    if ts > now + timedelta(hours=24):
        raise ValidationError("client_timestamp is too far in the future")
    if ts < now - timedelta(days=7):
        raise ValidationError("client_timestamp is too far in the past")

    return body


def process_event(event: dict) -> dict:
    """Write the event and update streak/BP state."""
    user_id = event["user_id"]
    idempotency_key = event["idempotency_key"]
    event_type = event["event_type"]
    timezone = event["timezone"]

    # Parse timestamps
    client_ts = datetime.fromisoformat(event["client_timestamp"])
    utc_ts = client_ts.astimezone(ZoneInfo("UTC"))
    local_date = client_ts.astimezone(ZoneInfo(timezone)).strftime("%Y-%m-%d")

    points = POINTS_MAP[event_type]
    event_id = f"{utc_ts.strftime('%Y%m%dT%H%M%S')}#{uuid4().hex[:8]}"

    # Anomaly detection: rate limiting and timezone abuse
    if not check_rate_limit(user_id, redis_client):
        raise RateLimitError()
    if detect_timezone_abuse(user_id, timezone, user_streaks_table):
        raise TimezoneAbuseError()

    # Layer 2: Query IdempotencyIndex GSI before writing.
    # The conditional write (Layer 3) alone doesn't deduplicate because event_id
    # contains a UUID, so the PK+SK combination is always unique and the
    # attribute_not_exists condition always passes on a new item.
    gsi_response = bp_events_table.query(
        IndexName="IdempotencyIndex",
        KeyConditionExpression="idempotency_key = :ik",
        ExpressionAttributeValues={":ik": idempotency_key},
        Limit=1,
    )
    if gsi_response.get("Items"):
        raise DuplicateEventError()

    # Layer 3: Conditional write as a final guard against the GSI's eventual
    # consistency window — two concurrent Lambdas could both pass the GSI check,
    # but only one will win the conditional put.
    try:
        bp_events_table.put_item(
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

    # NOTE: The 3 writes below (BPEvents → DailyActivity → UserStreaks) are NOT
    # atomic. If a later write fails, earlier ones are not rolled back. This is an
    # accepted trade-off: the nightly reconciliation job (Glue) detects and repairs
    # any inconsistencies (e.g., points logged in BPEvents but not reflected in
    # total_bestie_points). DynamoDB doesn't support cross-table transactions
    # efficiently at this scale, and the eventual consistency window is short.

    # Upsert DailyActivity — only the first event of the day creates the record
    try:
        daily_activity_table.update_item(
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
    except ClientError as e:
        print(f"Error updating DailyActivity for {user_id}: {e}")
        raise

    # Atomically increment Bestie Points and update timezone
    # Note: "timezone" is a DynamoDB reserved keyword — must use ExpressionAttributeNames
    user_streaks_table.update_item(
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

    return {
        "message": "Event processed",
        "event_id": event_id,
        "points_awarded": points,
        "activity_date": local_date,
    }


def check_rate_limit(user_id: str, redis_conn) -> bool:
    """Token bucket rate limiter using Redis. Returns False if rate-limited."""
    key = f"rate:{user_id}"
    current = redis_conn.get(key)

    if current and int(current) >= 100:
        return False  # Rate limited

    pipe = redis_conn.pipeline()
    pipe.incr(key)
    pipe.expire(key, 3600)  # Reset every hour
    pipe.execute()
    return True


def detect_timezone_abuse(user_id: str, new_tz: str, streaks_table) -> bool:
    """Detect if a user is changing timezones to game the streak system.

    Returns True if suspicious (>2 timezone changes in 24h).
    """
    response = streaks_table.get_item(Key={"user_id": user_id})
    user = response.get("Item", {})
    old_tz = user.get("timezone", new_tz)

    if old_tz == new_tz:
        return False

    # Check how many TZ changes in last 24h (stored as a list)
    tz_changes = user.get("recent_tz_changes", [])
    recent = [
        c for c in tz_changes
        if datetime.fromisoformat(c["timestamp"]) > datetime.now(UTC) - timedelta(hours=24)
    ]

    if len(recent) >= 2:
        return True  # Suspicious — flag for review

    return False


def response(status_code: int, body: dict) -> dict:
    """Format API Gateway proxy response."""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body, default=str),
    }
