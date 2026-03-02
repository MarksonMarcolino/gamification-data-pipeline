"""
Lambda: Streak Processor

Triggered by DynamoDB Streams on the DailyActivity table.
When a new daily activity record is inserted, this function:
  1. Reads the user's current streak state from UserStreaks
  2. Compares the activity date with last_activity_date (in the user's TZ)
  3. Increments, resets, or skips the streak accordingly
  4. Writes back with a conditional update (optimistic locking)
  5. Invalidates the Redis cache
"""

import json
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import boto3
import redis

dynamodb = boto3.resource("dynamodb")
user_streaks_table = dynamodb.Table(os.environ.get("USER_STREAKS_TABLE", "UserStreaks"))

redis_client = redis.Redis(
    host=os.environ.get("REDIS_HOST", "localhost"),
    port=int(os.environ.get("REDIS_PORT", "6379")),
    decode_responses=True,
)

PROCESSED_SET_TTL = 3600  # 1 hour


def handler(event, context):
    """Lambda entry point — processes DynamoDB Stream records."""
    for record in event["Records"]:
        if record["eventName"] != "INSERT":
            continue

        new_image = record["dynamodb"]["NewImage"]
        user_id = new_image["user_id"]["S"]
        activity_date = new_image["activity_date"]["S"]
        event_id = new_image.get("first_event_id", {}).get("S", "unknown")

        # Idempotency: skip if already processed
        if redis_client.sismember(f"processed:{user_id}", event_id):
            print(f"Skipping already-processed event {event_id} for {user_id}")
            continue

        try:
            process_streak_update(user_id, activity_date)
            redis_client.sadd(f"processed:{user_id}", event_id)
            redis_client.expire(f"processed:{user_id}", PROCESSED_SET_TTL)
        except Exception as e:
            print(f"Error processing streak for {user_id}: {e}")
            raise

    return {"statusCode": 200, "body": "OK"}


def process_streak_update(user_id: str, activity_date: str) -> None:
    """Core streak logic: compare dates and update the streak counter."""
    # Read current state
    response = user_streaks_table.get_item(Key={"user_id": user_id})
    user = response.get("Item")

    if not user:
        # First-ever activity for this user
        initialize_user(user_id, activity_date)
        return

    last_activity = user.get("last_activity_date")
    current_streak = int(user.get("current_streak", 0))
    longest_streak = int(user.get("longest_streak", 0))
    version = int(user.get("version", 0))
    timezone = user.get("timezone", "UTC")

    if last_activity == activity_date:
        # Already updated today — no-op
        return

    # Determine if the activity is consecutive
    yesterday = get_previous_date(activity_date, timezone)

    if last_activity == yesterday:
        new_streak = current_streak + 1
    else:
        new_streak = 1  # Streak broken — reset

    new_longest = max(new_streak, longest_streak)

    # Conditional write with optimistic locking
    try:
        user_streaks_table.update_item(
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
            ConditionExpression="version = :expected_v",
        )
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        # Another invocation beat us — retry will re-read fresh state
        print(f"Concurrent update for {user_id}, retrying on next invocation")
        raise

    # Update cache
    update_cache(user_id, new_streak, new_longest, user.get("total_bestie_points", 0))
    print(f"Updated streak for {user_id}: {current_streak} -> {new_streak}")


def initialize_user(user_id: str, activity_date: str) -> None:
    """Create the initial UserStreaks record for a new user."""
    user_streaks_table.put_item(
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
    update_cache(user_id, 1, 1, 0)


def get_previous_date(date_str: str, timezone: str) -> str:
    """Return the calendar date before the given date in the user's timezone."""
    local_date = datetime.strptime(date_str, "%Y-%m-%d")
    previous = local_date - timedelta(days=1)
    return previous.strftime("%Y-%m-%d")


def update_cache(user_id: str, streak: int, longest: int, bp: int) -> None:
    """Write-through cache update to Redis."""
    try:
        redis_client.setex(
            f"streak:{user_id}",
            300,  # 5-minute TTL
            json.dumps({
                "current_streak": streak,
                "longest_streak": longest,
                "total_bestie_points": bp,
            }),
        )
    except redis.RedisError as e:
        # Cache failure is non-fatal — DynamoDB is the source of truth
        print(f"Redis cache update failed for {user_id}: {e}")
