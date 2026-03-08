"""
DynamoDB Table Definitions for the Streaks & Bestie Points System.

Creates three tables:
  - UserStreaks: hot table for current streak state and BP balance
  - BPEvents: append-only event log for point-earning interactions
  - DailyActivity: one record per user per day for streak calculation

Usage:
    python table_definitions.py [--endpoint-url http://localhost:8000]
"""

import argparse
import sys

import boto3
from botocore.exceptions import ClientError


def get_dynamodb_resource(endpoint_url: str | None = None):
    kwargs = {}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    return boto3.resource("dynamodb", **kwargs)


def create_user_streaks_table(dynamodb) -> None:
    """Create the UserStreaks table with a leaderboard GSI."""
    table_name = "UserStreaks"

    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "user_id", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "user_id", "AttributeType": "S"},
                {"AttributeName": "leaderboard_partition", "AttributeType": "S"},
                {"AttributeName": "current_streak", "AttributeType": "N"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "StreakLeaderboard",
                    "KeySchema": [
                        {"AttributeName": "leaderboard_partition", "KeyType": "HASH"},
                        {"AttributeName": "current_streak", "KeyType": "RANGE"},
                    ],
                    "Projection": {
                        "ProjectionType": "INCLUDE",
                        "NonKeyAttributes": [
                            "user_id",
                            "longest_streak",
                            "total_bestie_points",
                        ],
                    },
                },
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
        print(f"Created table: {table_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceInUseException":
            print(f"Table {table_name} already exists, skipping.")
        else:
            raise


def create_bp_events_table(dynamodb) -> None:
    """Create the BPEvents table with an idempotency GSI and TTL."""
    table_name = "BPEvents"

    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "user_id", "KeyType": "HASH"},
                {"AttributeName": "event_id", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "user_id", "AttributeType": "S"},
                {"AttributeName": "event_id", "AttributeType": "S"},
                {"AttributeName": "idempotency_key", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "IdempotencyIndex",
                    "KeySchema": [
                        {"AttributeName": "idempotency_key", "KeyType": "HASH"},
                    ],
                    "Projection": {"ProjectionType": "KEYS_ONLY"},
                },
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
        print(f"Created table: {table_name}")

        # Enable TTL for automatic event expiration (90 days)
        client = dynamodb.meta.client
        client.update_time_to_live(
            TableName=table_name,
            TimeToLiveSpecification={
                "Enabled": True,
                "AttributeName": "ttl",
            },
        )
        print(f"Enabled TTL on {table_name}.ttl")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceInUseException":
            print(f"Table {table_name} already exists, skipping.")
        else:
            raise


def create_daily_activity_table(dynamodb) -> None:
    """Create the DailyActivity table (user_id PK, activity_date SK)."""
    table_name = "DailyActivity"

    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "user_id", "KeyType": "HASH"},
                {"AttributeName": "activity_date", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "user_id", "AttributeType": "S"},
                {"AttributeName": "activity_date", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_IMAGE",
            },
        )
        table.wait_until_exists()
        print(f"Created table: {table_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceInUseException":
            print(f"Table {table_name} already exists, skipping.")
        else:
            raise


def main():
    parser = argparse.ArgumentParser(description="Create DynamoDB tables")
    parser.add_argument("--endpoint-url", help="DynamoDB endpoint (for local dev)")
    args = parser.parse_args()

    dynamodb = get_dynamodb_resource(args.endpoint_url)

    print("Creating DynamoDB tables for Streaks & Bestie Points...\n")
    create_user_streaks_table(dynamodb)
    create_bp_events_table(dynamodb)
    create_daily_activity_table(dynamodb)
    print("\nAll tables created successfully.")


if __name__ == "__main__":
    main()
