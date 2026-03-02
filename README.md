# Gamification Data Pipeline — Streaks & Bestie Points

Design document and code samples for Luzia's Streaks & Bestie Points gamification system.

**[View the full design document](https://marksonmarcolino.github.io/gamification-data-pipeline/)**

## Sections

1. **Data Modeling & System Design** — DynamoDB + Redshift hybrid, caching, indexing
2. **ETL & Data Pipeline Design** — Real-time and batch paths, timezone handling
3. **Data Integrity & Anomaly Detection** — Dedup, reconciliation, security

## Code Samples

| File | Description |
|---|---|
| `code-samples/dynamodb/table_definitions.py` | Boto3 DynamoDB table creation with GSIs |
| `code-samples/lambda/streak_processor.py` | Streak update logic with timezone handling |
| `code-samples/lambda/event_ingestion.py` | API event validation and ingestion |
| `code-samples/glue/etl_job.py` | PySpark dedup & transform job |
| `code-samples/sql/redshift_schema.sql` | Redshift analytical tables DDL |

## Local Development

```bash
pip install mkdocs-material
mkdocs serve
```

Then visit `http://localhost:8000`.
