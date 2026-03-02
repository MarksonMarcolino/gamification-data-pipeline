# Streaks & Bestie Points — Data Pipeline Design

## Overview

This document presents a **production-grade architecture** for Luzia's **Streaks & Bestie Points (BP)** gamification system. The system rewards millions of users worldwide for consistent daily engagement with the Luzia personal assistant app.

| Requirement | Solution |
|---|---|
| **Streak tracking** | Per-user daily activity detection with timezone-aware reset logic |
| **Bestie Points** | Event-driven point accrual from conversations and tool usage |
| **Scale** | Millions of DAU, global distribution, sub-100ms reads |
| **Real-time** | Instant streak/BP updates reflected in the mobile app |
| **Analytics** | Historical data warehouse for product and business intelligence |

---

## Tech Stack

```mermaid
graph TD
    App["📱 Mobile App"] -->|REST API| APIGW["API Gateway"]

    subgraph real["⚡ Real-Time Path"]
        APIGW --> Ingest["Lambda\nEvent Ingestion"]
        Ingest --> DDB[("DynamoDB\n(UserStreaks · BPEvents · DailyActivity)")]
        DDB --> Streams["DynamoDB Streams"]
        Streams --> Processor["Lambda\nStreak Processor"]
        Processor --> DDB
        Processor --> Cache["ElastiCache\nRedis"]
        Ingest -.->|read cache| Cache
    end

    subgraph batch["📊 Batch / Analytics Path"]
        Streams --> Firehose["Kinesis\nData Firehose"]
        Firehose --> S3Raw[("S3\nRaw Events")]
        S3Raw --> Glue["AWS Glue\nPySpark ETL"]
        Glue --> S3Cur[("S3\nCurated Layer")]
        S3Cur --> Redshift[("Amazon\nRedshift")]
    end

    style real fill:#1a1a2e,stroke:#6c63ff,color:#fff
    style batch fill:#1a1a2e,stroke:#f59e0b,color:#fff
    style App fill:#6c63ff,stroke:#6c63ff,color:#fff
    style APIGW fill:#4338ca,stroke:#4338ca,color:#fff
    style Ingest fill:#7c3aed,stroke:#7c3aed,color:#fff
    style DDB fill:#2563eb,stroke:#2563eb,color:#fff
    style Streams fill:#0891b2,stroke:#0891b2,color:#fff
    style Processor fill:#7c3aed,stroke:#7c3aed,color:#fff
    style Cache fill:#dc2626,stroke:#dc2626,color:#fff
    style Firehose fill:#ea580c,stroke:#ea580c,color:#fff
    style S3Raw fill:#d97706,stroke:#d97706,color:#fff
    style Glue fill:#ca8a04,stroke:#ca8a04,color:#fff
    style S3Cur fill:#65a30d,stroke:#65a30d,color:#fff
    style Redshift fill:#059669,stroke:#059669,color:#fff
```

---

## Design Document Sections

### [1. Data Modeling & System Design](data-modeling.md)
Database design, table schemas, DynamoDB access patterns, caching with ElastiCache, and performance optimization for peak-load scenarios.

### [2. ETL & Data Pipeline Design](etl-pipeline.md)
End-to-end pipeline from mobile events to the analytical data warehouse — real-time and batch paths, timezone handling, and data transformation.

### [3. Data Integrity & Anomaly Detection](data-integrity.md)
Idempotency, deduplication, consistency validation, anomaly detection, and security controls to keep streak data trustworthy.

---

## Code Samples

Working implementations are in the [`code-samples/`](https://github.com/MarksonMarcolino/gamification-data-pipeline/tree/main/code-samples) directory:

| File | Purpose |
|---|---|
| `dynamodb/table_definitions.py` | Boto3 DynamoDB table creation with GSIs |
| `lambda/streak_processor.py` | Streak update logic with timezone handling |
| `lambda/event_ingestion.py` | API event validation and ingestion |
| `glue/etl_job.py` | PySpark dedup & transform job |
| `sql/redshift_schema.sql` | Analytical tables DDL |
