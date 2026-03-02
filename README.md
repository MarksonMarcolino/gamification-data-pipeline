# Gamification Data Pipeline — Streaks & Bestie Points

[![Deploy to GitHub Pages](https://github.com/MarksonMarcolino/gamification-data-pipeline/actions/workflows/deploy.yml/badge.svg)](https://github.com/MarksonMarcolino/gamification-data-pipeline/actions/workflows/deploy.yml)
[![GitHub Pages](https://img.shields.io/badge/docs-live-blue?logo=github)](https://marksonmarcolino.github.io/gamification-data-pipeline/)
[![AWS](https://img.shields.io/badge/cloud-AWS-FF9900?logo=amazon-web-services)](https://aws.amazon.com/)
[![Python](https://img.shields.io/badge/python-3.12-3776AB?logo=python&logoColor=white)](https://python.org)
[![MkDocs Material](https://img.shields.io/badge/docs-MkDocs%20Material-526CFE?logo=materialformkdocs&logoColor=white)](https://squidfunk.github.io/mkdocs-material/)

> **[View the full design document](https://marksonmarcolino.github.io/gamification-data-pipeline/)**

---

## Context

Luzia is the world-leading personal assistant, chatting with millions of people daily across the globe. The **Streaks & Bestie Points (BP)** system is a gamification feature designed to reward users for consistent daily engagement:

- **Streaks** track consecutive days of app usage — miss a day and the streak resets to zero
- **Bestie Points** are earned through interactions (conversations, tool usage) and can be accumulated for future rewards

This repository contains the **system design document** and **working code samples** for the data engineering infrastructure behind this feature, covering real-time processing, batch analytics, and data integrity at scale.

## Architecture Overview

The system uses an **event-driven, serverless architecture** on AWS with two data paths:

| Path | Purpose | Services |
|---|---|---|
| **Real-Time** | Instant streak/BP updates in the app | API Gateway, Lambda, DynamoDB, ElastiCache |
| **Batch** | Historical analytics and BI | DynamoDB Streams, Kinesis Firehose, S3, Glue, Redshift |

## Design Document Sections

| Section | Topic | Key Decisions |
|---|---|---|
| [1. Data Modeling & System Design](https://marksonmarcolino.github.io/gamification-data-pipeline/data-modeling/) | Database design, schemas, performance | DynamoDB + Redshift hybrid, ElastiCache, GSIs |
| [2. ETL & Data Pipeline Design](https://marksonmarcolino.github.io/gamification-data-pipeline/etl-pipeline/) | End-to-end pipeline, timezone handling | Real-time + batch paths, Step Functions orchestration |
| [3. Data Integrity & Anomaly Detection](https://marksonmarcolino.github.io/gamification-data-pipeline/data-integrity/) | Dedup, reconciliation, security | Idempotency keys, conditional writes, rate limiting |

## Code Samples

```
code-samples/
├── dynamodb/
│   └── table_definitions.py      # Boto3 DynamoDB table creation with GSIs & TTL
├── lambda/
│   ├── streak_processor.py       # Stream-triggered streak logic with optimistic locking
│   └── event_ingestion.py        # API event validation, dedup & point accrual
├── glue/
│   └── etl_job.py                # PySpark dedup, enrichment & daily aggregation
└── sql/
    └── redshift_schema.sql       # Fact/dim tables, analytical views, COPY template
```

## Tech Stack

| Category | Service | Role |
|---|---|---|
| **Compute** | AWS Lambda | Event ingestion, streak processing |
| **Database** | Amazon DynamoDB | Operational store (sub-10ms latency) |
| **Cache** | Amazon ElastiCache (Redis) | Read-through cache for streak lookups |
| **Streaming** | DynamoDB Streams, Kinesis Firehose | CDC and buffered delivery to S3 |
| **Storage** | Amazon S3 | Data lake (raw + curated layers) |
| **ETL** | AWS Glue (PySpark) | Deduplication, transformation, aggregation |
| **Warehouse** | Amazon Redshift | Analytical queries, BI dashboards |
| **Orchestration** | AWS Step Functions | Batch pipeline scheduling and monitoring |
| **API** | Amazon API Gateway | REST endpoint with auth and throttling |

## Local Development

```bash
pip install mkdocs-material
mkdocs serve
```

Then visit [http://localhost:8000](http://localhost:8000).
