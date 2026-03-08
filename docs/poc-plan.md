# POC: Gamification Data Pipeline — Local Simulation Notebook

## Context

You have a complete design doc + code samples for the Luzia Streaks & Bestie Points system (DynamoDB, Lambda, Glue, Redshift, Redis). The goal is to create a **single Jupyter notebook** that simulates the entire architecture locally with **zero infrastructure** — no Docker, no AWS account needed. This serves as a live demo during the interview, proving the design actually works.

**Requirements:** Python **3.11+** (for `datetime.UTC` and `ZoneInfo` stdlib support).

**Local mock strategy:**

| AWS Service | Local Replacement | Library |
|---|---|---|
| DynamoDB | In-memory mock | `moto` |
| Lambda | Direct function calls | Existing code, adapted |
| Redis/ElastiCache | In-memory mock | `fakeredis` |
| S3 + Glue (PySpark) | pandas DataFrames | `pandas` |
| Redshift | SQLite in-memory | `sqlite3` (stdlib) |
| DynamoDB Streams | Manual trigger after writes | Custom simulation |

---

## Files to Create

```
gamification-data-pipeline/
  notebook/
    requirements.txt                    # Python dependencies
    gamification_pipeline_poc.ipynb     # The single Jupyter notebook
```

### `requirements.txt`
```
jupyter>=1.0.0
moto[dynamodb,s3]>=5.0.0
boto3>=1.35.0
fakeredis>=2.21.0
pandas>=2.2.0
matplotlib>=3.9.0
tabulate>=0.9.0
```

---

## Notebook Structure (11 Sections)

### Section 1: Setup & Architecture Overview

**Markdown cell:**
- Architecture diagram (Mermaid or ASCII) showing both real-time and batch paths
- Mock-mapping table (above)
- Python 3.11+ requirement note

**Code cell — Environment & imports:**
```python
import os, json, sqlite3
from datetime import datetime, timedelta, UTC
from decimal import Decimal
from uuid import uuid4
from zoneinfo import ZoneInfo

import boto3, pandas as pd, matplotlib.pyplot as plt, fakeredis
from moto import mock_aws
from botocore.exceptions import ClientError

# Dummy AWS credentials — moto requires these even though nothing leaves the process
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

# Start moto mock — use .start() (not context manager) so state persists across cells
mock = mock_aws()
mock.start()

# Shared fakeredis instance — used by both event ingestion and streak processor
redis_client = fakeredis.FakeRedis(decode_responses=True)
```

**Code cell — Shared utilities:**
```python
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
    return pd.DataFrame([decimal_to_native(item) for item in items])
```

**Code cell — Shared resources dict:**
```python
# All notebook sections reference this dict instead of module-level globals
resources = {
    "dynamodb": boto3.resource("dynamodb", region_name="us-east-1"),
    "redis": redis_client,
    "tables": {},   # populated in Section 2
}
```

### Section 2: DynamoDB Table Creation

- Reuse logic from `code-samples/dynamodb/table_definitions.py`
- **Bug Fix #2**: Add `StreamSpecification` to DailyActivity table (already applied in source)
- Verification: `describe_table()` for each table, display schema as DataFrame

**Verification cell:**
```python
for name in ["UserStreaks", "BPEvents", "DailyActivity"]:
    desc = resources["dynamodb"].meta.client.describe_table(TableName=name)["Table"]
    print(f"\n{name}: {desc['TableStatus']}, Keys: {desc['KeySchema']}")
    if desc.get("GlobalSecondaryIndexes"):
        for gsi in desc["GlobalSecondaryIndexes"]:
            print(f"  GSI: {gsi['IndexName']} -> {gsi['KeySchema']}")
    if desc.get("StreamSpecification"):
        print(f"  Stream: {desc['StreamSpecification']}")
```

### Section 3: Sample Data Generation

**Timestamp strategy:** All event timestamps are generated **relative to `datetime.now(UTC)`** at notebook runtime. This avoids the `validate_event()` rejection window (>7 days in the past, >24h in the future). The base date `T` is set to `today`, and all user scenarios are computed as offsets from `T`.

```python
T = datetime.now(UTC).replace(hour=12, minute=0, second=0, microsecond=0)
```

6 users, each testing a different scenario:

| User | Timezone | Scenario | Date range (relative to T) |
|---|---|---|---|
| `usr_alice` | `America/Sao_Paulo` | 4-day streak, 1-day gap, 2-day restart | T-6,T-5,T-4,T-3 (skip T-2), T-1,T-0 |
| `usr_bob` | `Asia/Tokyo` | Near-midnight activity (23:58 / 00:02 local) | T-3 to T |
| `usr_carol` | `Europe/London` | Events around DST boundary (March 29 2026 for UK) | T-2 to T |
| `usr_dave` | `America/New_York` | Sends duplicate events (same idempotency key repeated) | T-2 to T |
| `usr_eve` | `Pacific/Auckland` | Extreme UTC+13 offset, date boundary crossing | T-3 to T |
| `usr_frank` | `America/Sao_Paulo` | 105 events in 1 hour (rate-limit test) + bad payloads | T-1 to T |

**Data generation rules:**
- ~100–150 total events in the exact API payload format from the docs
- Vary `source` field across `mobile_app`, `web`, `whatsapp` for richer ETL/analytics
- Vary `event_type` across all 4 types (`CONVERSATION_START`, `TOOL_USE`, `DAILY_OPEN`, `SHARE_RESULT`)
- Dave's duplicates: reuse the **same** `idempotency_key` for 5+ events to test GSI dedup
- Frank's burst: 105 events with timestamps within the same hour; 5 events with `event_type: "INVALID_TYPE"` for validation errors
- Carol's DST: if the notebook runs in March near the UK DST transition, use the actual date; otherwise, simulate by having events on two consecutive days where `get_previous_date` must handle the timezone correctly
- Use a **seeded `random.Random(42)`** for reproducible UUIDs in idempotency keys (instead of `uuid4()` for test data generation only — production code still uses `uuid4`)

**Output:** Display the generated events DataFrame with columns: `user_id`, `event_type`, `client_timestamp`, `timezone`, `idempotency_key`, `source`.

### Section 4: Real-Time Path — Event Ingestion

**Adapt `event_ingestion.py` for the notebook:**
- Replace module-level `dynamodb`, `redis_client`, and table references with `resources` dict
- Functions accept dependencies as parameters (dependency injection):
  ```python
  def process_event(event, bp_events_table, daily_activity_table,
                    user_streaks_table, redis_conn):
  ```

**Bug Fix #1**: GSI lookup on `IdempotencyIndex` before write (already applied in source).

**Bug Fix #4**: `check_rate_limit()` and `detect_timezone_abuse()` integrated (already applied in source).

**Bug Fix #4a** (new): Update `process_event()` to **maintain `recent_tz_changes`** on UserStreaks when the timezone changes. Without this, `detect_timezone_abuse()` always reads an empty list and never triggers. Add after the UserStreaks update:
```python
# Track timezone changes for abuse detection
if old_tz and old_tz != timezone:
    user_streaks_table.update_item(
        Key={"user_id": user_id},
        UpdateExpression="SET recent_tz_changes = list_append(if_not_exists(recent_tz_changes, :empty), :change)",
        ExpressionAttributeValues={
            ":empty": [],
            ":change": [{"timezone": timezone, "timestamp": utc_ts.isoformat()}],
        },
    )
```

**Processing loop with progress tracking:**
```python
results = {"processed": 0, "duplicate": 0, "rate_limited": 0, "validation_error": 0, "error": 0}

for i, event in enumerate(events):
    try:
        result = process_event(event, ...)
        results["processed"] += 1
    except DuplicateEventError:
        results["duplicate"] += 1
    except RateLimitError:
        results["rate_limited"] += 1
    except ValidationError:
        results["validation_error"] += 1
    except Exception as e:
        results["error"] += 1

print(f"Results: {results}")
```

**Display cells:**
- Ingestion results summary table
- BPEvents table as DataFrame (via `scan_table_to_df`)
- DailyActivity table as DataFrame
- UserStreaks table as DataFrame (note: streaks not yet computed — only BP and timezone set)

**Programmatic assertions:**
```python
# Dave's duplicates caught
dave_events = bp_events_df[bp_events_df["user_id"] == "usr_dave"]
assert len(dave_events) < total_dave_events_sent, "Dedup should have filtered Dave's duplicates"

# Frank's rate limiting
assert results["rate_limited"] > 0, "Frank's burst should trigger rate limiting"

# Frank's bad payloads
assert results["validation_error"] >= 5, "Frank's invalid event_types should be rejected"
```

### Section 5: Real-Time Path — Streak Processor

**Adapt `streak_processor.py` for the notebook:**
- Replace module-level globals with `resources` dict parameters
- Functions accept `user_streaks_table` and `redis_conn` as parameters

**Bug Fix #3**: `get_previous_date` uses timezone-aware arithmetic (already applied in source).

**Stream simulation strategy:**
moto doesn't emit real DynamoDB Stream events. Simulate by:
1. Scan `DailyActivity` table
2. Sort records by `(user_id, activity_date)` — this ensures chronological processing per user
3. For each record, call `process_streak_update(user_id, activity_date, ...)`

**Critical: Handle the `initialize_user` race condition:**
Event ingestion (Section 4) already created UserStreaks records via `update_item` (with `ADD total_bestie_points` and `SET timezone`). These records exist but have NO streak fields (`current_streak`, `longest_streak`, `last_activity_date`, `version`). The streak processor handles this correctly:
- `get_item` returns the existing item (so `initialize_user` is NOT called)
- `user.get("current_streak", 0)` → 0 (field missing, defaults to 0)
- `user.get("version", 0)` → 0
- The update adds the streak fields without overwriting `total_bestie_points` (since it uses `SET` for streak fields, not `PUT`)

The `initialize_user` path is only hit if a DailyActivity record exists for a user who has NO UserStreaks record at all — which can't happen in our simulation since event ingestion runs first.

**Display cells:**
- UserStreaks table as DataFrame (now with streak data populated)
- Redis cache state: iterate `streak:*` keys and display as DataFrame
- Per-user streak summary: user_id, current_streak, longest_streak, last_activity_date

**Programmatic assertions:**
```python
alice = streaks_df[streaks_df["user_id"] == "usr_alice"].iloc[0]
assert alice["longest_streak"] == 4, f"Alice longest should be 4, got {alice['longest_streak']}"
assert alice["current_streak"] == 2, f"Alice current should be 2, got {alice['current_streak']}"

bob = streaks_df[streaks_df["user_id"] == "usr_bob"].iloc[0]
assert bob["current_streak"] >= 2, "Bob's near-midnight events should count as consecutive days"
```

### Section 6: Cache Layer Verification

**Read path demonstration:**
```python
def get_user_streak(user_id, user_streaks_table, redis_conn):
    """Read path: Redis first, fallback to DynamoDB."""
    cached = redis_conn.get(f"streak:{user_id}")
    if cached:
        return json.loads(cached), "cache_hit"
    # Cache miss — read from DynamoDB
    response = user_streaks_table.get_item(Key={"user_id": user_id})
    item = response.get("Item", {})
    return decimal_to_native(item), "cache_miss"
```

**Verification cells:**
- For each user, call `get_user_streak()` and show hit/miss status
- Compare DynamoDB vs Redis state side-by-side — should match for streak fields
- Display Redis rate-limit keys (`rate:*`) with current counts
- Display Redis processed-event sets (`processed:*`)
- **Force a cache miss** by deleting a key, then re-read to show the fallback path

**Programmatic assertions:**
```python
for user_id in user_ids:
    dynamo_data = get_from_dynamodb(user_id)
    redis_data = get_from_redis(user_id)
    assert dynamo_data["current_streak"] == redis_data["current_streak"], \
        f"Cache mismatch for {user_id}"
```

### Section 7: Batch ETL (pandas replacing PySpark/Glue)

**Data extraction:** Scan BPEvents from DynamoDB (moto) into a pandas DataFrame using `scan_table_to_df()`.

**Translate `code-samples/glue/etl_job.py` logic to pandas:**

| PySpark Operation | pandas Equivalent |
|---|---|
| `Window.partitionBy("idempotency_key").orderBy(...)` | `df.sort_values("created_at").drop_duplicates("idempotency_key", keep="first")` |
| `F.col("points_awarded").cast("integer")` | `df["points_awarded"].astype(int)` |
| `F.when(condition, value).otherwise(default)` | `np.where(condition, value, default)` |
| `df.groupBy(...).agg(...)` | `df.groupby(...).agg(...)` |
| `spark.read.parquet(USER_DIM_PATH)` | Hardcoded `pd.DataFrame` with user dimensions |

**Steps:**
1. **Dedup** by `idempotency_key` via `drop_duplicates(subset="idempotency_key", keep="first")`
2. **Schema validation** — cast types, flag anomalous records (`points_awarded < 0` or `> 100`)
3. **Enrich** with user dimension data (hardcoded DataFrame):
   ```python
   user_dim = pd.DataFrame([
       {"user_id": "usr_alice", "country": "BR", "signup_date": "2025-01-15", "user_segment": "power_user"},
       {"user_id": "usr_bob",   "country": "JP", "signup_date": "2025-02-20", "user_segment": "casual"},
       ...
   ])
   enriched_df = clean_df.merge(user_dim, on="user_id", how="left")
   ```
4. **Compute daily aggregates** via `groupby(["user_id", "event_date"]).agg(...)`

**Display cells:**
- Dedup funnel: raw count → deduped count → anomaly-free count → enriched count
- Anomalies table (if any)
- Daily summaries DataFrame
- Save `enriched_df` and `daily_summaries_df` for Section 8

**Programmatic assertions:**
```python
assert len(deduped_df) < len(raw_df), "Dedup should remove some records"
assert len(enriched_df) == len(clean_df), "Enrichment should not change row count"
```

### Section 8: Analytical Warehouse (SQLite replacing Redshift)

**SQLite adaptations from Redshift DDL:**

| Redshift Feature | SQLite Equivalent |
|---|---|
| `DISTKEY`, `SORTKEY` | Removed (SQLite has no distribution) |
| `TIMESTAMP WITH TIME ZONE` | `TEXT` (store ISO 8601 strings) |
| `GETDATE()` | `datetime('now')` |
| `DATE_TRUNC('week', event_date)` | `date(event_date, 'weekday 0', '-6 days')` |
| `CREATE TABLE ... (LIKE ...)` | `CREATE TABLE ... AS SELECT * FROM ... WHERE 0` |
| `VARCHAR(N)` | `TEXT` |

**Steps:**
1. Create SQLite in-memory database: `conn = sqlite3.connect(":memory:")`
2. Create tables: `fact_bp_events`, `fact_daily_summaries`, `dim_user_streaks`, `stg_bp_events`
3. Load curated DataFrames from Section 7: `enriched_df.to_sql("fact_bp_events", conn, ...)`
4. Load UserStreaks snapshot from DynamoDB scan: `streaks_df.to_sql("dim_user_streaks", conn, ...)`
5. Run analytical queries (adapted from the 3 Redshift views):
   - **Weekly stats**: active users, avg points, avg events by week and country
   - **Streak distribution**: user counts bucketed by streak length (0, 1-3, 4-7, 8-14, 15-30, 30+)
   - **Event type performance**: event counts, total points, unique users by event type and date
6. **Demo staging + merge pattern:**
   ```python
   # Simulate Redshift COPY → staging → merge
   new_batch_df.to_sql("stg_bp_events", conn, if_exists="replace", index=False)
   conn.execute("""
       DELETE FROM fact_bp_events WHERE idempotency_key IN
       (SELECT idempotency_key FROM stg_bp_events)
   """)
   conn.execute("INSERT INTO fact_bp_events SELECT * FROM stg_bp_events")
   conn.execute("DELETE FROM stg_bp_events")
   conn.commit()
   ```

**Display cells:**
- Each analytical query result as a DataFrame
- Staging merge before/after row counts

### Section 9: Visualizations

4 charts with matplotlib (`%matplotlib inline` in first cell):

1. **Streak Timeline** — line chart per user (x: date, y: streak count). Shows Alice's 4→0→2 pattern clearly.
2. **BP Accumulation** — cumulative points per user over time. Shows Frank's plateau when rate-limited.
3. **Streak Distribution** — bar chart from the analytical view (streak buckets on x-axis, user counts on y-axis).
4. **Dedup Funnel** — horizontal bar chart: raw events → deduped → anomaly-free → warehouse loaded. Shows data quality pipeline stages.

**Chart formatting:**
- Use `plt.style.use("seaborn-v0_8-whitegrid")` for clean appearance
- Include titles, axis labels, and legends
- Use `fig, axes = plt.subplots(2, 2, figsize=(14, 10))` for a 2x2 grid
- `plt.tight_layout()` to prevent label overlap

### Section 10: Bug Fix Summary & Discussion Points

**Markdown cell — Bug fixes table (6 issues identified, 4 code-level fixes):**

| # | File | Problem | Fix | Severity |
|---|---|---|---|---|
| 1 | `event_ingestion.py` | `attribute_not_exists` on new item always passes — dedup broken | Query `IdempotencyIndex` GSI before write (Layer 2) | Critical |
| 2 | `table_definitions.py` | No `StreamSpecification` on DailyActivity | Add `StreamEnabled: True, StreamViewType: NEW_IMAGE` | High |
| 3 | `streak_processor.py` | `get_previous_date` ignores `timezone` param — DST-unsafe | Use timezone-aware datetime with noon anchor | Medium |
| 4 | `event_ingestion.py` | Rate limiting / anomaly detection not integrated | Add `check_rate_limit()` + `detect_timezone_abuse()` calls + maintain `recent_tz_changes` | Medium |
| 5 | `data-modeling.md` | Architecture diagram shows `DAX / ElastiCache` but code uses Redis only | Changed to `ElastiCache Redis` — Redis supports computed aggregates, rate limiting, processed-event sets that DAX cannot | Low |
| 6 | `event_ingestion.py` | 3 sequential writes (BPEvents → DailyActivity → UserStreaks) are not atomic | Documented trade-off; nightly reconciliation job detects/repairs inconsistencies | Design |

**Markdown cell — Interview talking points:**
- **DynamoDB vs Aurora**: HTTP-based (no connection pooling), auto-scaling, conditional writes for optimistic locking, Streams for CDC. Aurora would work at moderate scale but requires connection management and read replicas.
- **Write-through cache**: streak data must be immediately consistent — user should see updated streak instantly. Write-behind would introduce a staleness window.
- **Optimistic locking**: `version` attribute prevents concurrent Lambda invocations from overwriting each other. Retry on `ConditionalCheckFailedException` re-reads fresh state.
- **Timezone as first-class concern**: all dates derived server-side from `client_timestamp` + `timezone`. Never trust client date calculation. `activity_date` is always in the user's local timezone.
- **Multi-layer dedup**: Layer 1 (client key) → Layer 2 (GSI lookup) → Layer 3 (conditional write). Each layer catches different failure modes.
- **Atomicity trade-off**: DynamoDB doesn't support efficient cross-table transactions at scale. Accept eventual consistency with nightly reconciliation rather than pay the latency/cost of `TransactWriteItems`.

### Section 11: Cleanup & Teardown

```python
# Stop moto mock — release all in-memory DynamoDB state
mock.stop()

# Clear any lingering environment variables
for key in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION"]:
    os.environ.pop(key, None)

print("All mocks stopped. Notebook complete.")
```

---

## Key Adaptations from Production Code

1. **moto context**: Use `mock.start()` at notebook top, `mock.stop()` in Section 11 (not decorator/context-manager, so state persists across cells)
2. **Decimal handling**: DynamoDB returns `Decimal` — `decimal_to_native()` utility defined in Section 1, used in Sections 4, 5, 6, and 7 wherever DynamoDB scan results are converted to DataFrames
3. **Stream simulation**: moto doesn't emit real stream events — Section 5 scans DailyActivity sorted by `(user_id, activity_date)` and calls `process_streak_update()` for each record chronologically
4. **Dependency injection**: Instead of module-level globals, all functions accept tables/redis as parameters via the `resources` dict. This makes the notebook testable and avoids import-time side effects.
5. **Timestamp generation**: All test event timestamps are computed relative to `datetime.now(UTC)` to stay within the `validate_event()` acceptance window (≤7 days in the past, ≤24h in the future)
6. **`recent_tz_changes` maintenance**: The notebook's adapted `process_event()` appends to `recent_tz_changes` on UserStreaks when a timezone change is detected, enabling `detect_timezone_abuse()` to actually trigger

---

## Bug Fixes Included

| # | File | Problem | Fix |
|---|---|---|---|
| 1 | `event_ingestion.py` | `attribute_not_exists` on new item always passes — dedup broken | Query `IdempotencyIndex` GSI before write |
| 2 | `table_definitions.py` | No `StreamSpecification` on tables | Add `StreamEnabled: True, StreamViewType: NEW_IMAGE` |
| 3 | `streak_processor.py` | `get_previous_date` ignores `timezone` param | Use timezone-aware datetime arithmetic with noon anchor |
| 4 | `event_ingestion.py` | Rate limiting / anomaly detection not integrated | Add `check_rate_limit()` + `detect_timezone_abuse()` calls |
| 5 | `data-modeling.md` | Diagram shows `DAX / ElastiCache` — code uses Redis | Updated to `ElastiCache Redis` consistently |
| 6 | `event_ingestion.py` | 3 sequential writes are not atomic | Documented trade-off + nightly reconciliation reference |

---

## Verification

### Setup
1. Ensure Python **3.11+**: `python --version`
2. `pip install -r notebook/requirements.txt`
3. `jupyter notebook notebook/gamification_pipeline_poc.ipynb`
4. **Kernel → Restart & Run All** — should complete with zero errors

### Automated Assertions (built into notebook cells)
All of these are `assert` statements that fail loudly if the condition is not met:

| Check | Section | Assertion |
|---|---|---|
| Dave's duplicates caught | 4 | Unique events for Dave < total events sent |
| Frank's rate limiting works | 4 | `results["rate_limited"] > 0` |
| Frank's bad payloads rejected | 4 | `results["validation_error"] >= 5` |
| Alice's streak: 4 → break → 2 | 5 | `longest_streak == 4`, `current_streak == 2` |
| Bob's near-midnight events consecutive | 5 | `current_streak >= 2` |
| Carol's DST-boundary dates correct | 5 | Streak increments across DST change |
| Eve's UTC+13 offset handled | 5 | Activity dates are in NZ local time, streak is correct |
| Cache matches DynamoDB | 6 | `dynamo_streak == redis_streak` for all users |
| Dedup removes records in ETL | 7 | `len(deduped) < len(raw)` |
| Enrichment preserves row count | 7 | `len(enriched) == len(clean)` |
| BP reconciliation | 6 | `sum(BPEvents.points)` per user == `UserStreaks.total_bestie_points` |
| SQLite views return data | 8 | `len(weekly_stats) > 0`, `len(streak_dist) > 0`, `len(event_perf) > 0` |
| All 4 charts render | 9 | No exceptions during `plt.show()` |
