# Campus Sensor Monitoring and Alerts

FastAPI service for individual and batch campus readings, organizer-managed alert rules, rolling-window averages, and alert acknowledgement/resolution. It uses an asynchronous bounded ingestion queue and a single SQLite WAL writer that persists short batches. The accompanying simulator generates ordinary readings and occasional thermal excursions; the load driver targets 10,000 readings/second in batches.

## Run locally

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
$env:CAMPUS_DB_PATH = "data/campus.sqlite3"
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Open `http://127.0.0.1:8000/docs` for the interactive API. The service creates its SQLite database on first launch. Start with the local API only; add an authenticated gateway and TLS before exposing it outside a trusted machine/network.

## Example

Create a 10-minute average rule (the first alert is eligible after at least three samples):

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/rules -ContentType 'application/json' -Body '{"name":"Warm room","sensor_type":"temperature","mode":"rolling_average","operator":"gt","threshold":28,"window_seconds":600,"minimum_samples":3,"enabled":true}'
```

Send one reading:

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/readings -ContentType 'application/json' -Body '{"sensor_id":"room-204","sensor_type":"temperature","value":29.4,"unit":"C"}'
```

Send batches with `POST /readings/batch` and a JSON body `{"readings":[...]}` (maximum 1,000 records per request). The response is `202 Accepted`; readings are first queued in process memory and committed asynchronously. `GET /metrics` reports queue depth and persisted totals. Rules support `threshold` and `rolling_average`, comparison operators `gt`, `gte`, `lt`, `lte`, `eq`, optional sensor-specific scope, and enabled/disabled state. Alerts are deduplicated while a rule/sensor alert remains open or acknowledged. Resolve an alert with `POST /alerts/{id}/resolve`; subsequent violations can create a new alert.

Run the simulator:

```powershell
python -m app.simulate --seconds 30 --rate 1000 --batch-size 250 --sensors 40
```

Run the challenge-rate load driver while the API is running:

```powershell
python -m app.load_test --rate 10000 --seconds 10 --batch-size 500 --out reports/latest-load-test.json
```

Wait for `queue_depth` to return to zero before comparing `accepted_readings` with `persisted_readings`. The captured development run is in [reports/load-test-2026-10-14.json](reports/load-test-2026-10-14.json).

## API map

| Method | Route | Purpose |
|---|---|---|
| `POST` | `/readings` | Queue one validated reading |
| `POST` | `/readings/batch` | Queue up to 1,000 readings |
| `GET` | `/sensors` | List observed sensor/type pairs |
| `POST`, `GET` | `/rules` | Create/list threshold and time-window rules |
| `GET`, `PUT`, `PATCH`, `DELETE` | `/rules/{id}` | Read, replace, enable/disable, or delete a rule |
| `GET` | `/alerts` | Paginated alert history, optionally filtered by state |
| `POST` | `/alerts/{id}/acknowledge` | Acknowledge an open alert |
| `POST` | `/alerts/{id}/resolve` | Resolve an alert |
| `GET` | `/health`, `/metrics` | Health and ingestion counters |

## Architecture and reliability choices

- Producers validate data and enqueue without waiting for storage; the bounded queue absorbs short bursts. Full queues return retryable `503` instead of consuming unbounded memory.
- A single async worker groups writes into SQLite WAL transactions and evaluates rules in event time. The rolling average keeps recent samples per rule and sensor in memory.
- SQLite WAL makes the service simple to run and inspect on one machine. One API process owns the rule/window caches and writer; multi-process deployment would split state and is unsupported.
- The `202` response confirms process-memory acceptance, not durable commit. A process or machine crash can lose queued readings and recent window state. For production, put a replicated stream (for example Kafka/Redpanda) ahead of workers, persist raw events before acknowledgement, distribute rule state, authenticate organizers, add retention/backup, and protect transport with TLS.
- Duplicate `reading_id` values are ignored by SQLite. Alerts are episode-deduplicated while open/acknowledged. Resolved alerts remain in history.

## Scope

## Development load run

On the development machine (Windows, Python 3.12, single Uvicorn process, local SQLite WAL), a 10-second run targeting 10,000 readings/second submitted 117,500 readings in 500-reading batches. All 117,500 were accepted, with zero HTTP errors/rejections; measured accepted throughput was 11,681.81 readings/second. Batch request latency was 43.911 ms p50, 63.346 ms p95 and 117.720 ms p99. The queue peaked at 64,578 in-flight readings in the first end-of-load snapshot; after draining, server counters confirmed 117,501 accepted and persisted (including one earlier alert smoke reading), queue depth zero and no rejections.

The backend also exercised rule creation, one-reading threshold ingestion, alert creation, acknowledgement, resolution and rule deletion against the live service. This run measures the stated local batch workload, not internet-scale latency. The asynchronous `202` contract and single-process rule cache are the central trade-off; see the reliability section before deployment.
