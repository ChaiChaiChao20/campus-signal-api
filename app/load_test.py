from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import httpx


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[min(len(ordered)-1, int((len(ordered)-1)*pct))], 3)


async def run(url: str, rate: int, duration: int, batch_size: int, sensors: int) -> dict:
    request_count = 0
    submitted = accepted = rejected = 0
    latencies: list[float] = []
    errors: list[str] = []
    max_in_flight = 32
    semaphore = asyncio.Semaphore(max_in_flight)
    start = time.perf_counter()
    stop_at = start + duration
    sequence = 0
    tasks: set[asyncio.Task] = set()
    async with httpx.AsyncClient(base_url=url, timeout=30, limits=httpx.Limits(max_connections=max_in_flight)) as client:
        async def send(rows: list[dict]) -> None:
            nonlocal request_count, accepted, rejected
            async with semaphore:
                request_count += 1
                before = time.perf_counter()
                try:
                    response = await client.post("/readings/batch", json={"readings": rows})
                    latencies.append((time.perf_counter()-before)*1000)
                    if response.status_code == 202:
                        accepted += int(response.json().get("accepted", 0))
                    else:
                        rejected += len(rows)
                        if len(errors) < 8:
                            errors.append(f"HTTP {response.status_code}: {response.text[:200]}")
                except Exception as exc:
                    latencies.append((time.perf_counter()-before)*1000)
                    rejected += len(rows)
                    if len(errors) < 8:
                        errors.append(str(exc))

        while time.perf_counter() < stop_at:
            slice_start = time.perf_counter()
            remaining = max(0, stop_at-slice_start)
            count = min(batch_size, max(1, int(rate * min(batch_size/rate, remaining))))
            stamp = datetime.now(timezone.utc).isoformat()
            rows = [{"reading_id": str(uuid.uuid4()), "sensor_id": f"load-{(sequence+j)%sensors:04}",
                     "sensor_type": "temperature", "value": 24.0 + ((sequence+j)%31)/10,
                     "unit": "C", "timestamp": stamp} for j in range(count)]
            task = asyncio.create_task(send(rows))
            tasks.add(task)
            task.add_done_callback(tasks.discard)
            submitted += count
            sequence += count
            await asyncio.sleep(max(0, batch_size/rate-(time.perf_counter()-slice_start)))
        if tasks:
            await asyncio.gather(*tasks)
    elapsed = time.perf_counter()-start
    async with httpx.AsyncClient(base_url=url, timeout=30) as client:
        metrics = (await client.get('/metrics')).json()
    report = {
        "run_at_utc": datetime.now(timezone.utc).isoformat(),
        "target_readings_per_second": rate,
        "duration_seconds": round(elapsed, 3),
        "batch_size": batch_size,
        "concurrent_request_limit": max_in_flight,
        "submitted_readings": submitted,
        "accepted_by_api": accepted,
        "rejected_or_failed": rejected,
        "accepted_throughput_per_second": round(accepted/elapsed, 2),
        "request_count": request_count,
        "request_latency_ms": {"p50": percentile(latencies,.50),"p95": percentile(latencies,.95),"p99": percentile(latencies,.99),
                               "mean": round(statistics.fmean(latencies),3) if latencies else None},
        "server_metrics_at_end": metrics,
        "errors": errors,
        "interpretation": "Accepted means placed in the process-local queue, not synchronously committed to SQLite. Verify persisted_readings after the queue drains.",
    }
    print(json.dumps(report, indent=2))
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Send a controlled batch-ingestion load to the running local API")
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--rate", type=int, default=10000, help="target readings/second")
    parser.add_argument("--seconds", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--sensors", type=int, default=200)
    parser.add_argument("--out", default="reports/latest-load-test.json")
    a = parser.parse_args()
    if min(a.rate,a.seconds,a.batch_size,a.sensors) < 1 or a.batch_size > 1000:
        parser.error("all values must be positive and batch-size must be <= 1000")
    report = asyncio.run(run(a.url,a.rate,a.seconds,a.batch_size,a.sensors))
    path=Path(a.out);path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(report,indent=2),encoding='utf-8')


if __name__ == "__main__":
    main()
