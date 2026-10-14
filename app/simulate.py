from __future__ import annotations

import argparse
import asyncio
import math
import random
import time
import uuid

import httpx


async def run(base_url: str, seconds: float, rate: int, batch_size: int, sensor_count: int) -> None:
    random.seed(41)
    interval = batch_size / rate
    deadline = time.perf_counter() + seconds
    tasks: set[asyncio.Task] = set()
    sent = 0
    async with httpx.AsyncClient(base_url=base_url, timeout=10, limits=httpx.Limits(max_connections=24)) as client:
        async def post_batch(rows: list[dict]) -> None:
            response = await client.post("/readings/batch", json={"readings": rows})
            response.raise_for_status()

        while time.perf_counter() < deadline:
            due = min(batch_size, max(1, int(rate * min(interval, max(0.05, deadline-time.perf_counter())))))
            now = time.time()
            rows = []
            for i in range(due):
                seq = sent + i
                event = seq % 15000 in range(0, 100)
                value = 25.0 + random.gauss(0, 1.1) + (14.0 if event else 0.0)
                rows.append({"reading_id": str(uuid.uuid4()), "sensor_id": f"temp-{seq % sensor_count:04}",
                             "sensor_type": "temperature", "value": round(value, 3), "unit": "C",
                             "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now)) + f".{int((now%1)*1_000_000):06d}Z"})
            task = asyncio.create_task(post_batch(rows))
            tasks.add(task)
            task.add_done_callback(tasks.discard)
            sent += due
            await asyncio.sleep(interval)
        if tasks:
            await asyncio.gather(*tasks)
    print(f"Simulator submitted {sent} readings to {base_url}")


def main() -> None:
    p = argparse.ArgumentParser(description="Generate campus sensor traffic and occasional thermal spikes")
    p.add_argument("--url", default="http://127.0.0.1:8000")
    p.add_argument("--seconds", type=float, default=30)
    p.add_argument("--rate", type=int, default=100)
    p.add_argument("--batch-size", type=int, default=100)
    p.add_argument("--sensors", type=int, default=40)
    a = p.parse_args()
    if a.rate < 1 or a.batch_size < 1 or a.batch_size > 1000 or a.sensors < 1:
        p.error("rate and sensors must be positive; batch-size must be 1..1000")
    asyncio.run(run(a.url, a.seconds, a.rate, a.batch_size, a.sensors))


if __name__ == "__main__":
    main()
