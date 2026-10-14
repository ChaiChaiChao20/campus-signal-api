from __future__ import annotations

import asyncio
import bisect
import logging
import os
import time
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field, field_validator

from .storage import Store

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("campus-signal")
DB_PATH = Path(os.getenv("CAMPUS_DB_PATH", "data/campus.sqlite3"))
MAX_QUEUE = int(os.getenv("INGEST_QUEUE_CAPACITY", "100000"))
BATCH_SIZE = int(os.getenv("PERSIST_BATCH_SIZE", "1000"))
FLUSH_SECONDS = float(os.getenv("PERSIST_FLUSH_SECONDS", "0.025"))
MAX_INPUT_BATCH = 1000
OPERATORS = {"gt", "gte", "lt", "lte", "eq"}
store = Store(DB_PATH)


def epoch_ms(value: datetime | None = None) -> int:
    return int((value or datetime.now(timezone.utc)).timestamp() * 1000)


class Reading(BaseModel):
    model_config = ConfigDict(extra="forbid")
    reading_id: str = Field(default_factory=lambda: str(uuid.uuid4()), max_length=80)
    sensor_id: str = Field(min_length=1, max_length=100)
    sensor_type: str = Field(min_length=1, max_length=80, description="For example temperature, humidity, or vibration")
    value: float
    unit: str | None = Field(default=None, max_length=24)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("sensor_id", "sensor_type")
    @classmethod
    def trim_identifiers(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("must contain non-whitespace characters")
        return cleaned

    @field_validator("value")
    @classmethod
    def finite_value(cls, value: float) -> float:
        if not (-1e12 < value < 1e12):
            raise ValueError("value must be finite and within the supported numeric range")
        return value

    def stored(self, received_ms: int) -> dict[str, Any]:
        ts = self.timestamp if self.timestamp.tzinfo else self.timestamp.replace(tzinfo=timezone.utc)
        return {"reading_id": self.reading_id, "sensor_id": self.sensor_id, "sensor_type": self.sensor_type,
                "value": self.value, "unit": self.unit, "timestamp_ms": epoch_ms(ts), "received_ms": received_ms}


class BatchReadings(BaseModel):
    model_config = ConfigDict(extra="forbid")
    readings: list[Reading] = Field(min_length=1, max_length=MAX_INPUT_BATCH)


class RuleIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=120)
    sensor_type: str = Field(min_length=1, max_length=80)
    sensor_id: str | None = Field(default=None, max_length=100)
    mode: Literal["threshold", "rolling_average"] = "threshold"
    operator: Literal["gt", "gte", "lt", "lte", "eq"] = "gt"
    threshold: float
    window_seconds: int | None = Field(default=None, ge=1, le=86400)
    minimum_samples: int = Field(default=3, ge=1, le=10000)
    enabled: bool = True

    @field_validator("name", "sensor_type")
    @classmethod
    def trim_rule_strings(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must contain non-whitespace characters")
        return value

    @field_validator("window_seconds")
    @classmethod
    def validate_window(cls, value: int | None, info: Any) -> int | None:
        if info.data.get("mode") == "rolling_average" and value is None:
            raise ValueError("rolling_average requires window_seconds")
        return value


class RulePatch(BaseModel):
    enabled: bool


class Runtime:
    def __init__(self) -> None:
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=MAX_QUEUE)
        self.rules: dict[str, dict[str, Any]] = {}
        self.active_alerts: set[tuple[str, str]] = set()
        self.windows: dict[tuple[str, str], list[tuple[int, float]]] = defaultdict(list)
        self.worker: asyncio.Task[None] | None = None
        self.accepted = 0
        self.persisted = 0
        self.rejected = 0
        self.flushes = 0
        self.started_ms = epoch_ms()
        self.rule_lock = asyncio.Lock()

    async def start(self) -> None:
        await asyncio.to_thread(store.initialize)
        for raw in await asyncio.to_thread(store.load_rules):
            raw["enabled"] = bool(raw["enabled"])
            self.rules[raw["rule_id"]] = raw
        self.active_alerts = await asyncio.to_thread(store.active_alert_keys)
        self.worker = asyncio.create_task(self._consume(), name="sqlite-batched-writer")

    async def stop(self) -> None:
        if self.worker:
            try:
                await asyncio.wait_for(self.queue.join(), timeout=float(os.getenv("SHUTDOWN_DRAIN_SECONDS", "10")))
            except TimeoutError:
                log.error("Timed out waiting for the queue to flush; attempting final best-effort drain")
            self.worker.cancel()
            with suppress(asyncio.CancelledError):
                await self.worker
        # Best-effort final drain during graceful shutdown.
        pending: list[dict[str, Any]] = []
        while not self.queue.empty():
            pending.append(self.queue.get_nowait())
        if pending:
            try:
                await self._persist(pending)
            except Exception:
                log.exception("Final queue drain failed; %d accepted readings were not persisted", len(pending))

    async def _consume(self) -> None:
        while True:
            first = await self.queue.get()
            batch = [first]
            deadline = asyncio.get_running_loop().time() + FLUSH_SECONDS
            while len(batch) < BATCH_SIZE:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    break
                try:
                    batch.append(await asyncio.wait_for(self.queue.get(), timeout=remaining))
                except TimeoutError:
                    break
            try:
                await self._persist(batch)
            except Exception:
                log.exception("Batch persistence failed; retaining batch and retrying")
                await asyncio.sleep(0.25)
                # Never acknowledge persistence to metrics until the transaction commits.
                while True:
                    try:
                        await self._persist(batch)
                        break
                    except Exception:
                        log.exception("Batch persistence retry failed")
                        await asyncio.sleep(1.0)

    async def _persist(self, batch: list[dict[str, Any]]) -> None:
        async with self.rule_lock:
            await self._persist_locked(batch)

    async def _persist_locked(self, batch: list[dict[str, Any]]) -> None:
        now = epoch_ms()
        new_alerts: list[dict[str, Any]] = []
        active = set(self.active_alerts)
        rules = [r for r in self.rules.values() if r["enabled"]]
        for reading in batch:
            for rule in rules:
                if rule["sensor_type"] != reading["sensor_type"]:
                    continue
                if rule["sensor_id"] and rule["sensor_id"] != reading["sensor_id"]:
                    continue
                observed: float | None = reading["value"]
                if rule["mode"] == "rolling_average":
                    key = (rule["rule_id"], reading["sensor_id"])
                    points = self.windows[key]
                    ts = reading["timestamp_ms"]
                    bisect.insort(points, (ts, reading["value"]))
                    cutoff = ts - int(rule["window_seconds"] or 600) * 1000
                    first_live = bisect.bisect_left(points, (cutoff, float("-inf")))
                    if first_live:
                        del points[:first_live]
                    # Late-arriving events must never see measurements from
                    # after their event timestamp in a past-looking window.
                    past_end = bisect.bisect_right(points, (ts, float("inf")))
                    window_points = points[:past_end]
                    if len(window_points) < rule["minimum_samples"]:
                        continue
                    observed = sum(value for _, value in window_points) / len(window_points)
                if not _matches(float(observed), rule["operator"], float(rule["threshold"])):
                    continue
                active_key = (rule["rule_id"], reading["sensor_id"])
                if active_key in active:
                    continue
                active.add(active_key)
                new_alerts.append({
                    "alert_id": str(uuid.uuid4()), "rule_id": rule["rule_id"], "rule_name": rule["name"],
                    "sensor_id": reading["sensor_id"], "sensor_type": reading["sensor_type"], "mode": rule["mode"],
                    "observed": float(observed), "threshold": float(rule["threshold"]),
                    "message": f"{rule['name']}: {reading['sensor_type']} {rule['operator']} {rule['threshold']} (observed {float(observed):.3f})",
                    "created_ms": now, "updated_ms": now,
                })
        inserted = await asyncio.to_thread(store.persist_batch, batch, new_alerts, now)
        self.active_alerts.update((alert["rule_id"], alert["sensor_id"]) for alert in new_alerts)
        self.persisted += inserted
        self.flushes += 1
        for _ in batch:
            self.queue.task_done()


def _matches(value: float, operator: str, threshold: float) -> bool:
    return {"gt": value > threshold, "gte": value >= threshold, "lt": value < threshold,
            "lte": value <= threshold, "eq": value == threshold}[operator]


runtime = Runtime()


@asynccontextmanager
async def lifespan(_: FastAPI):
    await runtime.start()
    yield
    await runtime.stop()


app = FastAPI(title="Campus Sensor Monitoring and Alerts", version="1.0.0", lifespan=lifespan)


def rule_payload(rule: RuleIn, rule_id: str, created_ms: int, updated_ms: int) -> dict[str, Any]:
    item = rule.model_dump()
    item.update(rule_id=rule_id, enabled=int(item["enabled"]), created_ms=created_ms, updated_ms=updated_ms)
    return item


async def enqueue(readings: list[Reading]) -> dict[str, Any]:
    now = epoch_ms()
    if runtime.queue.maxsize - runtime.queue.qsize() < len(readings):
        runtime.rejected += len(readings)
        raise HTTPException(status_code=503, detail={"message": "ingestion queue is full; retry with backoff", "retryable": True})
    for reading in readings:
        runtime.queue.put_nowait(reading.stored(now))
    runtime.accepted += len(readings)
    return {"status": "queued", "accepted": len(readings), "queue_depth": runtime.queue.qsize(),
            "durability": "accepted into this process memory queue; persistence is asynchronous"}


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"status": "ok", "version": app.version, "queue_depth": runtime.queue.qsize(), "queue_capacity": MAX_QUEUE}


@app.get("/metrics")
async def metrics() -> dict[str, Any]:
    return {"uptime_seconds": round((epoch_ms() - runtime.started_ms) / 1000, 3), "queue_depth": runtime.queue.qsize(),
            "queue_capacity": MAX_QUEUE, "accepted_readings": runtime.accepted, "persisted_readings": runtime.persisted,
            "rejected_readings": runtime.rejected, "sqlite_flushes": runtime.flushes,
            "active_rules": sum(bool(r["enabled"]) for r in runtime.rules.values()), "database_readings": await asyncio.to_thread(store.reading_count)}


@app.post("/readings", status_code=202)
async def ingest_one(reading: Reading) -> dict[str, Any]:
    return await enqueue([reading])


@app.post("/readings/batch", status_code=202)
async def ingest_batch(body: BatchReadings) -> dict[str, Any]:
    return await enqueue(body.readings)


@app.get("/sensors")
async def list_sensors() -> list[dict[str, Any]]:
    return await asyncio.to_thread(store.sensors)


@app.post("/rules", status_code=201)
async def create_rule(rule: RuleIn) -> dict[str, Any]:
    now = epoch_ms()
    item = rule_payload(rule, str(uuid.uuid4()), now, now)
    async with runtime.rule_lock:
        await asyncio.to_thread(store.save_rule, item)
        runtime.rules[item["rule_id"]] = {**item, "enabled": bool(item["enabled"])}
    return {**item, "enabled": bool(item["enabled"])}


@app.get("/rules")
async def list_rules() -> list[dict[str, Any]]:
    return await asyncio.to_thread(store.list_rules)


@app.get("/rules/{rule_id}")
async def get_rule(rule_id: str) -> dict[str, Any]:
    rule = runtime.rules.get(rule_id)
    if not rule:
        raise HTTPException(404, "rule not found")
    return rule


@app.put("/rules/{rule_id}")
async def replace_rule(rule_id: str, body: RuleIn) -> dict[str, Any]:
    old = runtime.rules.get(rule_id)
    if not old:
        raise HTTPException(404, "rule not found")
    item = rule_payload(body, rule_id, int(old["created_ms"]), epoch_ms())
    async with runtime.rule_lock:
        await asyncio.to_thread(store.save_rule, item)
        runtime.rules[rule_id] = {**item, "enabled": bool(item["enabled"])}
    return runtime.rules[rule_id]


@app.patch("/rules/{rule_id}")
async def enable_rule(rule_id: str, body: RulePatch) -> dict[str, Any]:
    old = runtime.rules.get(rule_id)
    if not old:
        raise HTTPException(404, "rule not found")
    item = {**old, "enabled": body.enabled, "updated_ms": epoch_ms()}
    async with runtime.rule_lock:
        await asyncio.to_thread(store.save_rule, {**item, "enabled": int(item["enabled"])})
        runtime.rules[rule_id] = item
    return item


@app.delete("/rules/{rule_id}", status_code=204)
async def delete_rule(rule_id: str) -> Response:
    async with runtime.rule_lock:
        existed = await asyncio.to_thread(store.delete_rule, rule_id)
        runtime.rules.pop(rule_id, None)
    if not existed:
        raise HTTPException(404, "rule not found")
    return Response(status_code=204)


@app.get("/alerts")
async def list_alerts(state: Literal["open", "acknowledged", "resolved"] | None = None,
                     limit: int = Query(default=100, ge=1, le=500), offset: int = Query(default=0, ge=0)) -> list[dict[str, Any]]:
    return await asyncio.to_thread(store.list_alerts, state, limit, offset)


async def transition_alert(alert_id: str, state: Literal["acknowledged", "resolved"]) -> dict[str, Any]:
    async with runtime.rule_lock:
        result = await asyncio.to_thread(store.change_alert_state, alert_id, state, epoch_ms())
        if result is None:
            raise HTTPException(404, "alert not found or already resolved")
        if state == "resolved":
            runtime.active_alerts.discard((result["rule_id"], result["sensor_id"]))
    return result


@app.post("/alerts/{alert_id}/acknowledge")
async def acknowledge_alert(alert_id: str) -> dict[str, Any]:
    return await transition_alert(alert_id, "acknowledged")


@app.post("/alerts/{alert_id}/resolve")
async def resolve_alert(alert_id: str) -> dict[str, Any]:
    return await transition_alert(alert_id, "resolved")
