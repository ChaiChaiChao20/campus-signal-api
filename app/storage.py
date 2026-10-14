from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


class Store:
    """Single-writer SQLite store; writes are grouped into short transactions."""

    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=15)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=NORMAL")
        db.execute("PRAGMA busy_timeout=15000")
        return db

    def initialize(self) -> None:
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS readings (
                    reading_id TEXT PRIMARY KEY,
                    sensor_id TEXT NOT NULL,
                    sensor_type TEXT NOT NULL,
                    value REAL NOT NULL,
                    unit TEXT,
                    timestamp_ms INTEGER NOT NULL,
                    received_ms INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_readings_sensor_time
                    ON readings(sensor_id, sensor_type, timestamp_ms);
                CREATE TABLE IF NOT EXISTS rules (
                    rule_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    sensor_type TEXT NOT NULL,
                    sensor_id TEXT,
                    mode TEXT NOT NULL,
                    operator TEXT NOT NULL,
                    threshold REAL NOT NULL,
                    window_seconds INTEGER,
                    minimum_samples INTEGER NOT NULL,
                    enabled INTEGER NOT NULL,
                    created_ms INTEGER NOT NULL,
                    updated_ms INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_rules_enabled ON rules(enabled, sensor_type);
                CREATE TABLE IF NOT EXISTS alerts (
                    alert_id TEXT PRIMARY KEY,
                    rule_id TEXT NOT NULL,
                    rule_name TEXT NOT NULL,
                    sensor_id TEXT NOT NULL,
                    sensor_type TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    observed REAL NOT NULL,
                    threshold REAL NOT NULL,
                    message TEXT NOT NULL,
                    state TEXT NOT NULL,
                    created_ms INTEGER NOT NULL,
                    updated_ms INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_alerts_state_time ON alerts(state, created_ms DESC);
                CREATE INDEX IF NOT EXISTS ix_alerts_active_key ON alerts(rule_id, sensor_id, state);
            """)

    def load_rules(self) -> list[dict[str, Any]]:
        with self.connect() as db:
            rows = db.execute("SELECT * FROM rules ORDER BY created_ms").fetchall()
            return [dict(row) for row in rows]

    def active_alert_keys(self) -> set[tuple[str, str]]:
        with self.connect() as db:
            rows = db.execute("SELECT DISTINCT rule_id, sensor_id FROM alerts WHERE state IN ('open','acknowledged')").fetchall()
            return {(row["rule_id"], row["sensor_id"]) for row in rows}

    def persist_batch(self, readings: list[dict[str, Any]], alerts: list[dict[str, Any]], now_ms: int) -> int:
        with self.connect() as db:
            db.executemany("""
                INSERT OR IGNORE INTO readings(reading_id,sensor_id,sensor_type,value,unit,timestamp_ms,received_ms)
                VALUES(:reading_id,:sensor_id,:sensor_type,:value,:unit,:timestamp_ms,:received_ms)
            """, readings)
            accepted = db.total_changes
            db.executemany("""
                INSERT OR IGNORE INTO alerts(alert_id,rule_id,rule_name,sensor_id,sensor_type,mode,observed,threshold,message,state,created_ms,updated_ms)
                VALUES(:alert_id,:rule_id,:rule_name,:sensor_id,:sensor_type,:mode,:observed,:threshold,:message,'open',:created_ms,:updated_ms)
            """, alerts)
            return int(accepted)

    def save_rule(self, rule: dict[str, Any]) -> None:
        with self.connect() as db:
            db.execute("""
                INSERT INTO rules(rule_id,name,sensor_type,sensor_id,mode,operator,threshold,window_seconds,minimum_samples,enabled,created_ms,updated_ms)
                VALUES(:rule_id,:name,:sensor_type,:sensor_id,:mode,:operator,:threshold,:window_seconds,:minimum_samples,:enabled,:created_ms,:updated_ms)
                ON CONFLICT(rule_id) DO UPDATE SET
                    name=excluded.name,sensor_type=excluded.sensor_type,sensor_id=excluded.sensor_id,mode=excluded.mode,
                    operator=excluded.operator,threshold=excluded.threshold,window_seconds=excluded.window_seconds,
                    minimum_samples=excluded.minimum_samples,enabled=excluded.enabled,updated_ms=excluded.updated_ms
            """, rule)

    def delete_rule(self, rule_id: str) -> bool:
        with self.connect() as db:
            cur = db.execute("DELETE FROM rules WHERE rule_id=?", (rule_id,))
            return cur.rowcount > 0

    def list_rules(self) -> list[dict[str, Any]]:
        return self.load_rules()

    def list_alerts(self, state: str | None, limit: int, offset: int) -> list[dict[str, Any]]:
        sql = "SELECT * FROM alerts"
        params: list[Any] = []
        if state:
            sql += " WHERE state=?"
            params.append(state)
        sql += " ORDER BY created_ms DESC LIMIT ? OFFSET ?"
        params.extend((limit, offset))
        with self.connect() as db:
            return [dict(r) for r in db.execute(sql, params).fetchall()]

    def change_alert_state(self, alert_id: str, state: str, now_ms: int) -> dict[str, Any] | None:
        with self.connect() as db:
            cur = db.execute("UPDATE alerts SET state=?,updated_ms=? WHERE alert_id=? AND state IN ('open','acknowledged')", (state, now_ms, alert_id))
            if not cur.rowcount:
                return None
            row = db.execute("SELECT * FROM alerts WHERE alert_id=?", (alert_id,)).fetchone()
            return dict(row) if row else None

    def sensors(self) -> list[dict[str, Any]]:
        with self.connect() as db:
            return [dict(r) for r in db.execute("""
                SELECT sensor_id,sensor_type,COUNT(*) AS reading_count,MAX(timestamp_ms) AS last_reading_ms
                FROM readings GROUP BY sensor_id,sensor_type ORDER BY sensor_id,sensor_type
            """).fetchall()]

    def reading_count(self) -> int:
        with self.connect() as db:
            return int(db.execute("SELECT COUNT(*) FROM readings").fetchone()[0])

    def alert_count(self) -> int:
        with self.connect() as db:
            return int(db.execute("SELECT COUNT(*) FROM alerts").fetchone()[0])

    def save_report(self, report: dict[str, Any], path: str | Path) -> None:
        Path(path).write_text(json.dumps(report, indent=2), encoding="utf-8")
