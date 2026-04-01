"""
outputs/migrate/migrate_week5.py
=================================
Exports Week 5 PostgreSQL events table to canonical event_record JSONL.

Week 5 actual output: PostgreSQL `events` table with columns:
  event_id, stream_id, stream_position, global_position, event_type,
  event_version, payload (JSONB), metadata (JSONB), recorded_at

DEVIATIONS & SENTINEL VALUES:
  - aggregate_id     → stream_id after first "-" (e.g. "LoanApplication-{uuid}" → "{uuid}")
  - aggregate_type   → stream_id before first "-" (e.g. "LoanApplication")
  - sequence_number  → stream_position (per-stream, not global)
  - occurred_at      → set equal to recorded_at (no occurred_at in DB) (WARNING logged)
  - schema_version   → str(event_version) + ".0" (e.g. 1 → "1.0")
  - metadata.user_id → metadata.get("user_id", "unknown") sentinel (WARNING logged)
  - metadata.source_service → metadata.get("source_service", "unknown") sentinel (WARNING logged)
  - metadata.correlation_id → metadata.get("correlation_id", str(uuid4())) sentinel

Usage:
    python outputs/migrate/migrate_week5.py \\
        --db-url postgresql://user:pass@localhost:5432/ledger \\
        --output outputs/week5/events.jsonl
"""
import argparse
import asyncio
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path


def _warn(msg: str) -> None:
    print(f"WARNING: {msg}", file=sys.stderr)


def _transform_row(row: dict) -> dict:
    stream_id: str = row.get("stream_id", "")

    # Derive aggregate_type and aggregate_id from stream_id
    if "-" in stream_id:
        aggregate_type, _, aggregate_id = stream_id.partition("-")
    else:
        aggregate_type = stream_id
        aggregate_id = stream_id

    # schema_version: int → "N.0"
    event_version = row.get("event_version", 1)
    schema_version = f"{event_version}.0"

    # occurred_at sentinel
    recorded_at = row.get("recorded_at")
    if isinstance(recorded_at, datetime):
        recorded_at_str = recorded_at.isoformat()
    else:
        recorded_at_str = str(recorded_at) if recorded_at else datetime.now(timezone.utc).isoformat()

    _warn(f"occurred_at using sentinel recorded_at='{recorded_at_str}' for event_id={row.get('event_id')} (no occurred_at in DB)")

    # metadata enrichment
    raw_meta = row.get("metadata") or {}
    if isinstance(raw_meta, str):
        try:
            raw_meta = json.loads(raw_meta)
        except Exception:
            raw_meta = {}

    user_id = raw_meta.get("user_id")
    if not user_id:
        _warn(f"metadata.user_id using sentinel 'unknown' for event_id={row.get('event_id')}")
        user_id = "unknown"

    source_service = raw_meta.get("source_service")
    if not source_service:
        _warn(f"metadata.source_service using sentinel 'unknown' for event_id={row.get('event_id')}")
        source_service = "unknown"

    correlation_id = raw_meta.get("correlation_id")
    if not correlation_id:
        correlation_id = str(uuid.uuid4())

    causation_id = raw_meta.get("causation_id", None)

    # payload
    payload = row.get("payload") or {}
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = {}

    event_id = row.get("event_id")
    if isinstance(event_id, uuid.UUID):
        event_id = str(event_id)

    return {
        "event_id": event_id,
        "event_type": row.get("event_type", ""),
        "aggregate_id": aggregate_id,
        "aggregate_type": aggregate_type,
        "sequence_number": row.get("stream_position", 0),
        "payload": payload,
        "metadata": {
            "causation_id": causation_id,
            "correlation_id": correlation_id,
            "user_id": user_id,
            "source_service": source_service,
        },
        "schema_version": schema_version,
        "occurred_at": recorded_at_str,
        "recorded_at": recorded_at_str,
    }


async def _export_events(db_url: str, output_path: Path) -> None:
    try:
        import asyncpg
    except ImportError:
        print("ERROR: asyncpg is not installed. Run: pip install asyncpg", file=sys.stderr)
        sys.exit(1)

    try:
        conn = await asyncpg.connect(db_url)
    except Exception as exc:
        print(f"ERROR: Failed to connect to database: {exc}", file=sys.stderr)
        sys.exit(1)

    try:
        rows = await conn.fetch(
            """
            SELECT event_id, stream_id, stream_position, global_position,
                   event_type, event_version, payload, metadata, recorded_at
            FROM events
            ORDER BY global_position ASC
            """
        )
    except Exception as exc:
        print(f"ERROR: Failed to query events table: {exc}", file=sys.stderr)
        await conn.close()
        sys.exit(1)

    await conn.close()

    records = []
    for row in rows:
        try:
            row_dict = dict(row)
            record = _transform_row(row_dict)
            records.append(record)
        except Exception as exc:
            print(f"ERROR: Failed to transform row {row.get('event_id')}: {exc}", file=sys.stderr)

    with output_path.open("w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, default=str) + "\n")

    print(f"INFO: Wrote {len(records)} event_records to {output_path}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate Week 5 PostgreSQL → event_record JSONL")
    parser.add_argument("--db-url", required=True, help="PostgreSQL connection URL")
    parser.add_argument("--output", required=True, help="Output JSONL path")
    args = parser.parse_args()

    try:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        asyncio.run(_export_events(args.db_url, output_path))
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
