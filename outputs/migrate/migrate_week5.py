"""
outputs/migrate/migrate_week5.py
=================================
Exports Week 5 PostgreSQL events table to canonical event_record JSONL.

Deviations handled:
  - stream_position      → sequence_number
  - aggregate_id         → derived from stream_id (split on first "-", rest is UUID)
  - aggregate_type       → derived from stream_id prefix before first "-"
  - No occurred_at       → set equal to recorded_at (WARNING logged)
  - schema_version int   → converted to "1.0" string
  - No metadata.user_id  → sentinel "unknown"
  - No metadata.source_service → sentinel "unknown"

Usage:
    python outputs/migrate/migrate_week5.py \
        --db-url postgresql://user:pass@localhost:5432/ledger \
        --output outputs/week5/events.jsonl
"""
import argparse


def main():
    parser = argparse.ArgumentParser(description="Migrate Week 5 PostgreSQL → event_record")
    parser.add_argument("--db-url", required=True, help="PostgreSQL connection URL")
    parser.add_argument("--output", required=True, help="Output JSONL path")
    args = parser.parse_args()
    # TODO: implement


if __name__ == "__main__":
    main()
