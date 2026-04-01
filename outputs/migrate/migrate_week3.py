"""
outputs/migrate/migrate_week3.py
=================================
Combines Week 3 extraction_ledger.jsonl + fact_table.sqlite
into canonical extraction_record JSONL.

Deviations handled:
  - doc_id is filename   → uuid5(NAMESPACE_URL, filename)
  - facts in SQLite      → joined by doc_id into extracted_facts[]
  - no source_path       → sentinel: doc_id string
  - no source_hash       → sha256 of doc_id string
  - no entities[]        → empty list (facts have no entity extraction)
  - no extraction_model  → sentinel "unknown"
  - no processing_time_ms→ sentinel 0
  - no token_count       → sentinel {"input": 0, "output": 0}
  - timestamp null       → datetime.utcnow()

Usage:
    python outputs/migrate/migrate_week3.py \
        --ledger path/to/extraction_ledger.jsonl \
        --sqlite path/to/.refinery/fact_table.sqlite \
        --output outputs/week3/extractions.jsonl
"""
import argparse


def main():
    parser = argparse.ArgumentParser(description="Migrate Week 3 → extraction_record")
    parser.add_argument("--ledger", required=True, help="Path to extraction_ledger.jsonl")
    parser.add_argument("--sqlite", required=True, help="Path to fact_table.sqlite")
    parser.add_argument("--output", required=True, help="Output JSONL path")
    args = parser.parse_args()
    # TODO: implement


if __name__ == "__main__":
    main()
