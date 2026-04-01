"""
outputs/migrate/migrate_week3.py
=================================
Combines Week 3 extraction_ledger.jsonl + fact_table.sqlite
into canonical extraction_record JSONL.

Week 3 actual outputs:
  - extraction_ledger.jsonl: {doc_id (filename), final_strategy, total_cost_usd,
    timestamp, tables_found, text_blocks}
  - .refinery/fact_table.sqlite: table `facts` with columns
    id, doc_id, fact_key, fact_value, page_number, context, extraction_timestamp

DEVIATIONS & SENTINEL VALUES:
  - doc_id           → uuid5(NAMESPACE_URL, filename) (source is filename string)
  - source_path      → filename string sentinel (WARNING logged)
  - source_hash      → sha256 of doc_id string sentinel (WARNING logged)
  - extracted_facts  → from SQLite facts table joined by doc_id (filename)
  - fact_id          → generated UUIDv4 per fact
  - fact.confidence  → sentinel 0.85 (SQLite facts table has no confidence column) (WARNING logged)
  - entities[]       → empty list (no entity extraction in Week 3) (WARNING logged)
  - extraction_model → sentinel "unknown" (WARNING logged)
  - processing_time_ms → sentinel 0 (WARNING logged)
  - token_count      → sentinel {"input": 0, "output": 0} (WARNING logged)
  - extracted_at     → ledger timestamp if not null, else datetime.utcnow() (WARNING logged)

Usage:
    python outputs/migrate/migrate_week3.py \\
        --ledger path/to/extraction_ledger.jsonl \\
        --sqlite path/to/.refinery/fact_table.sqlite \\
        --output outputs/week3/extractions.jsonl
"""
import argparse
import hashlib
import json
import sqlite3
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path


def _warn(msg: str) -> None:
    print(f"WARNING: {msg}", file=sys.stderr)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _doc_uuid(filename: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, filename))


def _source_hash(doc_id_str: str) -> str:
    return hashlib.sha256(doc_id_str.encode()).hexdigest()


def _load_facts_from_sqlite(sqlite_path: Path) -> dict[str, list[dict]]:
    """Load all facts from SQLite, keyed by doc_id (filename)."""
    facts_by_doc: dict[str, list[dict]] = {}

    if not sqlite_path.exists():
        print(f"WARNING: SQLite file not found: {sqlite_path}", file=sys.stderr)
        return facts_by_doc

    try:
        conn = sqlite3.connect(str(sqlite_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Check what columns exist
        cursor.execute("PRAGMA table_info(facts)")
        columns = {row["name"] for row in cursor.fetchall()}

        select_cols = ["id", "doc_id", "fact_key", "fact_value", "page_number", "context"]
        # extraction_timestamp may or may not exist
        if "extraction_timestamp" in columns:
            select_cols.append("extraction_timestamp")

        cursor.execute(f"SELECT {', '.join(select_cols)} FROM facts")
        rows = cursor.fetchall()
        conn.close()

        for row in rows:
            doc_id_key = row["doc_id"]
            if doc_id_key not in facts_by_doc:
                facts_by_doc[doc_id_key] = []
            facts_by_doc[doc_id_key].append(dict(row))

    except Exception as exc:
        print(f"ERROR: Failed to read SQLite {sqlite_path}: {exc}", file=sys.stderr)

    return facts_by_doc


def _build_extracted_facts(raw_facts: list[dict], doc_id_str: str) -> list[dict]:
    result = []
    _warn(f"extracted_facts[].confidence using sentinel 0.85 for doc '{doc_id_str}' (no confidence column in SQLite)")
    for row in raw_facts:
        fact_text = f"{row.get('fact_key', '')}: {row.get('fact_value', '')}"
        result.append({
            "fact_id": str(uuid.uuid4()),
            "text": fact_text,
            "entity_refs": [],
            "confidence": 0.85,
            "page_ref": row.get("page_number", 0),
            "source_excerpt": row.get("context", ""),
        })
    return result


def _transform_ledger_entry(entry: dict, facts_by_doc: dict[str, list[dict]]) -> dict:
    filename = entry.get("doc_id", "")
    doc_id = _doc_uuid(filename)

    _warn(f"source_path using sentinel filename '{filename}' for doc '{doc_id}'")
    _warn(f"source_hash using sentinel sha256(doc_id) for doc '{doc_id}'")
    _warn(f"entities[] using sentinel [] for doc '{doc_id}' (no entity extraction in Week 3)")
    _warn(f"extraction_model using sentinel 'unknown' for doc '{doc_id}'")
    _warn(f"processing_time_ms using sentinel 0 for doc '{doc_id}'")
    _warn(f"token_count using sentinel {{input:0, output:0}} for doc '{doc_id}'")

    raw_facts = facts_by_doc.get(filename, [])
    extracted_facts = _build_extracted_facts(raw_facts, filename)

    timestamp = entry.get("timestamp")
    if timestamp:
        extracted_at = timestamp
    else:
        _warn(f"extracted_at using datetime.utcnow() sentinel for doc '{doc_id}' (timestamp is null)")
        extracted_at = _now_iso()

    return {
        "doc_id": doc_id,
        "source_path": filename,
        "source_hash": _source_hash(doc_id),
        "extracted_facts": extracted_facts,
        "entities": [],
        "extraction_model": "unknown",
        "processing_time_ms": 0,
        "token_count": {"input": 0, "output": 0},
        "extracted_at": extracted_at,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate Week 3 → extraction_record JSONL")
    parser.add_argument("--ledger", required=True, help="Path to extraction_ledger.jsonl")
    parser.add_argument("--sqlite", required=True, help="Path to fact_table.sqlite")
    parser.add_argument("--output", required=True, help="Output JSONL path")
    args = parser.parse_args()

    try:
        ledger_path = Path(args.ledger)
        sqlite_path = Path(args.sqlite)
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if not ledger_path.exists():
            print(f"ERROR: Ledger file not found: {ledger_path}", file=sys.stderr)
            sys.exit(1)

        facts_by_doc = _load_facts_from_sqlite(sqlite_path)
        print(f"INFO: Loaded facts for {len(facts_by_doc)} documents from SQLite", file=sys.stderr)

        records = []
        with ledger_path.open(encoding="utf-8") as fh:
            for line_num, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    record = _transform_ledger_entry(entry, facts_by_doc)
                    records.append(record)
                except Exception as exc:
                    print(f"ERROR: Failed to process ledger line {line_num}: {exc}", file=sys.stderr)

        with output_path.open("w", encoding="utf-8") as fh:
            for rec in records:
                fh.write(json.dumps(rec) + "\n")

        print(f"INFO: Wrote {len(records)} extraction_records to {output_path}", file=sys.stderr)

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
