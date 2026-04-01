"""
outputs/migrate/migrate_week2.py
=================================
Transforms Week 2 AuditReport JSON files into canonical verdict_record JSONL.

Deviations handled:
  - No verdict_id        → generated UUIDv4
  - No overall_verdict   → derived from overall_score (>=3.5 PASS, <2.5 FAIL, else WARN)
  - No rubric_id         → sha256 of rubric filename if available, else sentinel "unknown"
  - No rubric_version    → sentinel "0.0.0"
  - No target_ref        → derived from repo_url
  - No confidence        → sentinel 0.0
  - timestamp (str)      → mapped to evaluated_at; if null uses datetime.utcnow()
  - scores dict          → mapped from criteria[].final_score

Usage:
    python outputs/migrate/migrate_week2.py \
        --source path/to/automaton-auditor/reports/ \
        --output outputs/week2/verdicts.jsonl
"""
import argparse


def main():
    parser = argparse.ArgumentParser(description="Migrate Week 2 AuditReport → verdict_record")
    parser.add_argument("--source", required=True, help="Directory containing AuditReport JSON files")
    parser.add_argument("--output", required=True, help="Output JSONL path")
    args = parser.parse_args()
    # TODO: implement


if __name__ == "__main__":
    main()
