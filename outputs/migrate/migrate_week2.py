"""
outputs/migrate/migrate_week2.py
=================================
Transforms Week 2 AuditReport JSON files into canonical verdict_record JSONL.

Week 2 actual output is AuditReport (Pydantic) saved as JSON files in reports/.
Fields: repo_url, executive_summary, overall_score (float), criteria (list of
CriterionResult), remediation_plan, timestamp.

DEVIATIONS & SENTINEL VALUES:
  - verdict_id       → generated UUIDv4 (no source field)
  - target_ref       → repo_url from AuditReport
  - rubric_id        → sha256("default-rubric") sentinel (WARNING logged)
  - rubric_version   → "1.0.0" sentinel (WARNING logged)
  - scores           → built from criteria[].dimension_name / criteria[].final_score
  - overall_verdict  → derived: overall_score >= 3.5 → PASS, < 2.5 → FAIL, else WARN
  - confidence       → 1.0 sentinel (WARNING logged)
  - evaluated_at     → AuditReport.timestamp if not null, else datetime.utcnow()

Usage:
    python outputs/migrate/migrate_week2.py \\
        --source path/to/automaton-auditor/reports/ \\
        --output outputs/week2/verdicts.jsonl
"""
import argparse
import hashlib
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path


SENTINEL_RUBRIC_ID = hashlib.sha256(b"default-rubric").hexdigest()
SENTINEL_RUBRIC_VERSION = "1.0.0"
SENTINEL_CONFIDENCE = 1.0


def _warn(msg: str) -> None:
    print(f"WARNING: {msg}", file=sys.stderr)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _derive_verdict(overall_score: float) -> str:
    if overall_score >= 3.5:
        return "PASS"
    elif overall_score < 2.5:
        return "FAIL"
    else:
        return "WARN"


def _transform_audit_report(data: dict, source_file: str) -> dict:
    verdict_id = str(uuid.uuid4())

    target_ref = data.get("repo_url", "")
    if not target_ref:
        _warn(f"target_ref is empty for {source_file}; repo_url missing")

    _warn(f"rubric_id using sentinel sha256('default-rubric') for {source_file}")
    _warn(f"rubric_version using sentinel '1.0.0' for {source_file}")
    _warn(f"confidence using sentinel 1.0 for {source_file}")

    overall_score = float(data.get("overall_score", 0.0))
    overall_verdict = _derive_verdict(overall_score)

    # Build scores dict from criteria
    scores = {}
    for criterion in data.get("criteria", []):
        dim_name = criterion.get("dimension_name", criterion.get("dimension_id", "unknown"))
        final_score = criterion.get("final_score", 0)

        # Collect evidence from judge_opinions
        evidence = []
        for opinion in criterion.get("judge_opinions", []):
            for cited in opinion.get("cited_evidence", []):
                evidence.append(cited)

        notes = criterion.get("dissent_summary") or criterion.get("remediation") or ""

        scores[dim_name] = {
            "score": final_score,
            "evidence": evidence,
            "notes": notes,
        }

    # evaluated_at
    timestamp = data.get("timestamp")
    if timestamp:
        evaluated_at = timestamp
    else:
        _warn(f"evaluated_at using datetime.utcnow() sentinel for {source_file} (timestamp is null)")
        evaluated_at = _now_iso()

    return {
        "verdict_id": verdict_id,
        "target_ref": target_ref,
        "rubric_id": SENTINEL_RUBRIC_ID,
        "rubric_version": SENTINEL_RUBRIC_VERSION,
        "scores": scores,
        "overall_verdict": overall_verdict,
        "overall_score": overall_score,
        "confidence": SENTINEL_CONFIDENCE,
        "evaluated_at": evaluated_at,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate Week 2 AuditReport → verdict_record JSONL")
    parser.add_argument("--source", required=True, help="Directory containing AuditReport JSON files")
    parser.add_argument("--output", required=True, help="Output JSONL path")
    args = parser.parse_args()

    try:
        source_dir = Path(args.source)
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if not source_dir.exists():
            print(f"ERROR: Source directory does not exist: {source_dir}", file=sys.stderr)
            sys.exit(1)

        json_files = sorted(source_dir.glob("*.json"))
        if not json_files:
            print(f"WARNING: No JSON files found in {source_dir}", file=sys.stderr)

        records = []
        for jf in json_files:
            try:
                data = json.loads(jf.read_text(encoding="utf-8"))
                record = _transform_audit_report(data, jf.name)
                records.append(record)
                print(f"INFO: Processed {jf.name}", file=sys.stderr)
            except Exception as exc:
                print(f"ERROR: Failed to process {jf}: {exc}", file=sys.stderr)

        with output_path.open("w", encoding="utf-8") as fh:
            for rec in records:
                fh.write(json.dumps(rec) + "\n")

        print(f"INFO: Wrote {len(records)} verdict_records to {output_path}", file=sys.stderr)

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
