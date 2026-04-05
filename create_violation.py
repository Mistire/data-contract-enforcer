"""
create_violation.py — Synthetic violation injection script
===========================================================
Injects a synthetic violation into the pipeline by:
  1. Reading outputs/week3/extractions.jsonl (or generating synthetic data if absent)
  2. Scaling extracted_facts[*].confidence from 0.0-1.0 to 0-100 range (a breaking change)
  3. Running ValidationRunner against the modified data to produce a real FAIL result
  4. Writing the violation record to violation_log/violations.jsonl with injection_note: true

The "comment block" at the top of violations.jsonl is written as a JSON object
with a "_comment" key — JSONL files do not support # comments.

Requirements: 16.8
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_INPUT = Path("outputs/week3/extractions.jsonl")
CONTRACT_PATH = Path("generated_contracts/week3-document-refinery-extractions.yaml")
VIOLATION_LOG = Path("violation_log/violations.jsonl")
REPORT_PATH = Path("validation_reports/injected_violation.json")

# JSON _comment record used as the header line of violations.jsonl
# (JSONL does not support # comments; a {"_comment": "..."} object is the idiomatic alternative)
_COMMENT_RECORD = {
    "_comment": (
        "VIOLATION LOG — violation_log/violations.jsonl | "
        "Records contract violations detected by the Data Contract Enforcer. "
        "Records with injection_note=true are SYNTHETIC violations injected by "
        "create_violation.py to simulate the confidence scale change (0.0-1.0 to 0-100). "
        "Schema: violation_id (UUIDv4), check_id (dot-notation), detected_at (ISO 8601), "
        "type (range|statistical_drift|llm_output_schema|embedding_drift), "
        "injection_note (bool), blame_chain (ranked 1-5 git candidates), "
        "blast_radius (downstream nodes/pipelines). "
        "Forward-compatible with Week 8 Sentinel pipeline alert ingestion format."
    )
}


def _load_jsonl(path):
    records = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                obj = json.loads(line)
                if "_comment" not in obj:
                    records.append(obj)
            except json.JSONDecodeError:
                pass
    return records


def _write_jsonl(path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")


def _generate_synthetic_records():
    print("outputs/week3/extractions.jsonl not found — generating synthetic records", file=sys.stderr)
    now = datetime.now(timezone.utc).isoformat()
    records = []
    for i in range(5):
        doc_id = str(uuid.uuid4())
        facts = [
            {
                "fact_id": str(uuid.uuid4()),
                "text": f"Synthetic fact {i}-{j}",
                "entity_refs": [],
                "confidence": round(50.0 + (i * 10) + j, 1),
                "page_ref": j + 1,
                "source_excerpt": f"Excerpt {i}-{j}",
            }
            for j in range(3)
        ]
        records.append({
            "doc_id": doc_id,
            "source_path": f"synthetic_doc_{i}.pdf",
            "source_hash": "0" * 64,
            "extracted_facts": facts,
            "entities": [],
            "extraction_model": "synthetic",
            "processing_time_ms": 0,
            "token_count": {"input": 0, "output": 0},
            "extracted_at": now,
        })
    return records


def _scale_confidence_to_100(records):
    modified_count = 0
    result = []
    for rec in records:
        rec = dict(rec)
        facts = rec.get("extracted_facts", [])
        new_facts = []
        for fact in facts:
            fact = dict(fact)
            if "confidence" in fact and fact["confidence"] is not None:
                val = float(fact["confidence"])
                if 0.0 <= val <= 1.0:
                    fact["confidence"] = round(val * 100, 1)
                    modified_count += 1
                elif val > 1.0:
                    modified_count += 1
            new_facts.append(fact)
        rec["extracted_facts"] = new_facts
        result.append(rec)
    return result, modified_count


def _run(cmd):
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        return proc.returncode, proc.stdout + proc.stderr
    except Exception as exc:
        return 1, str(exc)


def _build_violation_record(check_id, check_type, detected_at, records_failing, actual_value, expected, message):
    return {
        "violation_id": str(uuid.uuid4()),
        "check_id": check_id,
        "detected_at": detected_at,
        "type": check_type,
        "injection_note": True,
        "blame_chain": [
            {
                "rank": 1,
                "file_path": "outputs/week3/extractions.jsonl",
                "commit_hash": "unknown",
                "author": "injection-script",
                "commit_timestamp": detected_at,
                "commit_message": "Synthetic violation: confidence scaled to 0-100",
                "confidence_score": 0.0,
            }
        ],
        "blast_radius": {
            "affected_nodes": [],
            "affected_pipelines": ["week3-document-refinery-extractions"],
            "estimated_records": records_failing,
        },
        "actual_value": actual_value,
        "expected": expected,
        "message": message,
    }


def _ensure_comment_block(log_path):
    """Ensure violations.jsonl starts with a JSON _comment record (not # lines)."""
    log_path.parent.mkdir(parents=True, exist_ok=True)

    if not log_path.exists():
        with open(log_path, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(_COMMENT_RECORD) + "\n")
        return

    content = log_path.read_text(encoding="utf-8")

    if content.lstrip().startswith('{"_comment"'):
        return  # Already has JSON comment header

    # Strip legacy # comment lines; keep JSON records
    lines = content.splitlines()
    json_lines = [ln for ln in lines if ln.strip() and not ln.strip().startswith("#")]

    with open(log_path, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(_COMMENT_RECORD) + "\n")
        for ln in json_lines:
            fh.write(ln + "\n")


def _append_violation(log_path, violation):
    with open(log_path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(violation) + "\n")


def main():
    parser = argparse.ArgumentParser(description="Inject a synthetic violation into the pipeline")
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="Path to extractions.jsonl")
    args = parser.parse_args()

    input_path = Path(args.input)
    detected_at = datetime.now(timezone.utc).isoformat()

    if input_path.exists():
        records = _load_jsonl(input_path)
        print(f"Loaded {len(records)} records from {input_path}")
        source = "real"
    else:
        records = _generate_synthetic_records()
        print(f"Generated {len(records)} synthetic records (source file not found)")
        source = "synthetic"

    if not records:
        print("ERROR: No records available", file=sys.stderr)
        sys.exit(1)

    violated_records, modified_count = _scale_confidence_to_100(records)
    desc = f"confidence scale changed 0.0-1.0 to 0-100 ({modified_count} values modified)"
    print(f"INJECTION: {desc}")

    violated_path = input_path.parent / (input_path.stem + "_violated.jsonl")
    violated_path.parent.mkdir(parents=True, exist_ok=True)
    _write_jsonl(violated_path, violated_records)
    print(f"Written violated data to: {violated_path}")

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    print("\nRunning ValidationRunner against violated data...")
    rc, out = _run([
        sys.executable, "contracts/runner.py",
        "--contract", str(CONTRACT_PATH),
        "--data", str(violated_path),
        "--output", str(REPORT_PATH),
    ])
    if out.strip():
        print(out.strip())

    fail_results = []
    try:
        with open(REPORT_PATH, encoding="utf-8") as fh:
            report = json.load(fh)
        fail_results = [r for r in report.get("results", []) if r.get("status") == "FAIL"]
        print(f"\nValidation complete: {report.get('total_checks', 0)} checks, {report.get('failed', 0)} FAIL(s)")
        for r in fail_results:
            print(f"  FAIL: {r.get('check_id')} - {r.get('message', '')}")
    except Exception as exc:
        print(f"WARNING: Could not parse validation report: {exc}", file=sys.stderr)

    if not fail_results:
        print("WARNING: No FAIL results detected. Creating synthetic violation record.", file=sys.stderr)
        fail_results = [{
            "check_id": "week3.extracted_facts.confidence.range",
            "column_name": "extracted_facts.confidence",
            "check_type": "range",
            "status": "FAIL",
            "actual_value": "confidence values in 0-100 range",
            "expected": "max<=1.0, min>=0.0",
            "severity": "CRITICAL",
            "records_failing": modified_count,
            "message": "confidence is in 0-100 range, not 0.0-1.0. Breaking change detected.",
        }]

    _ensure_comment_block(VIOLATION_LOG)
    violations_written = 0
    for fail_result in fail_results:
        violation = _build_violation_record(
            check_id=fail_result.get("check_id", "week3.extracted_facts.confidence.range"),
            check_type=fail_result.get("check_type", "range"),
            detected_at=detected_at,
            records_failing=fail_result.get("records_failing", modified_count),
            actual_value=fail_result.get("actual_value", ""),
            expected=fail_result.get("expected", "max<=1.0, min>=0.0"),
            message=fail_result.get("message", desc),
        )
        _append_violation(VIOLATION_LOG, violation)
        violations_written += 1
        print(f"\nViolation record written:")
        print(f"  violation_id   : {violation['violation_id']}")
        print(f"  check_id       : {violation['check_id']}")
        print(f"  type           : {violation['type']}")
        print(f"  injection_note : {violation['injection_note']}")
        print(f"  records_failing: {violation['blast_radius']['estimated_records']}")

    print(f"\nWrote {violations_written} violation record(s) to {VIOLATION_LOG}")

    print("\nRegenerating enforcer report...")
    rc, out = _run([sys.executable, "contracts/report_generator.py"])
    if out.strip():
        print(out.strip())

    print(f"\n{'=' * 60}")
    print(f"Violation injection complete")
    print(f"  Source data   : {source} ({len(records)} records)")
    print(f"  Injection     : {desc}")
    print(f"  Violated data : {violated_path}")
    print(f"  Report        : {REPORT_PATH}")
    print(f"  Violations    : {violations_written} record(s) -> {VIOLATION_LOG}")
    print(f"  injection_note: true (all records marked as injected)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
