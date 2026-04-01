"""
outputs/migrate/migrate_week1.py
=================================
Generates synthetic intent_record JSONL from Week 1 Roo-Code
orchestration logs or produces a minimal fixture dataset.

Week 1 is a TypeScript IDE fork with no JSONL output. This script
either parses available Roo-Code logs or generates a synthetic
dataset that satisfies the canonical intent_record schema.

DEVIATIONS & SENTINEL VALUES:
  - intent_id             → generated UUIDv4 (no source field)
  - code_refs[].confidence→ sentinel 0.85 for synthetic records (WARNING logged)
  - code_refs[].line_end  → sentinel line_start + 25 for synthetic records (WARNING logged)
  - code_refs[].symbol    → sentinel "<unknown>" for synthetic records (WARNING logged)
  - created_at            → file mtime of log file, or datetime.utcnow() (WARNING logged)
  - description           → derived from log filename or synthetic template (WARNING logged)

Usage:
    python outputs/migrate/migrate_week1.py \\
        --roo-logs path/to/week1/Roo-Code/.orchestration/ \\
        --output outputs/week1/intent_records.jsonl
"""
import argparse
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path


GOVERNANCE_TAGS = ["auth", "pii", "billing", "logging", "validation"]

SYNTHETIC_RECORDS = [
    {
        "description": "Authenticate user via OAuth2 token exchange",
        "governance_tags": ["auth"],
        "file": "src/auth/oauth.ts",
        "line_start": 12,
        "symbol": "exchangeToken",
    },
    {
        "description": "Validate JWT signature and expiry",
        "governance_tags": ["auth"],
        "file": "src/auth/jwt.ts",
        "line_start": 34,
        "symbol": "verifyJwt",
    },
    {
        "description": "Redact PII fields before logging user events",
        "governance_tags": ["pii", "logging"],
        "file": "src/middleware/pii_redactor.ts",
        "line_start": 8,
        "symbol": "redactPii",
    },
    {
        "description": "Mask credit card number in payment payload",
        "governance_tags": ["pii", "billing"],
        "file": "src/billing/payment.ts",
        "line_start": 55,
        "symbol": "maskCardNumber",
    },
    {
        "description": "Process subscription renewal charge",
        "governance_tags": ["billing"],
        "file": "src/billing/subscription.ts",
        "line_start": 101,
        "symbol": "renewSubscription",
    },
    {
        "description": "Emit structured audit log on billing event",
        "governance_tags": ["billing", "logging"],
        "file": "src/billing/audit.ts",
        "line_start": 22,
        "symbol": "emitBillingAudit",
    },
    {
        "description": "Write request trace to structured log sink",
        "governance_tags": ["logging"],
        "file": "src/logging/tracer.ts",
        "line_start": 67,
        "symbol": "traceRequest",
    },
    {
        "description": "Rotate log files older than retention window",
        "governance_tags": ["logging"],
        "file": "src/logging/rotation.ts",
        "line_start": 14,
        "symbol": "rotateLogs",
    },
    {
        "description": "Validate email address format on registration",
        "governance_tags": ["validation"],
        "file": "src/validation/email.ts",
        "line_start": 5,
        "symbol": "validateEmail",
    },
    {
        "description": "Enforce schema constraints on incoming API payload",
        "governance_tags": ["validation"],
        "file": "src/validation/schema.ts",
        "line_start": 30,
        "symbol": "enforceSchema",
    },
    {
        "description": "Sanitize user-supplied HTML to prevent XSS",
        "governance_tags": ["validation", "auth"],
        "file": "src/validation/sanitizer.ts",
        "line_start": 18,
        "symbol": "sanitizeHtml",
    },
    {
        "description": "Check RBAC permissions before resource access",
        "governance_tags": ["auth"],
        "file": "src/auth/rbac.ts",
        "line_start": 44,
        "symbol": "checkPermission",
    },
    {
        "description": "Anonymise PII in analytics export pipeline",
        "governance_tags": ["pii"],
        "file": "src/analytics/anonymiser.ts",
        "line_start": 77,
        "symbol": "anonymisePii",
    },
    {
        "description": "Validate invoice line-item totals before submission",
        "governance_tags": ["billing", "validation"],
        "file": "src/billing/invoice.ts",
        "line_start": 90,
        "symbol": "validateInvoice",
    },
    {
        "description": "Log authentication failures with redacted credentials",
        "governance_tags": ["auth", "logging", "pii"],
        "file": "src/auth/failure_logger.ts",
        "line_start": 3,
        "symbol": "logAuthFailure",
    },
]


def _warn(msg: str) -> None:
    print(f"WARNING: {msg}", file=sys.stderr)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_synthetic_record(template: dict, created_at: str) -> dict:
    line_start = template["line_start"]
    _warn(f"code_refs[].confidence using sentinel 0.85 for '{template['symbol']}'")
    _warn(f"code_refs[].line_end using sentinel line_start+25 for '{template['symbol']}'")
    return {
        "intent_id": str(uuid.uuid4()),
        "description": template["description"],
        "code_refs": [
            {
                "file": template["file"],
                "line_start": line_start,
                "line_end": line_start + 25,
                "symbol": template["symbol"],
                "confidence": 0.85,
            }
        ],
        "governance_tags": template["governance_tags"],
        "created_at": created_at,
    }


def _parse_roo_logs(roo_logs_path: Path) -> list[dict]:
    """Try to parse .orchestration/ JSON files for real data."""
    records = []
    orch_dir = roo_logs_path
    if not orch_dir.exists():
        return records

    json_files = list(orch_dir.rglob("*.json"))
    if not json_files:
        return records

    for jf in json_files:
        try:
            data = json.loads(jf.read_text(encoding="utf-8"))
            mtime = datetime.fromtimestamp(jf.stat().st_mtime, tz=timezone.utc).isoformat()

            # Roo-Code orchestration logs may have various shapes; try common keys
            tasks = []
            if isinstance(data, list):
                tasks = data
            elif isinstance(data, dict):
                tasks = data.get("tasks", data.get("steps", data.get("actions", [data])))

            for item in tasks:
                if not isinstance(item, dict):
                    continue

                description = (
                    item.get("description")
                    or item.get("task")
                    or item.get("instruction")
                    or item.get("content")
                    or item.get("message")
                )
                if not description:
                    _warn(f"description using sentinel from filename for log entry in {jf.name}")
                    description = f"Task from {jf.stem}"

                # Try to extract file references
                file_ref = item.get("file") or item.get("path") or item.get("filePath") or "unknown"
                line_start = int(item.get("line_start") or item.get("lineStart") or item.get("line") or 1)
                line_end = int(item.get("line_end") or item.get("lineEnd") or (line_start + 25))
                symbol = item.get("symbol") or item.get("function") or item.get("name") or "<unknown>"
                confidence = float(item.get("confidence") or 0.85)

                if file_ref == "unknown":
                    _warn(f"code_refs[].file using sentinel 'unknown' for entry in {jf.name}")
                if symbol == "<unknown>":
                    _warn(f"code_refs[].symbol using sentinel '<unknown>' for entry in {jf.name}")
                if confidence == 0.85:
                    _warn(f"code_refs[].confidence using sentinel 0.85 for entry in {jf.name}")

                # Infer governance tags from description keywords
                tags = []
                desc_lower = description.lower()
                for tag in GOVERNANCE_TAGS:
                    if tag in desc_lower:
                        tags.append(tag)
                if not tags:
                    tags = ["logging"]

                records.append({
                    "intent_id": str(uuid.uuid4()),
                    "description": description,
                    "code_refs": [
                        {
                            "file": file_ref,
                            "line_start": line_start,
                            "line_end": line_end,
                            "symbol": symbol,
                            "confidence": confidence,
                        }
                    ],
                    "governance_tags": tags,
                    "created_at": mtime,
                })
        except Exception as exc:
            print(f"ERROR: Failed to parse {jf}: {exc}", file=sys.stderr)

    return records


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate Week 1 → intent_record JSONL")
    parser.add_argument("--roo-logs", required=False, help="Path to Roo-Code orchestration logs directory")
    parser.add_argument("--output", required=True, help="Output JSONL path")
    args = parser.parse_args()

    try:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        records: list[dict] = []

        if args.roo_logs:
            roo_path = Path(args.roo_logs)
            print(f"INFO: Attempting to parse Roo-Code logs from {roo_path}", file=sys.stderr)
            records = _parse_roo_logs(roo_path)
            if records:
                print(f"INFO: Parsed {len(records)} records from Roo-Code logs", file=sys.stderr)
            else:
                print("INFO: No records found in Roo-Code logs; falling back to synthetic fixtures", file=sys.stderr)

        if not records:
            print("INFO: Generating synthetic fixture dataset (15 records)", file=sys.stderr)
            now = _now_iso()
            _warn("created_at using datetime.utcnow() sentinel for all synthetic records")
            for template in SYNTHETIC_RECORDS:
                records.append(_make_synthetic_record(template, now))

        with output_path.open("w", encoding="utf-8") as fh:
            for rec in records:
                fh.write(json.dumps(rec) + "\n")

        print(f"INFO: Wrote {len(records)} intent_records to {output_path}", file=sys.stderr)

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
