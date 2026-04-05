"""
contracts/schema_analyzer.py — SchemaEvolutionAnalyzer
=======================================================
Diffs consecutive schema snapshots, classifies each change using the
7-type taxonomy, and generates migration impact reports.

Usage:
    python contracts/schema_analyzer.py \
        --contract-id week3-document-refinery-extractions \
        --since "7 days ago" \
        --output validation_reports/schema_evolution.json
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

import yaml

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Change taxonomy
# ---------------------------------------------------------------------------

class ChangeType(Enum):
    ADD_NULLABLE_COLUMN = "ADD_NULLABLE_COLUMN"       # COMPATIBLE
    ADD_NONNULLABLE_COLUMN = "ADD_NONNULLABLE_COLUMN" # BREAKING
    REMOVE_COLUMN = "REMOVE_COLUMN"                   # BREAKING
    RENAME_COLUMN = "RENAME_COLUMN"                   # BREAKING (detect as remove+add)
    TYPE_CHANGE_WIDENING = "TYPE_CHANGE_WIDENING"     # COMPATIBLE
    TYPE_CHANGE_NARROWING = "TYPE_CHANGE_NARROWING"   # BREAKING
    RANGE_CHANGE = "RANGE_CHANGE"                     # BREAKING
    ENUM_ADD = "ENUM_ADD"                             # COMPATIBLE
    ENUM_REMOVE = "ENUM_REMOVE"                       # BREAKING


BREAKING_TYPES = {
    ChangeType.ADD_NONNULLABLE_COLUMN, ChangeType.REMOVE_COLUMN,
    ChangeType.RENAME_COLUMN, ChangeType.TYPE_CHANGE_NARROWING,
    ChangeType.RANGE_CHANGE, ChangeType.ENUM_REMOVE,
}


# ---------------------------------------------------------------------------
# Snapshot loading
# ---------------------------------------------------------------------------

def _load_snapshots(contract_id: str) -> list[tuple[str, dict]]:
    """Load all snapshots for a contract, sorted by timestamp. Returns [(timestamp, snapshot_dict)]"""
    snapshot_dir = Path("schema_snapshots") / contract_id
    if not snapshot_dir.exists():
        return []
    files = sorted(snapshot_dir.glob("*.yaml"))
    result = []
    for f in files:
        try:
            with open(f) as fh:
                data = yaml.safe_load(fh)
            result.append((f.stem, data))
        except Exception as exc:
            log.warning("Could not load snapshot %s: %s", f, exc)
    return result


def _filter_since(snapshots: list[tuple[str, dict]], since: str) -> list[tuple[str, dict]]:
    """Filter snapshots to those within the 'since' window (best-effort)."""
    # Parse simple patterns like "7 days ago", "14 days ago"
    try:
        import re
        match = re.match(r"(\d+)\s+days?\s+ago", since.strip(), re.IGNORECASE)
        if match:
            days = int(match.group(1))
            now = datetime.now(timezone.utc)
            filtered = []
            for ts, snap in snapshots:
                try:
                    # Timestamp format: 20260401T213311Z
                    snap_dt = datetime.strptime(ts, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
                    if (now - snap_dt).days <= days:
                        filtered.append((ts, snap))
                except Exception:
                    filtered.append((ts, snap))  # include if can't parse
            return filtered if filtered else snapshots
    except Exception:
        pass
    return snapshots


# ---------------------------------------------------------------------------
# Change classification
# ---------------------------------------------------------------------------

def classify_change(field: str, old_clause: dict | None, new_clause: dict | None) -> dict | None:
    if old_clause is None:
        nullable = not new_clause.get("required", False)
        ct = ChangeType.ADD_NULLABLE_COLUMN if nullable else ChangeType.ADD_NONNULLABLE_COLUMN
        return {
            "field": field,
            "change_type": ct.value,
            "compatible": nullable,
            "message": "Add nullable column" if nullable else "Add non-nullable column — coordinate with all producers",
            "severity": "LOW" if nullable else "HIGH",
            "old_value": None,
            "new_value": new_clause,
        }
    if new_clause is None:
        return {
            "field": field,
            "change_type": ChangeType.REMOVE_COLUMN.value,
            "compatible": False,
            "message": "Remove column — deprecation period mandatory",
            "severity": "CRITICAL",
            "old_value": old_clause,
            "new_value": None,
        }
    # Type change
    if old_clause.get("type") != new_clause.get("type"):
        widening = (old_clause.get("type") == "integer" and new_clause.get("type") == "number")
        ct = ChangeType.TYPE_CHANGE_WIDENING if widening else ChangeType.TYPE_CHANGE_NARROWING
        severity = "LOW" if widening else "CRITICAL"
        return {
            "field": field,
            "change_type": ct.value,
            "compatible": widening,
            "message": f"Type change {old_clause.get('type')} → {new_clause.get('type')}",
            "severity": severity,
            "old_value": old_clause.get("type"),
            "new_value": new_clause.get("type"),
        }
    # Range change
    if (old_clause.get("minimum") != new_clause.get("minimum") or
            old_clause.get("maximum") != new_clause.get("maximum")):
        # Detect scale shifts (e.g. 0-1 to 0-100)
        old_max = old_clause.get("maximum", 1) or 1
        new_max = new_clause.get("maximum", 1) or 1
        is_scale_shift = (old_max == 1 and new_max == 100)
        severity = "CRITICAL" if is_scale_shift else "HIGH"
        
        return {
            "field": field,
            "change_type": ChangeType.RANGE_CHANGE.value,
            "compatible": False,
            "message": (
                f"Range change: [{old_clause.get('minimum')},{old_clause.get('maximum')}] → "
                f"[{new_clause.get('minimum')},{new_clause.get('maximum')}]"
            ),
            "severity": severity,
            "old_value": {
                "minimum": old_clause.get("minimum"),
                "maximum": old_clause.get("maximum"),
            },
            "new_value": {
                "minimum": new_clause.get("minimum"),
                "maximum": new_clause.get("maximum"),
            },
        }
    # Enum change
    old_enum = set(old_clause.get("enum") or [])
    new_enum = set(new_clause.get("enum") or [])
    if old_enum != new_enum:
        removed = old_enum - new_enum
        added = new_enum - old_enum
        if removed:
            return {
                "field": field,
                "change_type": ChangeType.ENUM_REMOVE.value,
                "compatible": False,
                "message": f"Enum values removed: {sorted(removed)}",
                "severity": "HIGH",
                "old_value": sorted(old_enum),
                "new_value": sorted(new_enum),
            }
        return {
            "field": field,
            "change_type": ChangeType.ENUM_ADD.value,
            "compatible": True,
            "message": f"Enum values added: {sorted(added)}",
            "severity": "LOW",
            "old_value": sorted(old_enum),
            "new_value": sorted(new_enum),
        }
    return None  # no material change


# ---------------------------------------------------------------------------
# Snapshot diffing
# ---------------------------------------------------------------------------

def diff_snapshots(old: dict, new: dict) -> list[dict]:
    old_schema = old.get("schema", {})
    new_schema = new.get("schema", {})
    changes = []
    for field in set(old_schema) | set(new_schema):
        change = classify_change(field, old_schema.get(field), new_schema.get(field))
        if change:
            changes.append(change)
    return changes


# ---------------------------------------------------------------------------
# Migration impact report
# ---------------------------------------------------------------------------

def _migration_impact_report(
    contract_id: str,
    changes: list[dict],
    old_ts: str,
    new_ts: str,
) -> dict:
    breaking = [c for c in changes if not c["compatible"]]
    compatible = [c for c in changes if c["compatible"]]
    checklist = []
    for c in breaking:
        if c["change_type"] == "REMOVE_COLUMN":
            checklist.append(f"1. Notify all consumers of {c['field']} removal")
            checklist.append(f"2. Add deprecation alias for {c['field']} (minimum 2 sprints)")
            checklist.append(f"3. Get written acknowledgement from each consumer team")
        elif c["change_type"] == "RANGE_CHANGE":
            checklist.append(f"1. Update all consumers reading {c['field']} to handle new range")
            checklist.append(f"2. Re-establish statistical baseline after migration")
            checklist.append(f"3. Run ValidationRunner on migrated data before deploying")
        else:
            checklist.append(f"1. Coordinate with all producers for {c['field']} change")
            checklist.append(f"2. Deploy producers before consumers")
    # Per-consumer failure mode analysis (Mocked using lineage context)
    consumer_risks = []
    for c in breaking:
        # Re-using logic from what we know about subscribers
        consumer_risks.append({
            "field": c["field"],
            "risk_level": "CRITICAL" if c["severity"] == "CRITICAL" else "HIGH",
            "affected_consumers": ["default-consumer", "bi-reporting", "model-training"],
            "failure_mode": f"Consumer will experience {c['change_type']} on {c['field']}"
        })

    return {
        "contract_id": contract_id,
        "analysis_period": f"{old_ts} to {new_ts}",
        "breaking_count": len(breaking),
        "compatible_count": len(compatible),
        "changes": changes,
        "consumer_risks": consumer_risks,
        "migration_checklist": checklist,
        "rollback_plan": f"Revert to snapshot {old_ts} and re-run ContractGenerator",
        "compatibility_verdict": "BREAKING" if breaking else "COMPATIBLE",
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="SchemaEvolutionAnalyzer")
    parser.add_argument("--contract-id", required=True, dest="contract_id", help="Contract identifier")
    parser.add_argument("--since", default="7 days ago", help="Lookback window")
    parser.add_argument("--output", required=True, help="Path to output report JSON")
    args = parser.parse_args()

    try:
        # Load all snapshots for the contract
        all_snapshots = _load_snapshots(args.contract_id)
        if not all_snapshots:
            log.warning("No snapshots found for contract_id=%s", args.contract_id)
            output = {
                "contract_id": args.contract_id,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "message": f"No snapshots found for {args.contract_id}",
                "changes": [],
                "compatibility_verdict": "COMPATIBLE",
            }
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as fh:
                json.dump(output, fh, indent=2)
            return

        # Filter by --since window
        snapshots = _filter_since(all_snapshots, args.since)

        if len(snapshots) < 2:
            log.info("Only %d snapshot(s) in window — nothing to diff", len(snapshots))
            output = {
                "contract_id": args.contract_id,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "snapshots_analyzed": len(snapshots),
                "message": "Need at least 2 snapshots to diff",
                "changes": [],
                "compatibility_verdict": "COMPATIBLE",
            }
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as fh:
                json.dump(output, fh, indent=2)
            return

        # Diff consecutive pairs
        all_changes: list[dict] = []
        pair_reports: list[dict] = []
        for i in range(len(snapshots) - 1):
            old_ts, old_snap = snapshots[i]
            new_ts, new_snap = snapshots[i + 1]
            changes = diff_snapshots(old_snap, new_snap)
            if changes:
                all_changes.extend(changes)
                pair_reports.append({
                    "from_snapshot": old_ts,
                    "to_snapshot": new_ts,
                    "changes": changes,
                })

        # Build output
        has_breaking = any(not c["compatible"] for c in all_changes)
        output: dict[str, Any] = {
            "contract_id": args.contract_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "snapshots_analyzed": len(snapshots),
            "analysis_window": args.since,
            "total_changes": len(all_changes),
            "breaking_changes": sum(1 for c in all_changes if not c["compatible"]),
            "compatible_changes": sum(1 for c in all_changes if c["compatible"]),
            "compatibility_verdict": "BREAKING" if has_breaking else "COMPATIBLE",
            "pair_diffs": pair_reports,
        }

        # If breaking changes, add migration impact report
        if has_breaking and snapshots:
            first_ts = snapshots[0][0]
            last_ts = snapshots[-1][0]
            migration_report = _migration_impact_report(
                args.contract_id, all_changes, first_ts, last_ts
            )
            output["migration_impact"] = migration_report

        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as fh:
            json.dump(output, fh, indent=2)

        log.info(
            "Schema evolution report written to %s (%d changes, verdict=%s)",
            output_path, len(all_changes), output["compatibility_verdict"],
        )

    except Exception as exc:
        log.error("Fatal error in SchemaEvolutionAnalyzer: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
