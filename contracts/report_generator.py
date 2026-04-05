"""
contracts/report_generator.py — ReportGenerator
================================================
Reads validation_reports/ and violation_log/violations.jsonl and
produces enforcer_report/report_data.json with a Data Health Score,
plain-language violation summaries, and prioritised recommendations.

Usage:
    python contracts/report_generator.py
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Well-known paths
# ---------------------------------------------------------------------------

VALIDATION_REPORTS_DIR = Path("validation_reports")
VIOLATION_LOG_PATH = Path("violation_log") / "violations.jsonl"
AI_EXTENSIONS_PATH = Path("validation_reports") / "ai_extensions.json"
OUTPUT_PATH = Path("enforcer_report") / "report_data.json"


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def _load_json(path: Path) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as exc:
        log.warning("Cannot load JSON %s: %s", path, exc)
        return {}


def _load_jsonl(path: Path) -> list[dict]:
    records: list[dict] = []
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError as exc:
                    log.warning("Skipping malformed line %d in %s: %s", lineno, path, exc)
    except OSError as exc:
        log.warning("Cannot open %s: %s", path, exc)
    return records


def _load_all_validation_reports() -> list[dict]:
    """Load all JSON files from validation_reports/ (skip ai_extensions.json and schema_evolution)."""
    reports = []
    if not VALIDATION_REPORTS_DIR.exists():
        return reports
    for json_file in sorted(VALIDATION_REPORTS_DIR.glob("*.json")):
        # Skip ai_extensions and schema_evolution — they have different schemas
        name = json_file.name
        if name in ("ai_extensions.json",) or name.startswith("schema_evolution"):
            continue
        data = _load_json(json_file)
        if data and "results" in data:
            reports.append(data)
    return reports


# ---------------------------------------------------------------------------
# Health score
# ---------------------------------------------------------------------------

def compute_health_score(validation_reports: list[dict]) -> int:
    deductions = {"CRITICAL": 25, "HIGH": 10, "MEDIUM": 5, "LOW": 1}
    score = 100
    for report in validation_reports:
        for result in report.get("results", []):
            if result.get("status") in ("FAIL", "ERROR"):
                severity = result.get("severity") or "LOW"
                score -= deductions.get(severity, 1)
    return max(0, min(100, score))


# ---------------------------------------------------------------------------
# Plain language violation
# ---------------------------------------------------------------------------

def plain_language_violation(result: dict) -> dict:
    check_id = result.get("check_id", "")
    system = check_id.split(".")[0] if "." in check_id else "unknown"
    field = result.get("column_name", "unknown field")
    severity = result.get("severity", "LOW")
    records = result.get("records_failing", 0)
    return {
        "system": system,
        "field": field,
        "severity": severity,
        "impact": (
            f"The {field} field in {system} failed its {result.get('check_type', 'contract')} check. "
            f"Expected {result.get('expected', 'valid values')} but found "
            f"{result.get('actual_value', 'invalid values')}. "
            f"This affects {records} records."
        ),
    }


# ---------------------------------------------------------------------------
# Recommendations
# ---------------------------------------------------------------------------

_RECOMMENDATION_TEMPLATES = {
    "range": "Update {system} to output {field} within the declared range.",
    "required": "Ensure {system} always populates the required field {field}.",
    "enum": "Update {system} to only emit declared enum values for {field}.",
    "type": "Fix {system} to output {field} as the correct data type.",
    "uuid": "Ensure {system} generates valid UUIDs for {field}.",
    "datetime": "Ensure {system} outputs {field} in ISO 8601 format.",
    "statistical_drift": "Investigate {system} for data distribution shift in {field}.",
}


def _generate_recommendations(fail_results: list[dict]) -> list[str]:
    seen: set[str] = set()
    recs: list[str] = []
    for result in fail_results:
        check_id = result.get("check_id", "")
        system = check_id.split(".")[0] if "." in check_id else "unknown"
        field = result.get("column_name", "unknown field")
        check_type = result.get("check_type", "contract")
        
        # Enhanced with file paths and clauses
        contract_file = f"contracts/{system}.yaml"
        template = _RECOMMENDATION_TEMPLATES.get(check_type, "Review {system} contract for {field}.")
        rec = template.format(system=system, field=field)
        detailed_rec = f"{rec} (Action: Check {contract_file} for '{field}' {check_type} clause)"
        
        if detailed_rec not in seen:
            seen.add(detailed_rec)
            recs.append(detailed_rec)
    return recs


# ---------------------------------------------------------------------------
# Severity tally
# ---------------------------------------------------------------------------

def _tally_by_severity(fail_results: list[dict]) -> dict[str, int]:
    tally: dict[str, int] = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
    for result in fail_results:
        sev = result.get("severity") or "LOW"
        if sev in tally:
            tally[sev] += 1
        else:
            tally["LOW"] += 1
    return tally


# ---------------------------------------------------------------------------
# Period detection
# ---------------------------------------------------------------------------

def _detect_period(reports: list[dict]) -> str:
    timestamps = []
    for r in reports:
        ts = r.get("run_timestamp")
        if ts:
            try:
                timestamps.append(datetime.fromisoformat(ts.replace("Z", "+00:00")))
            except Exception:
                pass
    if not timestamps:
        today = datetime.now(timezone.utc).date().isoformat()
        return f"{today} to {today}"
    earliest = min(timestamps).date().isoformat()
    latest = max(timestamps).date().isoformat()
    return f"{earliest} to {latest}"


# ---------------------------------------------------------------------------
# AI risk assessment
# ---------------------------------------------------------------------------

def _build_ai_risk_assessment(ai_data: dict) -> dict:
    drift = ai_data.get("embedding_drift", {})
    output_rate = ai_data.get("output_violation_rate", {})
    return {
        "embedding_drift_status": drift.get("status", "UNKNOWN"),
        "embedding_drift_score": drift.get("drift_score", 0.0),
        "llm_violation_rate": output_rate.get("violation_rate", 0.0),
        "llm_violation_trend": output_rate.get("trend", "unknown"),
    }


# ---------------------------------------------------------------------------
# Health narrative
# ---------------------------------------------------------------------------

def _health_narrative(score: int, tally: dict[str, int]) -> str:
    critical = tally.get("CRITICAL", 0)
    high = tally.get("HIGH", 0)
    if critical > 0:
        return (
            f"Score of {score}/100. {critical} critical issue(s) require immediate action."
        )
    if high > 0:
        return (
            f"Score of {score}/100. {high} high-severity issue(s) should be addressed soon."
        )
    if score >= 90:
        return f"Score of {score}/100. Data contracts are in good health."
    return f"Score of {score}/100. Review flagged issues to improve data quality."


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    try:
        # Load all validation reports
        validation_reports = _load_all_validation_reports()
        log.info("Loaded %d validation reports", len(validation_reports))

        # Load violation log
        violations = _load_jsonl(VIOLATION_LOG_PATH)
        log.info("Loaded %d violation records", len(violations))

        # Load AI extensions report
        ai_data = _load_json(AI_EXTENSIONS_PATH)

        # Collect all FAIL/ERROR results
        all_fail_results: list[dict] = []
        for report in validation_reports:
            for result in report.get("results", []):
                if result.get("status") in ("FAIL", "ERROR"):
                    all_fail_results.append(result)

        # Compute health score
        health_score = compute_health_score(validation_reports)

        # Tally by severity
        severity_tally = _tally_by_severity(all_fail_results)

        # Top 3 violations (sorted by severity weight)
        severity_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3, None: 4}
        sorted_fails = sorted(
            all_fail_results,
            key=lambda r: severity_order.get(r.get("severity"), 4),
        )
        top_violations = [plain_language_violation(r) for r in sorted_fails[:3]]

        # Recommendations
        recommendations = _generate_recommendations(sorted_fails[:10])

        # Period
        period = _detect_period(validation_reports)

        # AI risk assessment
        ai_risk = _build_ai_risk_assessment(ai_data)

        # Health narrative
        narrative = _health_narrative(health_score, severity_tally)

        # Build output
        output: dict[str, Any] = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "period": period,
            "data_health_score": health_score,
            "health_narrative": narrative,
            "top_violations": top_violations,
            "total_violations_by_severity": severity_tally,
            "violation_count": len(all_fail_results),
            "violations_this_week": [plain_language_violation(r) for r in sorted_fails],
            "schema_changes_detected": _load_json(VALIDATION_REPORTS_DIR / "schema_evolution.json").get("total_changes", 0),
            "recommendations": recommendations,
            "ai_risk_assessment": ai_risk,
        }

        # Write output
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_PATH, "w", encoding="utf-8") as fh:
            json.dump(output, fh, indent=2)

        log.info(
            "Enforcer report written to %s (health_score=%d, violations=%d)",
            OUTPUT_PATH, health_score, len(all_fail_results),
        )

    except Exception as exc:
        log.error("Fatal error in ReportGenerator: %s", exc)
        # Write a minimal fallback report so the dashboard never crashes
        try:
            OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
            fallback = {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "period": "unknown",
                "data_health_score": 0,
                "health_narrative": f"Report generation failed: {exc}",
                "top_violations": [],
                "total_violations_by_severity": {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0},
                "violation_count": 0,
                "recommendations": [],
                "ai_risk_assessment": {
                    "embedding_drift_status": "UNKNOWN",
                    "embedding_drift_score": 0.0,
                    "llm_violation_rate": 0.0,
                    "llm_violation_trend": "unknown",
                },
            }
            with open(OUTPUT_PATH, "w", encoding="utf-8") as fh:
                json.dump(fallback, fh, indent=2)
        except Exception:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()
