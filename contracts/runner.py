"""
contracts/runner.py — ValidationRunner
=======================================
Executes every clause in a contract YAML against a data snapshot
and produces a structured PASS/FAIL/WARN/ERROR JSON report.

Usage:
    python contracts/runner.py \
        --contract generated_contracts/week3_extractions.yaml \
        --data outputs/week3/extractions.jsonl \
        --output validation_reports/week3_run.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

# ---------------------------------------------------------------------------
# Re-use flattening logic from generator (or duplicate for independence)
# ---------------------------------------------------------------------------
try:
    from contracts.generator import _flatten_to_dataframe, _load_jsonl
except ImportError:
    try:
        from generator import _flatten_to_dataframe, _load_jsonl
    except ImportError:
        # Inline fallback so runner is fully self-contained
        def _load_jsonl(path):  # type: ignore[misc]
            records = []
            try:
                with open(path, "r", encoding="utf-8") as fh:
                    for lineno, line in enumerate(fh, 1):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            records.append(json.loads(line))
                        except json.JSONDecodeError:
                            pass
            except OSError:
                pass
            return records

        def _flatten_to_dataframe(records):  # type: ignore[misc]
            if not records:
                return pd.DataFrame()
            flat_rows = []
            for record in records:
                base = {k: v for k, v in record.items() if not isinstance(v, (list, dict))}
                arrays = {k: v for k, v in record.items() if isinstance(v, list)}
                if not arrays:
                    flat_rows.append(base)
                else:
                    for array_key, items in arrays.items():
                        for item in (items or [{}]):
                            row = {**base}
                            if isinstance(item, dict):
                                for sub_key, sub_val in item.items():
                                    row[f"{array_key}.{sub_key}"] = sub_val
                            else:
                                row[array_key] = item
                            flat_rows.append(row)
            return pd.DataFrame(flat_rows) if flat_rows else pd.DataFrame()


logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
UUID_PATTERN = re.compile(r"^[0-9a-f-]{36}$", re.IGNORECASE)
GIT_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$", re.IGNORECASE)

BASELINES_PATH = Path("schema_snapshots") / "baselines.json"

# ---------------------------------------------------------------------------
# Never-crash wrapper
# ---------------------------------------------------------------------------

def _run_check_safe(check_fn, *args, **kwargs) -> dict:
    try:
        return check_fn(*args, **kwargs)
    except KeyError as e:
        return {
            "status": "ERROR",
            "message": f"Column not found: {e}",
            "severity": "CRITICAL",
            "records_failing": 0,
            "sample_failing": [],
        }
    except Exception as e:
        return {
            "status": "ERROR",
            "message": f"{type(e).__name__}: {e}",
            "severity": "CRITICAL",
            "records_failing": 0,
            "sample_failing": [],
        }

# ---------------------------------------------------------------------------
# Contract loading
# ---------------------------------------------------------------------------

def _load_contract(contract_path: str | Path) -> dict:
    """Load contract YAML; return empty dict on failure."""
    try:
        with open(contract_path, "r", encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}
    except Exception as exc:
        log.error("Cannot load contract %s: %s", contract_path, exc)
        return {}


def _load_data(data_path: str | Path) -> pd.DataFrame:
    """Load JSONL and flatten; return empty DataFrame on failure."""
    try:
        records = _load_jsonl(data_path)
        return _flatten_to_dataframe(records)
    except Exception as exc:
        log.error("Cannot load data %s: %s", data_path, exc)
        return pd.DataFrame()


def _sha256_file(path: str | Path) -> str:
    """Compute SHA-256 of a file; return 'unknown' on failure."""
    try:
        h = hashlib.sha256()
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return "unknown"


# ---------------------------------------------------------------------------
# Structural checks
# ---------------------------------------------------------------------------

def _check_required(col: str, clause: dict, df: pd.DataFrame) -> dict:
    """FAIL+CRITICAL if required column has nulls."""
    if col not in df.columns:
        return {
            "status": "ERROR",
            "message": f"Column '{col}' not found in data",
            "severity": "CRITICAL",
            "records_failing": 0,
            "sample_failing": [],
        }
    null_count = int(df[col].isna().sum())
    if null_count > 0:
        return {
            "status": "FAIL",
            "severity": "CRITICAL",
            "actual_value": f"null_count={null_count}",
            "expected": "null_count=0",
            "records_failing": null_count,
            "sample_failing": [],
            "message": f"Required column '{col}' has {null_count} null values.",
        }
    return {"status": "PASS", "records_failing": 0, "sample_failing": []}


def _check_type(col: str, clause: dict, df: pd.DataFrame) -> dict:
    """FAIL+CRITICAL if dtype doesn't match declared type."""
    if col not in df.columns:
        return {
            "status": "ERROR",
            "message": f"Column '{col}' not found in data",
            "severity": "CRITICAL",
            "records_failing": 0,
            "sample_failing": [],
        }
    declared = clause.get("type", "string")
    actual_dtype = str(df[col].dtype)
    numeric_types = {"number", "integer"}
    is_numeric_declared = declared in numeric_types
    is_numeric_actual = pd.api.types.is_numeric_dtype(df[col])

    if is_numeric_declared and not is_numeric_actual:
        return {
            "status": "FAIL",
            "severity": "CRITICAL",
            "actual_value": actual_dtype,
            "expected": declared,
            "records_failing": len(df),
            "sample_failing": [],
            "message": f"Column '{col}' dtype '{actual_dtype}' does not match declared type '{declared}'.",
        }
    if declared == "boolean" and actual_dtype not in ("bool", "boolean"):
        return {
            "status": "FAIL",
            "severity": "CRITICAL",
            "actual_value": actual_dtype,
            "expected": declared,
            "records_failing": len(df),
            "sample_failing": [],
            "message": f"Column '{col}' dtype '{actual_dtype}' does not match declared type '{declared}'.",
        }
    return {"status": "PASS", "records_failing": 0, "sample_failing": []}


def _check_range(col: str, clause: dict, df: pd.DataFrame) -> dict:
    """FAIL+CRITICAL if numeric min/max violates declared minimum/maximum."""
    if col not in df.columns:
        return {
            "status": "ERROR",
            "message": f"Column '{col}' not found in data",
            "severity": "CRITICAL",
            "records_failing": 0,
            "sample_failing": [],
        }
    minimum = clause.get("minimum")
    maximum = clause.get("maximum")
    numeric = pd.to_numeric(df[col], errors="coerce").dropna()
    if len(numeric) == 0:
        return {"status": "PASS", "records_failing": 0, "sample_failing": [], "message": "No numeric values to check."}

    actual_min = float(numeric.min())
    actual_max = float(numeric.max())
    actual_mean = float(numeric.mean())

    violations = []
    if minimum is not None and actual_min < minimum:
        violations.append(f"min={actual_min} < minimum={minimum}")
    if maximum is not None and actual_max > maximum:
        violations.append(f"max={actual_max} > maximum={maximum}")

    if violations:
        expected_parts = []
        if minimum is not None:
            expected_parts.append(f"min>={minimum}")
        if maximum is not None:
            expected_parts.append(f"max<={maximum}")
        return {
            "status": "FAIL",
            "severity": "CRITICAL",
            "actual_value": f"max={actual_max}, mean={round(actual_mean, 2)}",
            "expected": ", ".join(expected_parts),
            "records_failing": int(
                ((pd.to_numeric(df[col], errors="coerce") < (minimum or float("-inf"))) |
                 (pd.to_numeric(df[col], errors="coerce") > (maximum or float("inf")))).sum()
            ),
            "sample_failing": [],
            "message": "; ".join(violations),
        }
    return {
        "status": "PASS",
        "actual_value": f"min={actual_min}, max={actual_max}",
        "records_failing": 0,
        "sample_failing": [],
    }


def _check_enum(col: str, clause: dict, df: pd.DataFrame) -> dict:
    """FAIL if values outside declared enum list."""
    if col not in df.columns:
        return {
            "status": "ERROR",
            "message": f"Column '{col}' not found in data",
            "severity": "CRITICAL",
            "records_failing": 0,
            "sample_failing": [],
        }
    allowed = set(str(v) for v in clause.get("enum", []))
    non_null = df[col].dropna()
    bad = non_null[~non_null.astype(str).isin(allowed)]
    if len(bad) > 0:
        return {
            "status": "FAIL",
            "severity": "HIGH",
            "actual_value": f"found {bad.nunique()} non-conforming distinct values",
            "expected": f"enum={sorted(allowed)}",
            "records_failing": int(len(bad)),
            "sample_failing": list(bad.unique()[:5]),
            "message": f"Column '{col}' has {len(bad)} values outside enum {sorted(allowed)}.",
        }
    return {"status": "PASS", "records_failing": 0, "sample_failing": []}


def _check_uuid(col: str, clause: dict, df: pd.DataFrame) -> dict:
    """FAIL if values don't match UUID pattern."""
    if col not in df.columns:
        return {
            "status": "ERROR",
            "message": f"Column '{col}' not found in data",
            "severity": "CRITICAL",
            "records_failing": 0,
            "sample_failing": [],
        }
    non_null = df[col].dropna().astype(str)
    bad = non_null[~non_null.str.match(r"^[0-9a-fA-F-]{36}$")]
    if len(bad) > 0:
        return {
            "status": "FAIL",
            "severity": "HIGH",
            "actual_value": f"{len(bad)} non-UUID values",
            "expected": "format: uuid (^[0-9a-f-]{36}$)",
            "records_failing": int(len(bad)),
            "sample_failing": list(bad.unique()[:5]),
            "message": f"Column '{col}' has {len(bad)} values that don't match UUID pattern.",
        }
    return {"status": "PASS", "records_failing": 0, "sample_failing": []}


def _check_datetime(col: str, clause: dict, df: pd.DataFrame) -> dict:
    """FAIL if values don't parse as ISO 8601."""
    if col not in df.columns:
        return {
            "status": "ERROR",
            "message": f"Column '{col}' not found in data",
            "severity": "CRITICAL",
            "records_failing": 0,
            "sample_failing": [],
        }
    non_null = df[col].dropna().astype(str)
    bad_vals = []
    for val in non_null:
        try:
            datetime.fromisoformat(val.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            bad_vals.append(val)
    if bad_vals:
        return {
            "status": "FAIL",
            "severity": "HIGH",
            "actual_value": f"{len(bad_vals)} non-ISO-8601 values",
            "expected": "format: date-time (ISO 8601)",
            "records_failing": len(bad_vals),
            "sample_failing": bad_vals[:5],
            "message": f"Column '{col}' has {len(bad_vals)} values that don't parse as ISO 8601.",
        }
    return {"status": "PASS", "records_failing": 0, "sample_failing": []}


# ---------------------------------------------------------------------------
# Statistical drift checks
# ---------------------------------------------------------------------------

def _load_baselines() -> dict:
    """Load baselines.json; return empty dict if absent."""
    try:
        with open(BASELINES_PATH, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {}


def _write_baselines(contract_id: str, df: pd.DataFrame) -> None:
    """Write mean+stddev per numeric column to schema_snapshots/baselines.json."""
    try:
        BASELINES_PATH.parent.mkdir(parents=True, exist_ok=True)
        numeric_cols = df.select_dtypes(include="number").columns
        columns = {}
        for col in numeric_cols:
            series = df[col].dropna()
            if len(series) > 0:
                columns[col] = {
                    "mean": float(series.mean()),
                    "stddev": float(series.std()) if len(series) > 1 else 0.0,
                }
        baselines = {
            "written_at": datetime.utcnow().isoformat(),
            "contract_id": contract_id,
            "columns": columns,
        }
        with open(BASELINES_PATH, "w", encoding="utf-8") as fh:
            json.dump(baselines, fh, indent=2)
        log.info("Baselines written to %s", BASELINES_PATH)
    except Exception as exc:
        log.warning("Could not write baselines: %s", exc)


def _check_statistical_drift(col: str, df: pd.DataFrame, baselines: dict) -> dict | None:
    """Return drift check result or None if no baseline exists."""
    if col not in df.columns:
        return None
    col_baselines = baselines.get("columns", {})
    if col not in col_baselines:
        return None
    b = col_baselines[col]
    numeric = pd.to_numeric(df[col], errors="coerce").dropna()
    if len(numeric) == 0:
        return None
    current_mean = float(numeric.mean())
    current_std = float(numeric.std()) if len(numeric) > 1 else 0.0
    baseline_mean = b.get("mean", 0.0)
    baseline_std = b.get("stddev", 0.0)
    z_score = abs(current_mean - baseline_mean) / max(baseline_std, 1e-9)
    if z_score > 3:
        return {
            "status": "FAIL",
            "severity": "HIGH",
            "z_score": round(z_score, 2),
            "actual_value": f"mean={round(current_mean, 4)}, std={round(current_std, 4)}",
            "expected": f"baseline_mean={round(baseline_mean, 4)}, baseline_std={round(baseline_std, 4)}",
            "records_failing": 0,
            "sample_failing": [],
            "message": f"{col} mean drifted {z_score:.1f} stddev from baseline",
        }
    elif z_score > 2:
        return {
            "status": "WARN",
            "severity": "MEDIUM",
            "z_score": round(z_score, 2),
            "actual_value": f"mean={round(current_mean, 4)}",
            "expected": f"baseline_mean={round(baseline_mean, 4)}",
            "records_failing": 0,
            "sample_failing": [],
            "message": f"{col} mean within warning range ({z_score:.1f} stddev)",
        }
    return {
        "status": "PASS",
        "z_score": round(z_score, 2),
        "records_failing": 0,
        "sample_failing": [],
    }


# ---------------------------------------------------------------------------
# Canonical schema checks (hardcoded per week)
# ---------------------------------------------------------------------------

def _canonical_checks_week3(df: pd.DataFrame) -> list[dict]:
    results = []
    # confidence in [0.0, 1.0]
    conf_col = "extracted_facts.confidence"
    if conf_col in df.columns:
        numeric = pd.to_numeric(df[conf_col], errors="coerce").dropna()
        bad = numeric[(numeric < 0.0) | (numeric > 1.0)]
        if len(bad) > 0:
            results.append({
                "check_id": "week3.extracted_facts.confidence.range",
                "column_name": conf_col,
                "check_type": "range",
                "status": "FAIL",
                "actual_value": f"max={float(numeric.max())}, mean={round(float(numeric.mean()), 2)}",
                "expected": "max<=1.0, min>=0.0",
                "severity": "CRITICAL",
                "records_failing": int(len(bad)),
                "sample_failing": list(bad.head(5)),
                "message": "confidence is in 0-100 range, not 0.0-1.0. Breaking change detected.",
            })
        else:
            results.append({
                "check_id": "week3.extracted_facts.confidence.range",
                "column_name": conf_col,
                "check_type": "range",
                "status": "PASS",
                "records_failing": 0,
                "sample_failing": [],
            })
    # entity.type enum
    entity_type_col = "entities.type"
    if entity_type_col in df.columns:
        allowed = {"PERSON", "ORG", "LOCATION", "DATE", "AMOUNT", "OTHER"}
        non_null = df[entity_type_col].dropna().astype(str)
        bad = non_null[~non_null.isin(allowed)]
        status = "FAIL" if len(bad) > 0 else "PASS"
        results.append({
            "check_id": "week3.entities.type.enum",
            "column_name": entity_type_col,
            "check_type": "enum",
            "status": status,
            "actual_value": f"{len(bad)} non-conforming values" if bad is not None else "0",
            "expected": str(sorted(allowed)),
            "severity": "HIGH" if status == "FAIL" else None,
            "records_failing": int(len(bad)),
            "sample_failing": list(bad.unique()[:5]),
        })
    return results


def _canonical_checks_week5(df: pd.DataFrame) -> list[dict]:
    results = []
    # recorded_at >= occurred_at
    if "recorded_at" in df.columns and "occurred_at" in df.columns:
        try:
            rec = pd.to_datetime(df["recorded_at"], errors="coerce", utc=True)
            occ = pd.to_datetime(df["occurred_at"], errors="coerce", utc=True)
            bad_mask = rec < occ
            bad_count = int(bad_mask.sum())
            results.append({
                "check_id": "week5.recorded_at_gte_occurred_at",
                "column_name": "recorded_at",
                "check_type": "temporal_order",
                "status": "FAIL" if bad_count > 0 else "PASS",
                "actual_value": f"{bad_count} records where recorded_at < occurred_at",
                "expected": "recorded_at >= occurred_at",
                "severity": "CRITICAL" if bad_count > 0 else None,
                "records_failing": bad_count,
                "sample_failing": [],
            })
        except Exception as exc:
            results.append({
                "check_id": "week5.recorded_at_gte_occurred_at",
                "column_name": "recorded_at",
                "check_type": "temporal_order",
                "status": "ERROR",
                "message": str(exc),
                "records_failing": 0,
                "sample_failing": [],
            })
    # sequence_number monotonically increasing per aggregate_id
    if "sequence_number" in df.columns and "aggregate_id" in df.columns:
        try:
            violations = 0
            for agg_id, group in df.groupby("aggregate_id"):
                seq = group["sequence_number"].dropna()
                if len(seq) > 1:
                    diffs = seq.diff().dropna()
                    if (diffs <= 0).any():
                        violations += int((diffs <= 0).sum())
            results.append({
                "check_id": "week5.sequence_number.monotonic",
                "column_name": "sequence_number",
                "check_type": "monotonic",
                "status": "FAIL" if violations > 0 else "PASS",
                "actual_value": f"{violations} non-monotonic steps",
                "expected": "monotonically increasing per aggregate_id",
                "severity": "CRITICAL" if violations > 0 else None,
                "records_failing": violations,
                "sample_failing": [],
            })
        except Exception as exc:
            results.append({
                "check_id": "week5.sequence_number.monotonic",
                "column_name": "sequence_number",
                "check_type": "monotonic",
                "status": "ERROR",
                "message": str(exc),
                "records_failing": 0,
                "sample_failing": [],
            })
    return results


def _canonical_checks_week2(df: pd.DataFrame) -> list[dict]:
    results = []
    # overall_verdict enum
    if "overall_verdict" in df.columns:
        allowed = {"PASS", "FAIL", "WARN"}
        non_null = df["overall_verdict"].dropna().astype(str)
        bad = non_null[~non_null.isin(allowed)]
        results.append({
            "check_id": "week2.overall_verdict.enum",
            "column_name": "overall_verdict",
            "check_type": "enum",
            "status": "FAIL" if len(bad) > 0 else "PASS",
            "actual_value": f"{len(bad)} non-conforming values",
            "expected": str(sorted(allowed)),
            "severity": "CRITICAL" if len(bad) > 0 else None,
            "records_failing": int(len(bad)),
            "sample_failing": list(bad.unique()[:5]),
        })
    # scores.*.score int in [1,5]
    score_cols = [c for c in df.columns if c.startswith("scores.") and c.endswith(".score")]
    for sc in score_cols:
        numeric = pd.to_numeric(df[sc], errors="coerce").dropna()
        bad = numeric[(numeric < 1) | (numeric > 5)]
        results.append({
            "check_id": f"week2.{sc}.range",
            "column_name": sc,
            "check_type": "range",
            "status": "FAIL" if len(bad) > 0 else "PASS",
            "actual_value": f"{len(bad)} out-of-range values",
            "expected": "int in [1, 5]",
            "severity": "HIGH" if len(bad) > 0 else None,
            "records_failing": int(len(bad)),
            "sample_failing": list(bad.head(5)),
        })
    return results


def _canonical_checks_week4(df: pd.DataFrame) -> list[dict]:
    results = []
    if "git_commit" in df.columns:
        non_null = df["git_commit"].dropna().astype(str)
        bad = non_null[~non_null.str.match(r"^[0-9a-f]{40}$")]
        results.append({
            "check_id": "week4.git_commit.pattern",
            "column_name": "git_commit",
            "check_type": "pattern",
            "status": "FAIL" if len(bad) > 0 else "PASS",
            "actual_value": f"{len(bad)} non-conforming values",
            "expected": "^[0-9a-f]{40}$",
            "severity": "HIGH" if len(bad) > 0 else None,
            "records_failing": int(len(bad)),
            "sample_failing": list(bad.unique()[:5]),
        })
    return results


def _run_canonical_checks(contract_id: str, df: pd.DataFrame) -> list[dict]:
    """Run hardcoded canonical checks based on contract_id prefix."""
    cid = contract_id.lower()
    results = []
    if "week3" in cid:
        results.extend(_canonical_checks_week3(df))
    if "week5" in cid:
        results.extend(_canonical_checks_week5(df))
    if "week2" in cid:
        results.extend(_canonical_checks_week2(df))
    if "week4" in cid:
        results.extend(_canonical_checks_week4(df))
    return results


# ---------------------------------------------------------------------------
# Main runner logic
# ---------------------------------------------------------------------------

def _build_check_id(contract_id: str, col: str, check_type: str) -> str:
    safe_col = col.replace("[*]", "").replace(".", "_").strip("_")
    return f"{contract_id}.{safe_col}.{check_type}"


def run_validation(
    contract_path: str | Path,
    data_path: str | Path,
    mode: str = "AUDIT",
) -> tuple[dict, bool]:
    """
    Execute all contract checks against data and return (validation report dict, should_block).
    - AUDIT: should_block is always False.
    - WARN: should_block is always False.
    - ENFORCE: should_block is True if any CRITICAL severity failures exist.
    Never raises — all errors are captured in the report.
    """
    contract_path = Path(contract_path)
    data_path = Path(data_path)

    report_id = str(uuid.uuid4())
    run_timestamp = datetime.now(timezone.utc).isoformat()
    snapshot_id = _sha256_file(data_path)

    # Load contract
    contract_doc = _load_contract(contract_path)
    contract_id = contract_doc.get("id", contract_path.stem)
    schema = contract_doc.get("schema", {})

    # Load data
    df = _load_data(data_path)

    results: list[dict] = []
    baseline_exists = False

    # If data is empty, emit ERROR for all schema fields
    if df.empty:
        for field_path, clause in schema.items():
            results.append({
                "check_id": _build_check_id(contract_id, field_path, "data_load"),
                "column_name": field_path,
                "check_type": "data_load",
                "status": "ERROR",
                "actual_value": "empty dataframe",
                "expected": "non-empty data",
                "severity": "CRITICAL",
                "records_failing": 0,
                "sample_failing": [],
                "message": "Data file is empty or could not be parsed.",
            })
    else:
        # Load baselines for drift checks
        baselines = _load_baselines().get(contract_id, {})
        baseline_exists = bool(baselines.get("columns"))

        for field_path, clause in schema.items():
            if not isinstance(clause, dict):
                continue

            check_type_list = []

            # required check
            if clause.get("required", False):
                check_type_list.append(("required", _check_required))

            # type check
            if "type" in clause:
                check_type_list.append(("type", _check_type))

            # range check
            if "minimum" in clause or "maximum" in clause:
                check_type_list.append(("range", _check_range))

            # enum check
            if "enum" in clause:
                check_type_list.append(("enum", _check_enum))

            # uuid check
            if clause.get("format") == "uuid":
                check_type_list.append(("uuid", _check_uuid))

            # datetime check
            if clause.get("format") == "date-time":
                check_type_list.append(("datetime", _check_datetime))

            for check_type, check_fn in check_type_list:
                raw = _run_check_safe(check_fn, field_path, clause, df)
                result = {
                    "check_id": _build_check_id(contract_id, field_path, check_type),
                    "column_name": field_path,
                    "check_type": check_type,
                    "status": raw.get("status", "ERROR"),
                    "actual_value": raw.get("actual_value", ""),
                    "expected": raw.get("expected", ""),
                    "severity": raw.get("severity"),
                    "records_failing": raw.get("records_failing", 0),
                    "sample_failing": raw.get("sample_failing", []),
                    "message": raw.get("message", ""),
                }
                results.append(result)

            # statistical drift check (for numeric columns)
            if field_path in df.columns and pd.api.types.is_numeric_dtype(df[field_path]):
                drift = _run_check_safe(_check_statistical_drift, field_path, df, baselines)
                if drift and drift.get("status") is not None:
                    results.append({
                        "check_id": _build_check_id(contract_id, field_path, "statistical_drift"),
                        "column_name": field_path,
                        "check_type": "statistical_drift",
                        "status": drift.get("status", "PASS"),
                        "actual_value": drift.get("actual_value", ""),
                        "expected": drift.get("expected", ""),
                        "severity": drift.get("severity"),
                        "records_failing": drift.get("records_failing", 0),
                        "sample_failing": drift.get("sample_failing", []),
                        "message": drift.get("message", ""),
                        "z_score": drift.get("z_score"),
                    })

        # Canonical schema checks
        canonical = _run_canonical_checks(contract_id, df)
        for c in canonical:
            if "check_id" not in c:
                c["check_id"] = _build_check_id(contract_id, c.get("column_name", "unknown"), c.get("check_type", "canonical"))
            results.append(c)

    # Tally counts
    passed = sum(1 for r in results if r.get("status") == "PASS")
    failed = sum(1 for r in results if r.get("status") == "FAIL")
    warned = sum(1 for r in results if r.get("status") == "WARN")
    errored = sum(1 for r in results if r.get("status") == "ERROR")

    # Write baselines on first successful run
    if not baseline_exists and errored == 0 and failed == 0:
         # Runner currently doesn't write back to the shared baselines.json
         # because the Generator now handles it. But we keep it as a fallback.
        pass

    report = {
        "report_id": report_id,
        "contract_id": contract_id,
        "snapshot_id": snapshot_id,
        "run_timestamp": run_timestamp,
        "total_checks": len(results),
        "passed": passed,
        "failed": failed,
        "warned": warned,
        "errored": errored,
        "results": results,
    }

    # Blocking logic
    should_block = False
    if mode == "ENFORCE":
        # Block if any FAIL or ERROR has CRITICAL severity
        critical_issues = [
            r for r in results 
            if r.get("status") in ("FAIL", "ERROR") and r.get("severity") == "CRITICAL"
        ]
        if critical_issues:
            should_block = True
            log.error("ENFORCEMENT FAILURE: %d critical violations found", len(critical_issues))

    return report, should_block


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="ValidationRunner")
    parser.add_argument("--contract", required=True, help="Path to contract YAML")
    parser.add_argument("--data", required=True, help="Path to data JSONL")
    parser.add_argument("--output", required=True, help="Path to output report JSON")
    parser.add_argument(
        "--mode", 
        choices=["AUDIT", "WARN", "ENFORCE"], 
        default="AUDIT",
        help="Semantics: AUDIT (report only), WARN (log failures), ENFORCE (exit 1 on CRITICAL)"
    )
    args = parser.parse_args()

    try:
        report, should_block = run_validation(args.contract, args.data, mode=args.mode)
    except Exception as exc:
        log.error("Fatal error in run_validation: %s", exc)
        should_block = True
        report = {
            "report_id": str(uuid.uuid4()),
            "contract_id": "unknown",
            "snapshot_id": "unknown",
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
            "total_checks": 0,
            "passed": 0,
            "failed": 0,
            "warned": 0,
            "errored": 1,
            "results": [
                {
                    "check_id": "runner.fatal_error",
                    "column_name": "",
                    "check_type": "fatal",
                    "status": "ERROR",
                    "message": f"{type(exc).__name__}: {exc}",
                    "severity": "CRITICAL",
                    "records_failing": 0,
                    "sample_failing": [],
                }
            ],
        }

    try:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, default=str)
        log.info("Report written to %s", out_path)
        print(
            f"Validation complete: {report['passed']} passed, "
            f"{report['failed']} failed, {report['warned']} warned, "
            f"{report['errored']} errored",
            file=sys.stderr,
        )

        if should_block:
            log.error("Blocking execution due to contract violations (mode=%s)", args.mode)
            sys.exit(1)

    except Exception as exc:
        log.error("Could not write report to %s: %s", args.output, exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
