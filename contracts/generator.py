"""
contracts/generator.py — ContractGenerator
==========================================
Profiles a JSONL file and produces a Bitol-compatible contract YAML
plus a parallel dbt schema.yml.

Usage:
    python contracts/generator.py \
        --source outputs/week3/extractions.jsonl \
        --contract-id week3-document-refinery-extractions \
        --lineage outputs/week4/lineage_snapshots.jsonl \
        --output generated_contracts/
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class ColumnProfile:
    name: str
    dtype: str
    null_fraction: float
    cardinality: int
    sample_values: list
    min: float | None = None
    max: float | None = None
    mean: float | None = None
    p25: float | None = None
    p50: float | None = None
    p75: float | None = None
    p95: float | None = None
    p99: float | None = None
    stddev: float | None = None


@dataclass
class ContractClause:
    field_path: str
    type: str
    required: bool
    format: str | None = None
    pattern: str | None = None
    minimum: float | None = None
    maximum: float | None = None
    enum: list | None = None
    description: str = ""
    llm_annotation: str | None = None


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def _load_jsonl(path: str | Path) -> list[dict]:
    """Load JSONL, skip malformed lines (log warning), return list of dicts."""
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
        log.error("Cannot open %s: %s", path, exc)
    return records


# ---------------------------------------------------------------------------
# Flattening
# ---------------------------------------------------------------------------

def _flatten_record(record: dict) -> list[dict]:
    """
    Flatten one record to one-or-more flat dicts.

    - Top-level scalars stay as-is.
    - Arrays of dicts: explode to ``{array_key}.{sub_key}`` columns.
    - Arrays of scalars: explode to ``{array_key}`` column.
    - Records with no arrays: return a single flat dict.
    """
    base: dict[str, Any] = {}
    arrays: dict[str, list] = {}

    for key, value in record.items():
        if isinstance(value, list):
            arrays[key] = value
        elif isinstance(value, dict):
            # Flatten one level of nested dicts with dot notation
            for sub_key, sub_val in value.items():
                if not isinstance(sub_val, (list, dict)):
                    base[f"{key}.{sub_key}"] = sub_val
                else:
                    base[key] = str(value)
                    break
            else:
                pass  # all sub-values already added
        else:
            base[key] = value

    if not arrays:
        return [base]

    # Explode each array independently, then cross-join with base
    # Strategy: pick the first array as the primary explode axis;
    # for multiple arrays, we replicate base fields for each item.
    rows: list[dict] = []

    # Collect all array items into rows
    # We explode each array separately and merge base fields
    array_rows: list[dict] = []
    for arr_key, arr_items in arrays.items():
        if not arr_items:
            continue
        for item in arr_items:
            row: dict[str, Any] = {}
            if isinstance(item, dict):
                for sub_key, sub_val in item.items():
                    if not isinstance(sub_val, (list, dict)):
                        row[f"{arr_key}.{sub_key}"] = sub_val
            else:
                row[arr_key] = item
            array_rows.append(row)

    if not array_rows:
        return [base]

    # Merge base into every array row
    for arr_row in array_rows:
        merged = {**base, **arr_row}
        rows.append(merged)

    return rows if rows else [base]


def _flatten_to_dataframe(records: list[dict]) -> pd.DataFrame:
    """Flatten nested arrays to one row per item using dot-notation column names."""
    if not records:
        return pd.DataFrame()

    flat_rows: list[dict] = []
    for record in records:
        try:
            flat_rows.extend(_flatten_record(record))
        except Exception as exc:
            log.warning("Could not flatten record: %s", exc)

    if not flat_rows:
        return pd.DataFrame()

    return pd.DataFrame(flat_rows)


# ---------------------------------------------------------------------------
# Profiling
# ---------------------------------------------------------------------------

def _profile_columns(df: pd.DataFrame) -> list[ColumnProfile]:
    """Profile every column in the DataFrame."""
    profiles: list[ColumnProfile] = []

    for col in df.columns:
        series = df[col]
        total = len(series)
        null_count = series.isna().sum()
        null_fraction = float(null_count / total) if total > 0 else 0.0

        non_null = series.dropna()
        cardinality = int(non_null.nunique())
        sample_values = list(non_null.unique()[:5])

        dtype_str = str(series.dtype)

        profile = ColumnProfile(
            name=col,
            dtype=dtype_str,
            null_fraction=null_fraction,
            cardinality=cardinality,
            sample_values=sample_values,
        )

        # Numeric stats
        numeric_series = pd.to_numeric(non_null, errors="coerce").dropna()
        if len(numeric_series) > 0 and series.dtype in (
            "float64", "int64", "float32", "int32"
        ) or (len(numeric_series) > 0 and pd.api.types.is_numeric_dtype(series)):
            try:
                profile.min = float(numeric_series.min())
                profile.max = float(numeric_series.max())
                profile.mean = float(numeric_series.mean())
                profile.p25 = float(numeric_series.quantile(0.25))
                profile.p50 = float(numeric_series.quantile(0.50))
                profile.p75 = float(numeric_series.quantile(0.75))
                profile.p95 = float(numeric_series.quantile(0.95))
                profile.p99 = float(numeric_series.quantile(0.99))
                profile.stddev = float(numeric_series.std())
            except Exception as exc:
                log.warning("Could not compute numeric stats for column %s: %s", col, exc)

        profiles.append(profile)

    return profiles


# ---------------------------------------------------------------------------
# Clause generation
# ---------------------------------------------------------------------------

_DTYPE_TO_CONTRACT_TYPE = {
    "float64": "number",
    "float32": "number",
    "int64": "integer",
    "int32": "integer",
    "bool": "boolean",
    "object": "string",
}


def _infer_type(dtype: str) -> str:
    return _DTYPE_TO_CONTRACT_TYPE.get(dtype, "string")


def _generate_clauses(profiles: list[ColumnProfile]) -> list[ContractClause]:
    """Generate contract clauses from column profiles."""
    clauses: list[ContractClause] = []

    for p in profiles:
        contract_type = _infer_type(p.dtype)
        required = p.null_fraction == 0.0
        fmt: str | None = None
        pattern: str | None = None
        minimum: float | None = None
        maximum: float | None = None
        enum: list | None = None
        description = ""

        # _id → uuid format + pattern
        if p.name.endswith("_id"):
            fmt = "uuid"
            pattern = r"^[0-9a-f-]{36}$"

        # _at → date-time format
        if p.name.endswith("_at"):
            fmt = "date-time"

        # confidence → range 0.0–1.0
        if "confidence" in p.name and p.dtype in ("float64", "int64", "float32", "int32") or (
            "confidence" in p.name and pd.api.types.is_numeric_dtype(pd.Series(dtype=p.dtype))
        ):
            minimum = 0.0
            maximum = 1.0
            description = (
                "Confidence score. Must remain 0.0-1.0 float. BREAKING if changed to 0-100."
            )

        # low-cardinality string → enum
        if p.dtype == "object" and p.cardinality <= 10:
            # Only emit enum if sample_values covers the full cardinality
            if len(p.sample_values) >= p.cardinality and p.cardinality > 0:
                enum = [str(v) for v in p.sample_values[: p.cardinality]]

        clause = ContractClause(
            field_path=p.name,
            type=contract_type,
            required=required,
            format=fmt,
            pattern=pattern,
            minimum=minimum,
            maximum=maximum,
            enum=enum,
            description=description,
        )
        clauses.append(clause)

    return clauses


# ---------------------------------------------------------------------------
# Lineage injection
# ---------------------------------------------------------------------------

def _inject_lineage(contract_id: str, lineage_path: str | Path | None) -> dict:
    """Load latest lineage snapshot and find downstream nodes."""
    empty = {"upstream": [], "downstream": []}

    if lineage_path is None:
        return empty

    records = _load_jsonl(lineage_path)
    if not records:
        return empty

    # Use the latest record (last line)
    latest = records[-1]

    # Derive the system name from the contract_id (e.g. "week3" from "week3-...")
    system_name = contract_id.split("-")[0] if "-" in contract_id else contract_id

    nodes = latest.get("nodes", [])
    edges = latest.get("edges", [])

    # Find source nodes that match the system name
    source_node_ids = {
        n["node_id"]
        for n in nodes
        if system_name.lower() in str(n.get("node_id", "")).lower()
        or system_name.lower() in str(n.get("label", "")).lower()
    }

    # Find downstream nodes: edges where source contains the system name
    downstream_node_ids: set[str] = set()
    for edge in edges:
        src = str(edge.get("source", ""))
        tgt = str(edge.get("target", ""))
        if any(system_name.lower() in src.lower() for _ in [1]):
            downstream_node_ids.add(tgt)

    downstream = []
    for node_id in downstream_node_ids:
        downstream.append({
            "id": node_id,
            "fields_consumed": ["doc_id", "extracted_facts"],
        })

    return {"upstream": [], "downstream": downstream}


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

def _clause_to_dict(clause: ContractClause) -> dict:
    d: dict[str, Any] = {
        "type": clause.type,
        "required": clause.required,
    }
    if clause.format is not None:
        d["format"] = clause.format
    if clause.pattern is not None:
        d["pattern"] = clause.pattern
    if clause.minimum is not None:
        d["minimum"] = clause.minimum
    if clause.maximum is not None:
        d["maximum"] = clause.maximum
    if clause.enum is not None:
        d["enum"] = clause.enum
    if clause.description:
        d["description"] = clause.description
    if clause.llm_annotation is not None:
        d["llm_annotation"] = clause.llm_annotation
    return d


def _write_bitol_yaml(
    contract_id: str,
    clauses: list[ContractClause],
    lineage: dict,
    output_dir: str | Path,
) -> Path:
    """Write Bitol-compatible contract YAML and return the output path."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Derive week/path hint from contract_id
    parts = contract_id.split("-")
    week = parts[0] if parts else "unknown"

    schema_section: dict[str, Any] = {}
    for clause in clauses:
        schema_section[clause.field_path] = _clause_to_dict(clause)

    contract_doc = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": contract_id,
        "info": {
            "title": contract_id,
            "version": "1.0.0",
            "owner": "data-contract-enforcer",
        },
        "servers": {
            "local": {
                "type": "local",
                "path": f"outputs/{week}/...",
                "format": "jsonl",
            }
        },
        "schema": schema_section,
        "lineage": lineage,
    }

    out_path = output_dir / f"{contract_id}.yaml"
    try:
        with open(out_path, "w", encoding="utf-8") as fh:
            yaml.dump(contract_doc, fh, default_flow_style=False, allow_unicode=True, sort_keys=False)
    except OSError as exc:
        log.error("Failed to write Bitol YAML to %s: %s", out_path, exc)
        raise

    return out_path


def _write_dbt_yaml(
    contract_id: str,
    clauses: list[ContractClause],
    output_dir: str | Path,
) -> Path:
    """Write dbt schema.yml and return the output path."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    columns = []
    for clause in clauses:
        col: dict[str, Any] = {"name": clause.field_path, "tests": []}
        if clause.required:
            col["tests"].append("not_null")
        if clause.enum:
            col["tests"].append({"accepted_values": {"values": clause.enum}})
        columns.append(col)

    dbt_doc = {
        "version": 2,
        "models": [
            {
                "name": contract_id,
                "columns": columns,
            }
        ],
    }

    out_path = output_dir / f"{contract_id}_dbt.yml"
    try:
        with open(out_path, "w", encoding="utf-8") as fh:
            yaml.dump(dbt_doc, fh, default_flow_style=False, allow_unicode=True, sort_keys=False)
    except OSError as exc:
        log.error("Failed to write dbt YAML to %s: %s", out_path, exc)
        raise

    return out_path


def _write_snapshot(contract_id: str, clauses: list[ContractClause]) -> Path:
    """Write timestamped schema snapshot to schema_snapshots/{contract_id}/{timestamp}.yaml."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snapshot_dir = Path("schema_snapshots") / contract_id
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    schema_section: dict[str, Any] = {}
    for clause in clauses:
        schema_section[clause.field_path] = _clause_to_dict(clause)

    snapshot_doc = {
        "contract_id": contract_id,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "schema": schema_section,
    }

    out_path = snapshot_dir / f"{timestamp}.yaml"
    try:
        with open(out_path, "w", encoding="utf-8") as fh:
            yaml.dump(snapshot_doc, fh, default_flow_style=False, allow_unicode=True, sort_keys=False)
    except OSError as exc:
        log.error("Failed to write snapshot to %s: %s", out_path, exc)
        raise

    return out_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="ContractGenerator")
    parser.add_argument("--source", required=True, help="Path to input JSONL file")
    parser.add_argument("--contract-id", required=True, dest="contract_id", help="Contract identifier")
    parser.add_argument("--lineage", required=False, default=None, help="Path to lineage snapshot JSONL")
    parser.add_argument("--output", required=True, help="Output directory for generated contracts")
    args = parser.parse_args()

    try:
        # 1. Load + flatten → DataFrame
        records = _load_jsonl(args.source)
        if not records:
            log.error("No records loaded from %s — aborting", args.source)
            sys.exit(1)

        df = _flatten_to_dataframe(records)
        if df.empty:
            log.error("DataFrame is empty after flattening — aborting")
            sys.exit(1)

        # 2. Profile columns
        profiles = _profile_columns(df)

        # 3. Generate clauses
        clauses = _generate_clauses(profiles)

        # 4. Inject lineage
        lineage = _inject_lineage(args.contract_id, args.lineage)

        # 5. Write Bitol YAML
        bitol_path = _write_bitol_yaml(args.contract_id, clauses, lineage, args.output)

        # 6. Write dbt YAML
        dbt_path = _write_dbt_yaml(args.contract_id, clauses, args.output)

        # 7. Write snapshot
        snapshot_path = _write_snapshot(args.contract_id, clauses)

        # 8. Summary
        print(
            f"Generated {len(clauses)} clauses for {args.contract_id}",
            file=sys.stderr,
        )
        print(f"  Bitol YAML : {bitol_path}", file=sys.stderr)
        print(f"  dbt YAML   : {dbt_path}", file=sys.stderr)
        print(f"  Snapshot   : {snapshot_path}", file=sys.stderr)

    except Exception as exc:
        log.error("Fatal error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
