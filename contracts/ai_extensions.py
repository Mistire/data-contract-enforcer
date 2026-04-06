"""
contracts/ai_extensions.py — AI Contract Extensions
=====================================================
Three AI-specific contract checks:
  1. Embedding drift detection (cosine distance from baseline centroid)
  2. Prompt input schema validation (JSON Schema, quarantine on failure)
  3. LLM output schema violation rate tracking

Usage:
    python contracts/ai_extensions.py \
        --mode all \
        --extractions outputs/week3/extractions.jsonl \
        --verdicts outputs/week2/verdicts.jsonl \
        --output validation_reports/ai_extensions.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def _load_jsonl(path: str | Path) -> list[dict]:
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


# ---------------------------------------------------------------------------
# 1. Embedding drift
# ---------------------------------------------------------------------------

def check_embedding_drift(
    texts: list[str],
    baseline_path: str = "schema_snapshots/embedding_baselines.npz",
    threshold: float = 0.15,
) -> dict:
    try:
        import numpy as np
    except ImportError:
        return {"status": "ERROR", "message": "numpy not available", "drift_score": 0.0}

    try:
        from langchain_openai import OpenAIEmbeddings
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            return {"status": "ERROR", "message": "No OPENAI_API_KEY found", "drift_score": 0.0}
        
        # Configure for OpenRouter if applicable
        if api_key.startswith("sk-or-"):
            embeddings = OpenAIEmbeddings(
                model="text-embedding-3-small", 
                openai_api_key=api_key,
                openai_api_base="https://openrouter.ai/api/v1"
            )
        else:
            embeddings = OpenAIEmbeddings(model="text-embedding-3-small", openai_api_key=api_key)
            
        sample = texts[:200] if len(texts) > 200 else texts
        if not sample:
            return {"status": "ERROR", "message": "No texts provided", "drift_score": 0.0}
            
        vectors_list = embeddings.embed_documents(sample)
        vecs = np.array(vectors_list)
        current_centroid = vecs.mean(axis=0)
    except Exception as exc:
        log.error("Embedding API failed: %s", exc)
        return {"status": "ERROR", "message": f"Embedding API failed: {exc}", "drift_score": 0.0}

    bp = Path(baseline_path)
    if not bp.exists():
        bp.parent.mkdir(parents=True, exist_ok=True)
        np.savez(str(bp), centroid=current_centroid)
        return {
            "status": "BASELINE_SET",
            "drift_score": 0.0,
            "threshold": threshold,
            "message": "Baseline established. Run again to detect drift.",
        }

    try:
        baseline = np.load(str(bp))["centroid"]
        dot = np.dot(current_centroid, baseline)
        norm = np.linalg.norm(current_centroid) * np.linalg.norm(baseline)
        cosine_sim = dot / (norm + 1e-9)
        drift = float(1 - cosine_sim)
        return {
            "status": "FAIL" if drift > threshold else "PASS",
            "drift_score": round(drift, 4),
            "threshold": threshold,
            "interpretation": (
                "semantic content of text has shifted" if drift > threshold else "stable"
            ),
        }
    except Exception as exc:
        return {"status": "ERROR", "message": f"Drift computation failed: {exc}", "drift_score": 0.0}


# ---------------------------------------------------------------------------
# 2. Prompt input validation
# ---------------------------------------------------------------------------

PROMPT_INPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["doc_id", "source_path", "content_preview"],
    "properties": {
        "doc_id": {"type": "string", "minLength": 36, "maxLength": 36},
        "source_path": {"type": "string", "minLength": 1},
        "content_preview": {"type": "string", "maxLength": 8000},
    },
    "additionalProperties": False,
}


def validate_prompt_inputs(
    records: list[dict],
    quarantine_path: str = "outputs/quarantine/quarantine.jsonl",
) -> dict:
    try:
        from jsonschema import validate, ValidationError
    except ImportError:
        log.warning("jsonschema not available — skipping prompt input validation")
        return {"valid_count": len(records), "quarantined_count": 0, "error": "jsonschema not installed"}

    valid, quarantined = [], []
    for r in records:
        try:
            validate(instance=r, schema=PROMPT_INPUT_SCHEMA)
            valid.append(r)
        except ValidationError as e:
            quarantined.append({"record": r, "error": e.message})

    if quarantined:
        try:
            Path(quarantine_path).parent.mkdir(parents=True, exist_ok=True)
            with open(quarantine_path, "a") as f:
                for q in quarantined:
                    f.write(json.dumps(q) + "\n")
        except Exception as exc:
            log.warning("Could not write quarantine file: %s", exc)

    return {"valid_count": len(valid), "quarantined_count": len(quarantined)}


# ---------------------------------------------------------------------------
# 3. LLM output violation rate
# ---------------------------------------------------------------------------

def check_output_schema_violation_rate(
    verdict_records: list[dict],
    baseline_rate: float | None = None,
    warn_threshold: float = 0.02,
) -> dict:
    total = len(verdict_records)
    violations = sum(
        1 for v in verdict_records
        if v.get("overall_verdict") not in ("PASS", "FAIL", "WARN")
    )
    rate = violations / max(total, 1)
    trend = "unknown"
    if baseline_rate is not None:
        trend = "rising" if rate > baseline_rate * 1.5 else "stable"

    # Hash of the verdict records as proxy for prompt_hash
    content = json.dumps([v.get("overall_verdict") for v in verdict_records], sort_keys=True)
    prompt_hash = hashlib.sha256(content.encode()).hexdigest()[:16]

    res = {
        "run_date": datetime.now(timezone.utc).date().isoformat(),
        "prompt_hash": prompt_hash,
        "total_outputs": total,
        "schema_violations": violations,
        "violation_rate": round(rate, 4),
        "trend": trend,
        "status": "WARN" if rate > warn_threshold else "PASS",
    }

    if res["status"] == "WARN":
        try:
            log_path = Path("violation_log/violations.jsonl")
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(log_path, "a") as f:
                violation_record = {
                    "violation_id": f"ai-output-{prompt_hash}",
                    "check_id": "ai_extensions.output_schema_violation_rate",
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                    "type": "llm_output_schema",
                    "violation_rate": res["violation_rate"],
                    "message": f"LLM output schema violation rate ({res['violation_rate']}) exceeds threshold ({warn_threshold})"
                }
                f.write(json.dumps(violation_record) + "\n")
        except Exception as exc:
            log.warning("Could not write AI violation to log: %s", exc)

    return res


# ---------------------------------------------------------------------------
# Extraction record → prompt input conversion
# ---------------------------------------------------------------------------

def _extraction_to_prompt_input(record: dict) -> dict | None:
    """Convert an extraction record to a prompt input record for validation."""
    try:
        doc_id = str(record.get("doc_id", ""))
        source_path = str(record.get("source_path", ""))
        # Build content_preview from extracted_facts or source_path
        facts = record.get("extracted_facts", [])
        if facts and isinstance(facts, list):
            preview = " ".join(
                f.get("text", "") for f in facts[:3] if isinstance(f, dict)
            )[:8000]
        else:
            preview = source_path[:8000]
        return {
            "doc_id": doc_id,
            "source_path": source_path,
            "content_preview": preview,
        }
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="AI Contract Extensions")
    parser.add_argument(
        "--mode", default="all",
        choices=["all", "drift", "prompt", "output"],
        help="Which extensions to run",
    )
    parser.add_argument("--extractions", default="outputs/week3/extractions.jsonl", help="Path to extractions JSONL")
    parser.add_argument("--verdicts", default="outputs/week2/verdicts.jsonl", help="Path to verdicts JSONL")
    parser.add_argument("--output", required=True, help="Path to output AI metrics JSON")
    args = parser.parse_args()

    results: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": args.mode,
    }

    # Load data files (degrade gracefully if missing)
    extractions: list[dict] = []
    verdicts: list[dict] = []

    if args.extractions:
        extractions = _load_jsonl(args.extractions)
        log.info("Loaded %d extraction records", len(extractions))

    if args.verdicts:
        verdicts = _load_jsonl(args.verdicts)
        log.info("Loaded %d verdict records", len(verdicts))

    # 1. Embedding drift
    if args.mode in ("all", "drift"):
        try:
            texts = []
            for rec in extractions:
                facts = rec.get("extracted_facts", [])
                if isinstance(facts, list):
                    for f in facts:
                        if isinstance(f, dict) and f.get("text"):
                            texts.append(f["text"])
                elif rec.get("source_path"):
                    texts.append(rec["source_path"])
            drift_result = check_embedding_drift(texts)
            results["embedding_drift"] = drift_result
        except Exception as exc:
            results["embedding_drift"] = {
                "status": "ERROR",
                "message": f"Drift check failed: {exc}",
                "drift_score": 0.0,
            }

    # 2. Prompt input validation
    if args.mode in ("all", "prompt"):
        try:
            prompt_inputs = []
            for rec in extractions:
                pi = _extraction_to_prompt_input(rec)
                if pi:
                    prompt_inputs.append(pi)
            prompt_result = validate_prompt_inputs(prompt_inputs)
            results["prompt_validation"] = prompt_result
        except Exception as exc:
            results["prompt_validation"] = {
                "valid_count": 0,
                "quarantined_count": 0,
                "error": str(exc),
            }

    # 3. LLM output violation rate
    if args.mode in ("all", "output"):
        try:
            violation_result = check_output_schema_violation_rate(verdicts)
            results["output_violation_rate"] = violation_result
        except Exception as exc:
            results["output_violation_rate"] = {
                "status": "ERROR",
                "message": str(exc),
                "violation_rate": 0.0,
            }

    # Write output
    try:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        # 4. Unified status summary
        drift_status = results.get("embedding_drift", {}).get("status", "SKIP")
        output_status = results.get("output_violation_rate", {}).get("status", "SKIP")
        
        status_code = "PASS"
        if "FAIL" in (drift_status, output_status):
            status_code = "FAIL"
        elif "WARN" in (drift_status, output_status):
            status_code = "WARN"
        elif "ERROR" in (drift_status, output_status):
            status_code = "ERROR"

        results["status_summary"] = {
            "overall_ai_status": status_code,
            "drift_detected": drift_status == "FAIL",
            "high_violation_rate": output_status == "WARN"
        }

        with open(output_path, "w", encoding="utf-8") as fh:
            json.dump(results, fh, indent=2)
        log.info("AI extensions report written to %s", output_path)
    except Exception as exc:
        log.error("Failed to write output: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
