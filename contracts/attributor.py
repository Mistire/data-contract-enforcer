"""
contracts/attributor.py — ViolationAttributor
==============================================
Traces a contract violation to the upstream git commit that caused it
using the Week 4 lineage graph and git log/blame.

Usage:
    python contracts/attributor.py \
        --violation validation_reports/violated_run.json \
        --lineage outputs/week4/lineage_snapshots.jsonl \
        --contract generated_contracts/week3-document-refinery-extractions.yaml \
        --output violation_log/violations.jsonl
"""
from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

SUBSCRIPTIONS_PATH = Path("schema_snapshots") / "subscriptions.json"


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
        log.error("Cannot open %s: %s", path, exc)
    return records


def _load_json(path: str | Path) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as exc:
        log.error("Cannot load JSON %s: %s", path, exc)
        return {}


def _load_yaml(path: str | Path) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}
    except Exception as exc:
        log.error("Cannot load YAML %s: %s", path, exc)
        return {}


# ---------------------------------------------------------------------------
# Subscription Registry (Dynamic)
# ---------------------------------------------------------------------------

class SubscriptionRegistry:
    def __init__(self, storage_path: Path):
        self.storage_path = storage_path
        self.subscribers: list[dict] = []
        self._load()

    def _load(self):
        if self.storage_path.exists():
            try:
                with open(self.storage_path, "r") as f:
                    self.subscribers = json.load(f)
            except Exception as exc:
                log.warning("Could not load subscriptions from %s: %s", self.storage_path, exc)

    def _save(self):
        try:
            self.storage_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.storage_path, "w") as f:
                json.dump(self.subscribers, f, indent=2)
        except Exception as exc:
            log.warning("Could not save subscriptions: %s", exc)

    def register(self, subscriber_id: str, contact_channel: str, priority: int, consumed_fields: list[str]):
        """Register a new subscriber and persist."""
        new_sub = {
            "subscriber_id": subscriber_id,
            "contact_channel": contact_channel,
            "priority": priority,
            "consumed_fields": consumed_fields,
            "registered_at": datetime.now(timezone.utc).isoformat()
        }
        # Update existing or add new
        for i, sub in enumerate(self.subscribers):
            if sub["subscriber_id"] == subscriber_id:
                self.subscribers[i] = new_sub
                break
        else:
            self.subscribers.append(new_sub)
        self._save()
        log.info("Registered subscriber: %s", subscriber_id)

    def get_subscribers(self, field_path: str) -> list[dict]:
        """Return subscribers consuming a specific field."""
        return [
            s for s in self.subscribers 
            if any(field_path.startswith(cf.replace("[*]", "")) for cf in s["consumed_fields"])
        ]

    def get_downstream_impact(self, field_path: str) -> dict:
        """Calculate impact score and list affected consumer types."""
        subs = self.get_subscribers(field_path)
        if not subs:
            return {"impact_score": 0.0, "consumer_types": []}
        
        max_priority = max(s["priority"] for s in subs)
        consumer_types = list(set(s["contact_channel"].split(":")[0] for s in subs))
        return {
            "impact_score": round(max_priority / 10.0, 2),
            "consumer_types": consumer_types,
            "subscriber_count": len(subs)
        }

    def enrich_blast_radius(self, blast_radius: dict, field_path: str):
        """Add subscriber info and contamination depth to blast radius."""
        impact = self.get_downstream_impact(field_path)
        subs = self.get_subscribers(field_path)
        
        blast_radius["impact_assessment"] = impact
        blast_radius["subscribers"] = [
            {"id": s["subscriber_id"], "priority": s["priority"]} for s in subs
        ]
        # Contamination depth: distance in lineage + severity weight
        blast_radius["contamination_depth"] = len(blast_radius.get("affected_nodes", []))


# ---------------------------------------------------------------------------
# Lineage traversal
# ---------------------------------------------------------------------------

def _column_to_node(failing_column: str, lineage_snapshot: dict) -> str | None:
    """Map dot-notation column name to a lineage graph node_id."""
    # Extract system prefix: "extracted_facts.confidence" → "week3"
    system = failing_column.split(".")[0].split("_")[0]  # e.g. "extracted" → try "week3"
    nodes = lineage_snapshot.get("nodes", [])
    for node in nodes:
        nid = node.get("node_id", "")
        if system.lower() in nid.lower():
            return nid
    # Fallback: return first FILE node
    for node in nodes:
        if node.get("type") == "FILE":
            return node.get("node_id")
    return None


def _traverse_upstream(start_node: str, lineage_snapshot: dict, max_hops: int = 5) -> list[tuple[str, int]]:
    """BFS upstream from start_node. Returns [(node_id, hop_distance), ...]"""
    edges = lineage_snapshot.get("edges", [])
    # Build reverse adjacency: target → [sources]
    reverse_adj: dict[str, list[str]] = {}
    for edge in edges:
        tgt = edge.get("target", "")
        src = edge.get("source", "")
        reverse_adj.setdefault(tgt, []).append(src)

    visited = {start_node: 0}
    queue = deque([(start_node, 0)])
    results = []
    while queue:
        node, depth = queue.popleft()
        if depth > 0:
            results.append((node, depth))
        if depth >= max_hops:
            continue
        for predecessor in reverse_adj.get(node, []):
            if predecessor not in visited:
                visited[predecessor] = depth + 1
                queue.append((predecessor, depth + 1))
    return results


# ---------------------------------------------------------------------------
# Git blame
# ---------------------------------------------------------------------------

def _run_git_log(file_path: str, repo_root: str = ".") -> list[dict]:
    try:
        result = subprocess.run(
            ["git", "log", "--follow", "--since=14 days ago",
             "--format=%H|%ae|%ai|%s", "--", file_path],
            capture_output=True, text=True, timeout=10, cwd=repo_root
        )
        commits = []
        for line in result.stdout.strip().split("\n"):
            if "|" in line:
                parts = line.split("|", 3)
                if len(parts) == 4:
                    commits.append({
                        "commit_hash": parts[0].strip(),
                        "author": parts[1].strip(),
                        "commit_timestamp": parts[2].strip(),
                        "commit_message": parts[3].strip(),
                    })
        return commits
    except Exception as exc:
        log.error("Git log failed for %s: %s", file_path, exc)
        return []


# ---------------------------------------------------------------------------
# Confidence scoring
# ---------------------------------------------------------------------------

def _score_candidates(commits: list[dict], violation_timestamp: str, lineage_distance: int) -> list[dict]:
    try:
        v_time = datetime.fromisoformat(violation_timestamp.replace("Z", "+00:00"))
    except Exception:
        v_time = datetime.now(timezone.utc)

    scored = []
    for rank, commit in enumerate(commits[:5], start=1):
        try:
            c_time = datetime.fromisoformat(
                commit["commit_timestamp"].replace(" +", "+").replace(" -", "-")
            )
            days_diff = abs((v_time - c_time).days)
        except Exception:
            days_diff = 7
        score = max(0.0, 1.0 - (days_diff * 0.1) - (lineage_distance * 0.2))
        scored.append({**commit, "rank": rank, "confidence_score": round(score, 3), "file_path": ""})

    # If no commits found, return synthetic fallback
    if not scored:
        scored = [{
            "rank": 1,
            "file_path": "unknown",
            "commit_hash": "unknown",
            "author": "unknown",
            "commit_timestamp": violation_timestamp,
            "commit_message": "No git history found in 14-day window",
            "confidence_score": 0.0,
        }]
    return sorted(scored, key=lambda x: x["confidence_score"], reverse=True)


# ---------------------------------------------------------------------------
# Blast radius
# ---------------------------------------------------------------------------

def _compute_blast_radius(contract_path: str, estimated_records: int, field_path: str, registry: SubscriptionRegistry) -> dict:
    try:
        with open(contract_path) as f:
            contract = yaml.safe_load(f)
        downstream = contract.get("lineage", {}).get("downstream", [])
        br = {
            "affected_nodes": [d["id"] for d in downstream],
            "affected_pipelines": [d["id"] for d in downstream if "pipeline" in d.get("id", "")],
            "estimated_records": estimated_records,
        }
        registry.enrich_blast_radius(br, field_path)
        return br
    except Exception as exc:
        log.warning("Blast radius computation failed: %s", exc)
        return {"affected_nodes": [], "affected_pipelines": [], "estimated_records": estimated_records}


# ---------------------------------------------------------------------------
# Node → file path extraction
# ---------------------------------------------------------------------------

def _node_to_file_path(node_id: str) -> str | None:
    """Extract a file path from a node_id like 'file::src/week3/extractor.py'."""
    if "::" in node_id:
        return node_id.split("::", 1)[1]
    # If it looks like a path directly
    if "/" in node_id or node_id.endswith(".py"):
        return node_id
    return None


# ---------------------------------------------------------------------------
# Core attribution logic
# ---------------------------------------------------------------------------

def _attribute_violation(
    fail_result: dict,
    lineage_snapshot: dict,
    contract_path: str,
    detected_at: str,
    registry: SubscriptionRegistry,
    repo_root: str = ".",
) -> dict:
    """Build a single violation record for one FAIL result."""
    check_id = fail_result.get("check_id", "unknown")
    column_name = fail_result.get("column_name", "")
    records_failing = fail_result.get("records_failing", 0)

    # 1. Map failing column to lineage node
    start_node = _column_to_node(column_name, lineage_snapshot)

    # 2. BFS upstream
    upstream_nodes: list[tuple[str, int]] = []
    if start_node:
        upstream_nodes = _traverse_upstream(start_node, lineage_snapshot)

    # 3. Collect git candidates from upstream file nodes
    all_commits: list[tuple[dict, int]] = []  # (commit, lineage_distance)
    for node_id, distance in upstream_nodes:
        file_path = _node_to_file_path(node_id)
        if file_path:
            commits = _run_git_log(file_path, repo_root=repo_root)
            for c in commits:
                all_commits.append((c, distance))

    # Also try the start node itself
    if start_node:
        file_path = _node_to_file_path(start_node)
        if file_path:
            commits = _run_git_log(file_path, repo_root=repo_root)
            for c in commits:
                all_commits.append((c, 0))

    # 4. Score candidates
    if all_commits:
        # Use minimum distance per commit hash
        best_by_hash: dict[str, tuple[dict, int]] = {}
        for commit, dist in all_commits:
            h = commit.get("commit_hash", "")
            if h not in best_by_hash or dist < best_by_hash[h][1]:
                best_by_hash[h] = (commit, dist)

        # Score using the best distance for each commit
        # Group by distance for scoring
        flat_commits = [c for c, _ in best_by_hash.values()]
        min_dist = min(d for _, d in best_by_hash.values()) if best_by_hash else 0
        blame_chain = _score_candidates(flat_commits, detected_at, min_dist)
    else:
        blame_chain = _score_candidates([], detected_at, 0)

    # Attach file_path to each blame entry
    for entry in blame_chain:
        if not entry.get("file_path") or entry["file_path"] == "":
            # Try to find a matching file from upstream nodes
            if upstream_nodes:
                fp = _node_to_file_path(upstream_nodes[0][0])
                entry["file_path"] = fp or "unknown"
            elif start_node:
                fp = _node_to_file_path(start_node)
                entry["file_path"] = fp or "unknown"

    # Clamp to 5 entries
    blame_chain = blame_chain[:5]

    # 5. Compute blast radius
    blast_radius = _compute_blast_radius(contract_path, records_failing, column_name, registry)

    # 6. Determine violation type
    check_type = fail_result.get("check_type", "range")

    return {
        "violation_id": str(uuid.uuid4()),
        "check_id": check_id,
        "detected_at": detected_at,
        "type": check_type,
        "injection_note": False,
        "blame_chain": blame_chain,
        "blast_radius": blast_radius,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="ViolationAttributor")
    parser.add_argument("--violation", required=True, help="Path to validation report JSON")
    parser.add_argument("--lineage", required=True, help="Path to lineage snapshot JSONL")
    parser.add_argument("--contract", required=True, help="Path to contract YAML")
    parser.add_argument("--output", required=True, help="Path to violation log JSONL")
    args = parser.parse_args()

    try:
        # Load validation report
        report = _load_json(args.violation)
        if not report:
            log.error("Empty or invalid validation report: %s", args.violation)
            sys.exit(1)

        # Find all FAIL results
        results = report.get("results", [])
        fail_results = [r for r in results if r.get("status") in ("FAIL", "ERROR")]

        if not fail_results:
            log.info("No FAIL results found in %s — nothing to attribute", args.violation)
            return

        # Initialize Registry
        registry = SubscriptionRegistry(SUBSCRIPTIONS_PATH)
        # Register a fallback if empty
        if not registry.subscribers:
            registry.register("default-consumer", "slack:#data-ops", 5, ["*"])

        # Load lineage snapshot (latest from JSONL)
        lineage_records = _load_jsonl(args.lineage)
        lineage_snapshot = lineage_records[-1] if lineage_records else {}

        detected_at = report.get("run_timestamp", datetime.now(timezone.utc).isoformat())

        # Determine repo root (parent of the contract file's project)
        contract_path = Path(args.contract)
        repo_root = str(contract_path.parent.parent) if contract_path.parent.parent.exists() else "."

        # Prepare output
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        violations_written = 0
        with open(output_path, "a", encoding="utf-8") as out_fh:
            for fail_result in fail_results:
                try:
                    violation = _attribute_violation(
                        fail_result=fail_result,
                        lineage_snapshot=lineage_snapshot,
                        contract_path=str(contract_path),
                        detected_at=detected_at,
                        registry=registry,
                        repo_root=repo_root,
                    )
                    out_fh.write(json.dumps(violation) + "\n")
                    violations_written += 1
                except Exception as exc:
                    log.warning("Failed to attribute violation for %s: %s", fail_result.get("check_id"), exc)
                    # Write a minimal fallback violation
                    fallback = {
                        "violation_id": str(uuid.uuid4()),
                        "check_id": fail_result.get("check_id", "unknown"),
                        "detected_at": detected_at,
                        "type": fail_result.get("check_type", "unknown"),
                        "injection_note": False,
                        "blame_chain": [{
                            "rank": 1,
                            "file_path": "unknown",
                            "commit_hash": "unknown",
                            "author": "unknown",
                            "commit_timestamp": detected_at,
                            "commit_message": f"Attribution failed: {exc}",
                            "confidence_score": 0.0,
                        }],
                        "blast_radius": {"affected_nodes": [], "affected_pipelines": [], "estimated_records": 0},
                    }
                    out_fh.write(json.dumps(fallback) + "\n")
                    violations_written += 1

        log.info("Wrote %d violation records to %s", violations_written, output_path)

    except Exception as exc:
        log.error("Fatal error in ViolationAttributor: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
