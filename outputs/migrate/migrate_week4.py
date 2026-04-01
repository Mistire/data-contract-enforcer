"""
outputs/migrate/migrate_week4.py
=================================
Converts Week 4 NetworkX node_link_data JSON files into
canonical lineage_snapshot JSONL.

Week 4 actual output: NetworkX node_link_data JSON format files at
.cartography/{project}/module_graph.json and lineage_graph.json.

node_link_data format:
  {"directed": true, "multigraph": false, "graph": {},
   "nodes": [{"id": "src/main.py", ...}],
   "links": [{"source": "src/main.py", "target": "src/utils.py", "type": "IMPORTS"}]}

DEVIATIONS & SENTINEL VALUES:
  - snapshot_id      → generated UUIDv4 (WARNING logged)
  - codebase_root    → --repo-root arg (absolute path)
  - git_commit       → subprocess git rev-parse HEAD; fallback "0"*40 (WARNING logged)
  - nodes.node_id    → "file::" + node["id"] (plain path → prefixed)
  - nodes.type       → "FILE" for all nodes (no type field in source) (WARNING logged)
  - nodes.label      → basename of path
  - nodes.metadata   → all other node attrs
  - edges.source     → "file::" + link["source"]
  - edges.target     → "file::" + link["target"]
  - edges.relationship → link.get("type", "IMPORTS")
  - edges.confidence → sentinel 1.0 (WARNING logged)
  - captured_at      → file mtime of source JSON as ISO 8601

Usage:
    python outputs/migrate/migrate_week4.py \\
        --module-graph path/to/.cartography/project/module_graph.json \\
        --lineage-graph path/to/.cartography/project/lineage_graph.json \\
        --repo-root path/to/repo \\
        --output outputs/week4/lineage_snapshots.jsonl
"""
import argparse
import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path


def _warn(msg: str) -> None:
    print(f"WARNING: {msg}", file=sys.stderr)


def _get_git_commit(repo_root: str) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", repo_root, "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            commit = result.stdout.strip()
            if len(commit) == 40:
                return commit
    except Exception:
        pass
    _warn(f"git_commit using sentinel '{'0' * 40}' (git rev-parse failed for {repo_root})")
    return "0" * 40


def _file_mtime_iso(path: Path) -> str:
    mtime = path.stat().st_mtime
    return datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()


def _transform_graph(graph_data: dict, source_path: Path, repo_root: str, git_commit: str) -> dict:
    snapshot_id = str(uuid.uuid4())
    _warn(f"snapshot_id using generated UUIDv4 for {source_path.name}")
    _warn(f"edges[].confidence using sentinel 1.0 for {source_path.name}")

    codebase_root = str(Path(repo_root).resolve())
    captured_at = _file_mtime_iso(source_path)

    nodes = []
    for node in graph_data.get("nodes", []):
        node_id_raw = str(node.get("id", ""))
        node_id = f"file::{node_id_raw}"
        label = os.path.basename(node_id_raw) if node_id_raw else ""

        # All other attrs go to metadata
        metadata = {k: v for k, v in node.items() if k != "id"}

        if "type" not in node:
            _warn(f"node type using sentinel 'FILE' for node '{node_id_raw}' (no type field in source)")

        nodes.append({
            "node_id": node_id,
            "type": "FILE",
            "label": label,
            "metadata": metadata,
        })

    edges = []
    for link in graph_data.get("links", []):
        source_raw = str(link.get("source", ""))
        target_raw = str(link.get("target", ""))
        relationship = link.get("type", "IMPORTS")

        edges.append({
            "source": f"file::{source_raw}",
            "target": f"file::{target_raw}",
            "relationship": relationship,
            "confidence": 1.0,
        })

    return {
        "snapshot_id": snapshot_id,
        "codebase_root": codebase_root,
        "git_commit": git_commit,
        "nodes": nodes,
        "edges": edges,
        "captured_at": captured_at,
    }


def _load_and_transform(graph_path: Path, repo_root: str, git_commit: str) -> dict | None:
    if not graph_path.exists():
        print(f"WARNING: Graph file not found, skipping: {graph_path}", file=sys.stderr)
        return None
    try:
        data = json.loads(graph_path.read_text(encoding="utf-8"))
        return _transform_graph(data, graph_path, repo_root, git_commit)
    except Exception as exc:
        print(f"ERROR: Failed to process {graph_path}: {exc}", file=sys.stderr)
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate Week 4 → lineage_snapshot JSONL")
    parser.add_argument("--module-graph", required=True, help="Path to module_graph.json")
    parser.add_argument("--lineage-graph", required=True, help="Path to lineage_graph.json")
    parser.add_argument("--repo-root", required=True, help="Path to the analysed repository root")
    parser.add_argument("--output", required=True, help="Output JSONL path")
    args = parser.parse_args()

    try:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        repo_root = str(Path(args.repo_root).resolve())
        git_commit = _get_git_commit(repo_root)

        records = []
        for graph_path_str in [args.module_graph, args.lineage_graph]:
            record = _load_and_transform(Path(graph_path_str), repo_root, git_commit)
            if record is not None:
                records.append(record)

        with output_path.open("w", encoding="utf-8") as fh:
            for rec in records:
                fh.write(json.dumps(rec) + "\n")

        print(f"INFO: Wrote {len(records)} lineage_snapshots to {output_path}", file=sys.stderr)

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
