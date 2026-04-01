"""
outputs/migrate/migrate_week4.py
=================================
Converts Week 4 NetworkX node_link_data JSON files into
canonical lineage_snapshot JSONL.

Deviations handled:
  - No snapshot_id       → generated UUIDv4
  - No codebase_root     → derived from source file path
  - No git_commit        → subprocess git rev-parse HEAD in repo dir
  - No captured_at       → file mtime of source JSON
  - Node IDs plain paths → prefixed to file::{path}
  - No node type field   → inferred as FILE for all module nodes
  - Edge field "type"    → renamed to "relationship"
  - Missing READS/WRITES → kept as-is; CONFIGURES mapped to WRITES
  - No edge confidence   → sentinel 1.0

Usage:
    python outputs/migrate/migrate_week4.py \
        --module-graph path/to/.cartography/project/module_graph.json \
        --lineage-graph path/to/.cartography/project/lineage_graph.json \
        --repo-root path/to/repo \
        --output outputs/week4/lineage_snapshots.jsonl
"""
import argparse


def main():
    parser = argparse.ArgumentParser(description="Migrate Week 4 → lineage_snapshot")
    parser.add_argument("--module-graph", required=True, help="Path to module_graph.json")
    parser.add_argument("--lineage-graph", required=True, help="Path to lineage_graph.json")
    parser.add_argument("--repo-root", required=True, help="Path to the analysed repository root")
    parser.add_argument("--output", required=True, help="Output JSONL path")
    args = parser.parse_args()
    # TODO: implement


if __name__ == "__main__":
    main()
