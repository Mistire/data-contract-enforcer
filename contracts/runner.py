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
import argparse


def main():
    parser = argparse.ArgumentParser(description="ValidationRunner")
    parser.add_argument("--contract", required=True, help="Path to contract YAML")
    parser.add_argument("--data", required=True, help="Path to data JSONL")
    parser.add_argument("--output", required=True, help="Path to output report JSON")
    args = parser.parse_args()
    # TODO: implement


if __name__ == "__main__":
    main()
