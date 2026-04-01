"""
create_violation.py
====================
Injects a known violation into a dataset for testing the ValidationRunner
and ViolationAttributor. Run this before the violated_run validation step.

Injection Method A (default): Scale change — confidence 0.0-1.0 → 0-100
Injection Method B: Enum violation — invalid entity type

Usage:
    python create_violation.py --method A --input outputs/week3/extractions.jsonl
    python create_violation.py --method B --input outputs/week3/extractions.jsonl
"""
import argparse


def main():
    parser = argparse.ArgumentParser(description="Inject a known violation for testing")
    parser.add_argument("--method", default="A", choices=["A", "B"],
                        help="Injection method: A=scale change, B=enum violation")
    parser.add_argument("--input", default="outputs/week3/extractions.jsonl",
                        help="Source JSONL to inject violation into")
    args = parser.parse_args()
    # TODO: implement


if __name__ == "__main__":
    main()
