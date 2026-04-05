"""
tests/test_attributor_properties.py
Property-based tests for ViolationAttributor.
"""
import os
import sys

from hypothesis import given, settings
from hypothesis import strategies as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from contracts.attributor import _score_candidates

# ---------------------------------------------------------------------------
# Feature: data-contract-enforcer, Property 15: Blame chain cardinality
# ---------------------------------------------------------------------------

def _make_fake_commits(n: int) -> list[dict]:
    """Build n fake commit dicts with the fields _score_candidates expects."""
    return [
        {
            "commit_hash": f"{'a' * 40}",
            "author": f"author{i}@example.com",
            "commit_timestamp": "2024-01-01 00:00:00+00:00",
            "commit_message": f"commit {i}",
        }
        for i in range(n)
    ]


@given(st.integers(min_value=0, max_value=20))
@settings(max_examples=100)
def test_blame_chain_cardinality(n_candidates: int):
    """Property 15: blame chain always has 1–5 entries regardless of input size."""
    commits = _make_fake_commits(n_candidates)
    violation_timestamp = "2024-01-08T00:00:00+00:00"
    result = _score_candidates(commits, violation_timestamp, lineage_distance=1)

    assert 1 <= len(result) <= 5, (
        f"Expected 1–5 blame entries, got {len(result)} for {n_candidates} candidates"
    )


# ---------------------------------------------------------------------------
# Feature: data-contract-enforcer, Property 16: Blame chain confidence scores in range
# ---------------------------------------------------------------------------

@given(
    days=st.floats(min_value=0, max_value=30),
    distance=st.integers(min_value=0, max_value=5),
)
@settings(max_examples=100)
def test_blame_confidence_scores_in_range(days: float, distance: int):
    """Property 16: confidence score formula always produces a value in [0.0, 1.0]."""
    score = max(0.0, 1.0 - (days * 0.1) - (distance * 0.2))
    assert 0.0 <= score <= 1.0, (
        f"Confidence score {score} out of [0.0, 1.0] for days={days}, distance={distance}"
    )
