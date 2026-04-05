"""
tests/test_report_properties.py
Property-based tests for ReportGenerator.
"""
import os
import sys

from hypothesis import given, settings
from hypothesis import strategies as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from contracts.report_generator import compute_health_score

# ---------------------------------------------------------------------------
# Feature: data-contract-enforcer, Property 23: Data health score formula and bounds
# ---------------------------------------------------------------------------

_result_strategy = st.fixed_dictionaries(
    {
        "severity": st.sampled_from(["CRITICAL", "HIGH", "MEDIUM", "LOW", "WARNING"]),
        "status": st.sampled_from(["FAIL", "PASS", "WARN", "ERROR"]),
    }
)


@given(st.lists(_result_strategy, min_size=0, max_size=50))
@settings(max_examples=100)
def test_health_score_bounds(results: list[dict]):
    """Property 23: compute_health_score always returns a value in [0, 100]."""
    # compute_health_score expects a list of report dicts, each with a "results" key
    validation_reports = [{"results": results}]
    score = compute_health_score(validation_reports)
    assert 0 <= score <= 100, f"Health score {score} out of [0, 100]"


# ---------------------------------------------------------------------------
# Feature: data-contract-enforcer, Property 24: Health score is derived, not hard-coded
# ---------------------------------------------------------------------------

def _make_critical_fail_results(n: int) -> list[dict]:
    return [{"severity": "CRITICAL", "status": "FAIL"} for _ in range(n)]


@given(st.integers(min_value=1, max_value=4))
@settings(max_examples=100)
def test_health_score_derived_not_hardcoded(n: int):
    """Property 24: more CRITICAL violations always produce a lower health score.

    Constrained to n in [1, 4] so that n+1 CRITICAL FAILs (max 5 × 20 = 100 deducted)
    still leaves room for the scores to differ before hitting the floor of 0.
    """
    reports_n = [{"results": _make_critical_fail_results(n)}]
    reports_n1 = [{"results": _make_critical_fail_results(n + 1)}]

    score_n = compute_health_score(reports_n)
    score_n1 = compute_health_score(reports_n1)

    assert score_n != score_n1, (
        f"Scores should differ: n={n} → {score_n}, n+1={n+1} → {score_n1}"
    )
    assert score_n > score_n1, (
        f"More violations should lower score: n={n} → {score_n}, n+1={n+1} → {score_n1}"
    )
