"""
tests/test_runner_properties.py
Property-based tests for ValidationRunner.
"""
import os
import sys
import tempfile

import numpy as np
import pandas as pd
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from contracts.runner import run_validation, _check_statistical_drift

# ---------------------------------------------------------------------------
# Feature: data-contract-enforcer, Property 10: ValidationRunner never crashes
# ---------------------------------------------------------------------------

_MINIMAL_CONTRACT = """\
kind: DataContract
apiVersion: v3.0.0
id: test-contract
schema: {}
"""


@given(st.binary())
@settings(max_examples=200)
def test_runner_never_crashes(data: bytes):
    """Property 10: run_validation never raises regardless of data file content."""
    contract_file = None
    data_file = None
    try:
        # Write minimal contract YAML
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False, encoding="utf-8"
        ) as cf:
            cf.write(_MINIMAL_CONTRACT)
            contract_file = cf.name

        # Write arbitrary bytes as the "data" file
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as df_:
            df_.write(data)
            data_file = df_.name

        report = run_validation(contract_file, data_file)

        assert "report_id" in report
        assert "total_checks" in report
        assert "results" in report
    finally:
        if contract_file and os.path.exists(contract_file):
            os.unlink(contract_file)
        if data_file and os.path.exists(data_file):
            os.unlink(data_file)


# ---------------------------------------------------------------------------
# Feature: data-contract-enforcer, Property 13: Statistical drift detection
# ---------------------------------------------------------------------------

@given(
    baseline_mean=st.floats(min_value=0.5, max_value=0.99),
    baseline_std=st.floats(min_value=0.01, max_value=0.1),
)
@settings(max_examples=100)
def test_statistical_drift_detection(baseline_mean: float, baseline_std: float):
    """Property 13: _check_statistical_drift detects large drift (z > 3) correctly."""
    # Column of values drawn from Normal(50, 10) — far from baseline_mean in [0.5, 0.99]
    rng = np.random.default_rng(seed=42)
    values = rng.normal(50, 10, 100)
    df = pd.DataFrame({"confidence": values})

    baselines = {
        "columns": {
            "confidence": {
                "mean": baseline_mean,
                "stddev": baseline_std,
            }
        }
    }

    result = _check_statistical_drift("confidence", df, baselines)

    # The z-score = |50 - baseline_mean| / max(baseline_std, 1e-9)
    # With baseline_mean <= 0.99 and baseline_std <= 0.1, z >= (50 - 0.99) / 0.1 = 490 >> 3
    assert result is not None, "Expected a drift result, got None"
    assert result["status"] == "FAIL", f"Expected FAIL, got {result['status']}"
    assert result["z_score"] > 3, f"Expected z_score > 3, got {result['z_score']}"
