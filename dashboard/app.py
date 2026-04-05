"""
dashboard/app.py — Streamlit Enforcer Dashboard
================================================
Visualises live enforcement data. Reads files only — never imports from
contracts/ modules. Run with:

    streamlit run dashboard/app.py
"""
import json
import subprocess
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="Data Contract Enforcer", layout="wide")
st.title("🛡️ Data Contract Enforcer")

# ---------------------------------------------------------------------------
# Data loaders — file-only, no caching so refresh always works
# ---------------------------------------------------------------------------

def load_report_data():
    p = Path("enforcer_report/report_data.json")
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


def load_violations():
    p = Path("violation_log/violations.jsonl")
    if not p.exists():
        return []
    try:
        return [json.loads(l) for l in p.read_text().splitlines() if l.strip() and not l.strip().startswith("#")]
    except Exception:
        return []


def load_contracts():
    p = Path("generated_contracts")
    if not p.exists():
        return []
    return list(p.glob("*.yaml"))


def load_snapshots():
    p = Path("schema_snapshots")
    if not p.exists():
        return {}
    result = {}
    try:
        for contract_dir in p.iterdir():
            if contract_dir.is_dir():
                snaps = sorted(contract_dir.glob("*.yaml"))
                result[contract_dir.name] = snaps
    except Exception:
        pass
    return result


def load_validation_reports():
    p = Path("validation_reports")
    if not p.exists():
        return []
    reports = []
    for f in sorted(p.glob("*.json")):
        if f.name in ("ai_extensions.json",) or f.name.startswith("schema_evolution"):
            continue
        try:
            data = json.loads(f.read_text())
            if "results" in data:
                reports.append(data)
        except Exception:
            pass
    return reports


def load_ai_extensions():
    p = Path("validation_reports/ai_extensions.json")
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


def load_schema_evolution():
    p = Path("validation_reports")
    if not p.exists():
        return []
    results = []
    # Load all schema_evolution_*.json files
    for f in sorted(p.glob("schema_evolution*.json")):
        try:
            data = json.loads(f.read_text())
            results.append(data)
        except Exception:
            pass
    # Also try the legacy single-file path
    legacy = p / "schema_evolution.json"
    if legacy.exists() and not any(r.get("contract_id") == json.loads(legacy.read_text()).get("contract_id") for r in results):
        try:
            results.append(json.loads(legacy.read_text()))
        except Exception:
            pass
    return results


# ---------------------------------------------------------------------------
# Sidebar — Run Pipeline
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("⚙️ Run Pipeline")
    st.caption(f"Working directory: `{Path('.').resolve()}`")
    st.markdown("---")

    pipeline_steps = [
        ("1. Migrate Week 3", "python outputs/migrate/migrate_week3.py"),
        ("2. Generate Contracts", "python contracts/generator.py --source outputs/week3/extractions.jsonl --contract-id week3-document-refinery-extractions --lineage outputs/week4/lineage_snapshots.jsonl --output generated_contracts"),
        ("3. Run Validation", "python contracts/runner.py --contract generated_contracts/week3-document-refinery-extractions.yaml --data outputs/week3/extractions.jsonl --output validation_reports/week3_run.json"),
        ("4. Attribute Violations", "python contracts/attributor.py --violation validation_reports/week3_run.json --lineage outputs/week4/lineage_snapshots.jsonl --contract generated_contracts/week3-document-refinery-extractions.yaml --output violation_log/violations.jsonl"),
        ("5. Schema Evolution", "python contracts/schema_analyzer.py --contract-id week3-document-refinery-extractions --output validation_reports/schema_evolution.json"),
        ("6. Generate Report", "python contracts/report_generator.py"),
    ]

    for label, cmd in pipeline_steps:
        if st.button(label, use_container_width=True):
            with st.spinner(f"Running: {cmd}"):
                try:
                    proc = subprocess.run(
                        cmd, shell=True, capture_output=True, text=True, timeout=120
                    )
                    output = proc.stdout + proc.stderr
                    st.code(output or "(no output)", language="text")
                    if proc.returncode == 0:
                        st.success("Done")
                    else:
                        st.error(f"Exit code {proc.returncode}")
                except subprocess.TimeoutExpired:
                    st.error("Command timed out after 120s")
                except Exception as e:
                    st.error(f"Error: {e}")

    st.markdown("---")
    if st.button("🔄 Refresh Dashboard", use_container_width=True, type="primary"):
        st.rerun()


# ---------------------------------------------------------------------------
# Main tabs
# ---------------------------------------------------------------------------

tab_health, tab_violations, tab_evolution, tab_ai, tab_coverage = st.tabs(
    ["Health", "Violations", "Schema Evolution", "AI Risk", "Coverage"]
)

# ---------------------------------------------------------------------------
# Panel 1 — Data Health Score
# ---------------------------------------------------------------------------

with tab_health:
    st.subheader("Data Health Score")
    report = load_report_data()
    if report is None:
        st.info("No data yet — run the pipeline first")
    else:
        try:
            score = report.get("data_health_score", 0)
            narrative = report.get("health_narrative", "")
            severity_tally = report.get("total_violations_by_severity", {})

            col1, col2 = st.columns([1, 2])
            with col1:
                # Gauge chart with colour coding: green >=80, amber 50-79, red <50
                gauge_color = "#2ecc71" if score >= 80 else ("#f39c12" if score >= 50 else "#e74c3c")
                fig = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=score,
                    domain={"x": [0, 1], "y": [0, 1]},
                    title={"text": "Data Health Score"},
                    gauge={
                        "axis": {"range": [0, 100]},
                        "bar": {"color": gauge_color},
                        "steps": [
                            {"range": [0, 50], "color": "#fadbd8"},
                            {"range": [50, 80], "color": "#fdebd0"},
                            {"range": [80, 100], "color": "#d5f5e3"},
                        ],
                        "threshold": {
                            "line": {"color": "black", "width": 2},
                            "thickness": 0.75,
                            "value": score,
                        },
                    },
                ))
                fig.update_layout(height=250, margin=dict(t=40, b=10, l=10, r=10))
                st.plotly_chart(fig, use_container_width=True)
                if score >= 80:
                    st.success(f"✅ Healthy ({score}/100)")
                elif score >= 50:
                    st.warning(f"⚠️ Degraded ({score}/100)")
                else:
                    st.error(f"🚨 Critical ({score}/100)")

            with col2:
                if narrative:
                    st.markdown(f"**{narrative}**")
                generated_at = report.get("generated_at", "")
                if generated_at:
                    st.caption(f"Generated at: {generated_at}")

            st.markdown("---")
            st.subheader("Violations by Severity")
            if severity_tally:
                sev_df = pd.DataFrame(
                    [{"Severity": k, "Count": v} for k, v in severity_tally.items()]
                )
                st.dataframe(sev_df, use_container_width=True, hide_index=True)
            else:
                st.info("No severity data available")

            top_violations = report.get("top_violations", [])
            if top_violations:
                st.markdown("---")
                st.subheader("Top Violations")
                tv_df = pd.DataFrame(top_violations)
                st.dataframe(tv_df, use_container_width=True, hide_index=True)

            recommendations = report.get("recommendations", [])
            if recommendations:
                st.markdown("---")
                st.subheader("Recommendations")
                for i, rec in enumerate(recommendations, 1):
                    st.markdown(f"{i}. {rec}")
        except Exception as e:
            st.error(f"Error rendering health panel: {e}")

# ---------------------------------------------------------------------------
# Panel 2 — Violations table with expandable blame chain
# ---------------------------------------------------------------------------

with tab_violations:
    st.subheader("Violations")
    violations = load_violations()
    if not violations:
        st.info("No violations yet")
    else:
        try:
            # Build summary table with required columns: contract, check, severity, detected_at
            rows = []
            for v in violations:
                blame_chain = v.get("blame_chain", [])
                check_id = v.get("check_id", "")
                # Derive contract from check_id prefix (e.g. "week3.extracted_facts..." → "week3")
                contract = check_id.split(".")[0] if check_id else v.get("type", "")
                # Derive severity from blame chain or type
                severity = v.get("severity", "HIGH")
                rows.append({
                    "contract": contract,
                    "check": check_id,
                    "severity": severity,
                    "detected_at": v.get("detected_at", ""),
                })
            summary_df = pd.DataFrame(rows)
            st.dataframe(summary_df, use_container_width=True, hide_index=True)

            st.markdown("---")
            st.subheader("Violation Details")
            for i, v in enumerate(violations):
                check_id = v.get("check_id", f"violation_{i}")
                detected_at = v.get("detected_at", "")
                label = f"🔍 {check_id} — {detected_at}"
                with st.expander(label):
                    blame_chain = v.get("blame_chain", [])
                    if blame_chain:
                        st.markdown("**Blame Chain**")
                        bc_rows = []
                        for entry in blame_chain:
                            bc_rows.append({
                                "rank": entry.get("rank", ""),
                                "file_path": entry.get("file_path", ""),
                                "author": entry.get("author", ""),
                                "commit_message": entry.get("commit_message", ""),
                                "timestamp": entry.get("commit_timestamp", ""),
                                "confidence": entry.get("confidence_score", 0.0),
                            })
                        st.dataframe(pd.DataFrame(bc_rows), use_container_width=True, hide_index=True)
                    else:
                        st.caption("No blame chain data")

                    blast_radius = v.get("blast_radius", {})
                    if blast_radius:
                        st.markdown("**Blast Radius**")
                        affected_nodes = blast_radius.get("affected_nodes", [])
                        estimated_records = blast_radius.get("estimated_records", 0)
                        st.markdown(f"- Estimated records affected: **{estimated_records}**")
                        if affected_nodes:
                            st.markdown("- Affected nodes:")
                            for node in affected_nodes:
                                st.markdown(f"  - `{node}`")
                        else:
                            st.caption("No affected nodes identified")
        except Exception as e:
            st.error(f"Error rendering violations panel: {e}")

# ---------------------------------------------------------------------------
# Panel 3 — Schema evolution timeline
# ---------------------------------------------------------------------------

with tab_evolution:
    st.subheader("Schema Evolution")
    evolution_reports = load_schema_evolution()
    if not evolution_reports:
        st.info("No schema evolution data yet")
    else:
        try:
            for evolution in evolution_reports:
                contract_id = evolution.get("contract_id", "unknown")
                verdict = evolution.get("compatibility_verdict", "COMPATIBLE")
                st.markdown(f"#### Contract: `{contract_id}`")
                if verdict == "COMPATIBLE":
                    st.success(f"✅ Compatibility Verdict: {verdict}")
                else:
                    st.error(f"🚨 Compatibility Verdict: {verdict}")

                # Collect all changes from pair_diffs or top-level changes
                all_changes = []
                pair_diffs = evolution.get("pair_diffs", [])
                if pair_diffs:
                    for pair in pair_diffs:
                        for c in pair.get("changes", []):
                            c["_from"] = pair.get("from_snapshot", "")
                            c["_to"] = pair.get("to_snapshot", "")
                            all_changes.append(c)
                else:
                    all_changes = evolution.get("changes", [])

                if all_changes:
                    st.markdown("**Detected Changes**")
                    for c in all_changes:
                        compatible = c.get("compatible", True)
                        change_type = c.get("change_type", "")
                        field = c.get("field_path", c.get("field", ""))
                        message = c.get("message", "")
                        label = f"{'🟢 COMPATIBLE' if compatible else '🔴 BREAKING'} — `{field}` ({change_type})"
                        if compatible:
                            st.success(label)
                        else:
                            st.error(label)
                        if message:
                            st.caption(message)
                else:
                    st.info("No schema changes detected")

                # Show migration checklist if present
                migration = evolution.get("migration_impact", {})
                checklist = migration.get("migration_checklist", evolution.get("migration_checklist", []))
                if checklist:
                    with st.expander("Migration Checklist"):
                        for item in checklist:
                            st.markdown(f"- {item}")
                    rollback = migration.get("rollback_plan", "")
                    if rollback:
                        st.caption(f"Rollback: {rollback}")

                st.markdown("---")
        except Exception as e:
            st.error(f"Error rendering schema evolution panel: {e}")

# ---------------------------------------------------------------------------
# Panel 4 — AI risk panel
# ---------------------------------------------------------------------------

with tab_ai:
    st.subheader("AI Risk Assessment")
    ai_data = load_ai_extensions()
    if ai_data is None:
        st.info("No AI extension data yet")
    else:
        try:
            col1, col2, col3 = st.columns(3)

            # Embedding drift
            drift = ai_data.get("embedding_drift", {})
            drift_score = drift.get("drift_score", 0.0)
            drift_status = drift.get("status", "UNKNOWN")
            threshold = drift.get("threshold", 0.15)

            with col1:
                st.markdown("**Embedding Drift**")
                st.progress(min(1.0, float(drift_score) / max(float(threshold) * 2, 0.01)),
                            text=f"Drift score: {drift_score:.4f} (threshold: {threshold})")
                if drift_status == "PASS":
                    st.success(f"✅ {drift_status}")
                elif drift_status == "FAIL":
                    st.error(f"🚨 {drift_status}")
                else:
                    st.warning(f"⚠️ {drift_status}")

            # LLM violation rate
            output_rate = ai_data.get("output_violation_rate", {})
            violation_rate = output_rate.get("violation_rate", 0.0)
            trend = output_rate.get("trend", "unknown")

            with col2:
                st.markdown("**LLM Violation Rate**")
                st.metric("Violation Rate", f"{violation_rate:.2%}", delta=None)
                st.caption(f"Trend: {trend}")

            # Prompt quarantine
            quarantine = ai_data.get("prompt_validation", {})
            quarantine_count = quarantine.get("quarantined_count", 0)
            valid_count = quarantine.get("valid_count", 0)

            with col3:
                st.markdown("**Prompt Quarantine**")
                st.metric("Quarantined Prompts", quarantine_count)
                if valid_count or quarantine_count:
                    total = valid_count + quarantine_count
                    st.caption(f"Valid: {valid_count} / Total: {total}")

            # Full raw data in expander
            with st.expander("Raw AI Extensions Data"):
                st.json(ai_data)
        except Exception as e:
            st.error(f"Error rendering AI risk panel: {e}")

# ---------------------------------------------------------------------------
# Panel 5 — Contract coverage table
# ---------------------------------------------------------------------------

with tab_coverage:
    st.subheader("Contract Coverage")
    try:
        # Known inter-system interfaces
        interfaces = [
            ("W1→W2", "week1-intent-records"),
            ("W3→W4", "week3-document-refinery-extractions"),
            ("W4→W7", "week4-lineage"),
            ("W5→W7", "week5-event-records"),
            ("LangSmith→W7", "langsmith-traces"),
            ("W2→W7", "week2-verdicts"),
        ]

        contract_files = load_contracts()
        contract_stems = {f.stem for f in contract_files}

        validation_reports = load_validation_reports()
        # Build a map: contract_id → latest result
        latest_result: dict[str, str] = {}
        for report in validation_reports:
            cid = report.get("contract_id", "")
            failed = report.get("failed", 0)
            errored = report.get("errored", 0)
            result_str = "FAIL" if (failed + errored) > 0 else "PASS"
            latest_result[cid] = result_str

        rows = []
        for interface_label, contract_id in interfaces:
            has_contract = contract_id in contract_stems
            last_result = latest_result.get(contract_id, "—")
            rows.append({
                "Interface": interface_label,
                "Contract": "✅ Yes" if has_contract else "❌ No",
                "Last Result": last_result,
            })

        coverage_df = pd.DataFrame(rows)
        st.dataframe(coverage_df, use_container_width=True, hide_index=True)

        covered = sum(1 for r in rows if "Yes" in r["Contract"])
        st.caption(f"Coverage: {covered}/{len(interfaces)} interfaces have contracts")
    except Exception as e:
        st.error(f"Error rendering coverage panel: {e}")
