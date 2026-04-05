# Data Contract Enforcer

Schema integrity and lineage attribution system for a multi-week AI platform.

---

## Quick Start / Reproduction Steps

Follow these numbered steps on a fresh clone to reproduce all six entry-point scripts end-to-end.

### Prerequisites

```bash
python --version          # requires 3.11+
pip install -r requirements.txt
cp .env.example .env      # fill in API keys if needed
```

### Step 1 — Generate a contract

```bash
python contracts/generator.py \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --output generated_contracts/
```

Expected output: `generated_contracts/week3-document-refinery-extractions.yaml` with ≥8 clauses, plus a companion `_dbt.yml` file.

### Step 2 — Run validation

```bash
python contracts/runner.py \
  --contract generated_contracts/week3-document-refinery-extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --output validation_reports/week3_run.json
```

Expected output: `validation_reports/week3_run.json` — all structural checks PASS and a `data_health_score` is recorded.

### Step 3 — Attribute a violation

```bash
python contracts/attributor.py \
  --violation week3.extracted_facts.confidence.range \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --contract generated_contracts/week3-document-refinery-extractions.yaml \
  --output violation_log/violations.jsonl
```

Expected output: `violation_log/violations.jsonl` with a blame chain entry and blast-radius fields populated.

### Step 4 — Analyse schema evolution

```bash
python contracts/schema_analyzer.py \
  --contract-id week3-document-refinery-extractions \
  --output validation_reports/schema_evolution_week3.json
```

Expected output: `validation_reports/schema_evolution_week3.json` showing a diff between two schema snapshots with BREAKING / NON_BREAKING change classification.

### Step 5 — Run AI contract extensions (LLM output validation)

```bash
python contracts/ai_extensions.py \
  --mode output \
  --extractions outputs/week3/extractions.jsonl \
  --verdicts outputs/week2/verdicts.jsonl \
  --output validation_reports/ai_extensions.json
```

Expected output: `validation_reports/ai_extensions.json` with `output_violation_rate` block containing `total_outputs`, `schema_violations`, `violation_rate`, and a PASS/WARN status.

### Step 6 — Generate the enforcer report

```bash
python contracts/report_generator.py
```

Expected output: `enforcer_report/report_data.json` with a `data_health_score` in [0, 100] and per-contract summaries.

### Inject a synthetic violation (optional demo)

```bash
python create_violation.py
```

Expected output: writes a modified extractions file with a deliberate out-of-range confidence value, ready to be re-validated with Step 2.

### Launch the Streamlit dashboard

```bash
streamlit run dashboard/app.py
```

Open http://localhost:8501 in your browser to explore contracts, validation results, and schema evolution interactively.

### Run the property-based test suite

```bash
python -m pytest tests/ -q
```

Expected output: all Hypothesis property tests pass (green).

---

## Prerequisites

```bash
python --version   # requires 3.11+
pip install -e .
```

Copy `.env.example` to `.env` and fill in your API keys:
```bash
cp .env.example .env
```

## Step 0: Migrate prior-week outputs to canonical JSONL

```bash
python outputs/migrate/migrate_week2.py \
  --source path/to/automaton-auditor/reports/ \
  --output outputs/week2/verdicts.jsonl

python outputs/migrate/migrate_week3.py \
  --ledger path/to/extraction_ledger.jsonl \
  --sqlite path/to/.refinery/fact_table.sqlite \
  --output outputs/week3/extractions.jsonl

python outputs/migrate/migrate_week4.py \
  --module-graph path/to/.cartography/project/module_graph.json \
  --lineage-graph path/to/.cartography/project/lineage_graph.json \
  --repo-root path/to/repo \
  --output outputs/week4/lineage_snapshots.jsonl

python outputs/migrate/migrate_week5.py \
  --db-url postgresql://user:pass@localhost:5432/ledger \
  --output outputs/week5/events.jsonl
```

## Step 1: Generate contracts

```bash
python contracts/generator.py \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output generated_contracts/
```

Expected output: `generated_contracts/week3_extractions.yaml` (min 8 clauses)
and `generated_contracts/week3_extractions_dbt.yml`

## Step 2: Run validation (clean data — establishes baselines)

```bash
python contracts/runner.py \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --output validation_reports/clean_run.json
```

Expected output: `validation_reports/clean_run.json` — all structural checks PASS

## Step 3: Inject violation and run again

```bash
python create_violation.py --method A
python contracts/runner.py \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions_violated.jsonl \
  --output validation_reports/violated_run.json
```

Expected output: FAIL on `extracted_facts[*].confidence` range check

## Step 4: Attribute the violation

```bash
python contracts/attributor.py \
  --violation validation_reports/violated_run.json \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --contract generated_contracts/week3_extractions.yaml \
  --output violation_log/violations.jsonl
```

Expected output: `violation_log/violations.jsonl` with blame chain and blast radius

## Step 5: Run schema evolution analysis

```bash
python contracts/schema_analyzer.py \
  --contract-id week3-document-refinery-extractions \
  --output validation_reports/schema_evolution.json
```

Expected output: diff between two snapshots with BREAKING change classification

## Step 6: Run AI contract extensions

```bash
python contracts/ai_extensions.py \
  --mode all \
  --extractions outputs/week3/extractions.jsonl \
  --verdicts outputs/week2/verdicts.jsonl \
  --output validation_reports/ai_extensions.json
```

Expected output: embedding drift score, prompt validation counts, LLM violation rate

## Step 7: Generate Enforcer Report

```bash
python contracts/report_generator.py
```

Expected output: `enforcer_report/report_data.json` with `data_health_score` in [0, 100]

## Step 8: Launch dashboard

```bash
streamlit run dashboard/app.py
```

Open http://localhost:8501 in your browser.

## Run tests

```bash
pytest tests/ -v
pytest tests/ -m property -v   # property-based tests only
pytest tests/ -m integration -v
```

## Run dbt tests (contract validation via dbt)

The `dbt_project/` directory is a runnable dbt project that maps every contract
rule to a dbt test. This lets you validate the Week 3 and Week 5 datasets using
standard dbt tooling.

```bash
# Install dbt
pip install dbt-postgres   # or dbt-duckdb for local testing

# Copy and configure your profile
cp dbt_project/profiles.yml.example ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml with your database credentials

# Install dbt packages (dbt_utils)
cd dbt_project
dbt deps

# Run all contract tests for Week 3
dbt test --select tag:week3

# Run all contract tests for Week 5
dbt test --select tag:week5

# Run the confidence range check specifically (key contract target)
dbt test --select week3_extracted_facts

# Run the temporal ordering check (recorded_at >= occurred_at)
dbt test --select week5_events

# Run the sequence integrity audit (should return 0 rows)
dbt test --select week5_sequence_integrity
```

### Contract → dbt test mapping

| Contract rule | dbt test |
|---------------|----------|
| `required: true` | `not_null` |
| `unique: true` | `unique` |
| `enum: [PERSON, ORG, ...]` | `accepted_values` |
| `relationships` (foreign key) | `relationships` |
| `minimum: 0.0, maximum: 1.0` | `dbt_utils.expression_is_true` |
| `format: uuid` | `dbt_utils.expression_is_true` (regex) |
| `format: date-time` | `dbt_utils.expression_is_true` (cast) |
| `recorded_at >= occurred_at` | `dbt_utils.expression_is_true` |
| `sequence_number monotonic` | `week5_sequence_integrity` audit model |
