# DOMAIN_NOTES.md — Data Contract Enforcer

*Domain reconnaissance for the Data Contract Enforcer. All five questions are answered
with concrete evidence from the actual Weeks 1–5 system outputs.*

---

## Question 1: Backward-Compatible vs. Breaking Schema Changes

A **backward-compatible** change is one that downstream consumers can absorb without
modification — they continue to work correctly even if they haven't been updated.
A **breaking** change is one that causes downstream consumers to produce incorrect
output or fail entirely, even if they haven't changed.

### Three backward-compatible examples (from our own schemas)

**1. Adding a nullable column to `extraction_record`**
Adding `page_count: null | int` to the Week 3 extraction_record is compatible.
The Week 4 Cartographer, which consumes `doc_id` and `extracted_facts`, simply
ignores the new field. No consumer breaks.

**2. Adding a new enum value to `event_record.event_type`**
Adding `"LoanAmended"` to the Week 5 event stream is additive. Existing projections
that don't handle `LoanAmended` will skip it (or route it to a default handler)
without corrupting their state. The Week 7 Enforcer's `run_type` enum check would
flag it as a new value, but no consumer breaks.

**3. Widening a numeric type in `lineage_snapshot`**
Changing `edge.confidence` from `float32` to `float64` in the Week 4 lineage
snapshot is a widening change. All consumers that read confidence as a float
continue to work — they just get more precision.

### Three breaking examples (from our own schemas)

**1. Changing `extraction_record.extracted_facts[*].confidence` from float 0.0–1.0 to int 0–100**
This is the canonical breaking change for this project. The Week 4 Cartographer
uses confidence values to weight lineage edges. A value of `0.85` becomes `85`,
causing all downstream confidence-weighted logic to produce results 100× too large.
The change passes a type check (still numeric) but breaks statistical assumptions.
This is a **statistical breaking change** — the hardest class to detect.

**2. Renaming `verdict_record.overall_verdict` to `verdict_record.verdict_outcome`**
The Week 2 Digital Courtroom's `overall_verdict` field is consumed by the Week 7
AI Contract Extensions to compute the LLM output schema violation rate. Renaming
it breaks the enum check (`{PASS, FAIL, WARN}`) and causes the violation rate
metric to report 100% violations (all records appear to have missing `overall_verdict`).

**3. Removing `lineage_snapshot.edges[*].source` field**
The Week 7 ViolationAttributor depends on `edges[*].source` to traverse the lineage
graph upstream from a failing column. Removing this field makes the entire blame
chain computation impossible — the attributor falls back to a synthetic candidate
with `confidence_score: 0.0` for every violation.

---

## Question 2: The Confidence Scale Change

### Actual confidence distribution in `outputs/week3/extractions.jsonl`

Running the distribution script against the migrated Week 3 data:

```
count=144  min=0.850  max=0.850  mean=0.850
```

All 144 confidence values are exactly `0.850` — this is the sentinel value set by
`migrate_week3.py` because the original `fact_table.sqlite` has no `confidence`
column. This is itself a contract violation: the distribution is clamped at a single
value (mean > 0.99 would flag as "almost certainly clamped"; mean == 0.85 with
stddev == 0.0 is equally suspicious).

If the Week 3 system were updated to output confidence as integer 0–100 (e.g. `85`
instead of `0.85`), the distribution would shift to `mean=85.0`. The ValidationRunner
would catch this in two ways:

1. **Range check (CRITICAL):** `max=85.0 > maximum=1.0` → immediate FAIL
2. **Statistical drift (HIGH):** z-score = `|85.0 - 0.85| / max(0.0, 1e-9)` → z >> 3 → FAIL

### Bitol YAML clause that catches this change

```yaml
extracted_facts.confidence:
  type: number
  required: true
  minimum: 0.0
  maximum: 1.0
  description: >
    Confidence score for each extracted fact. Must remain 0.0-1.0 float.
    BREAKING CHANGE if changed to integer 0-100 scale — this would silently
    corrupt all downstream systems that use confidence for weighting or filtering.
```

This clause is present in `generated_contracts/week3-document-refinery-extractions.yaml`
and is enforced by the ValidationRunner's range check on every run.

---

## Question 3: Lineage-Based Blame Chain Construction

When the ValidationRunner detects a FAIL on `extracted_facts.confidence.range`,
the ViolationAttributor constructs a blame chain using this exact sequence:

### Step 1 — Map failing column to lineage node

The failing column name `extracted_facts.confidence` is stripped of array notation
and matched against node IDs in the Week 4 lineage graph. The graph uses
`file::{path}` node IDs. The attributor looks for nodes whose path contains
`week3` or `extraction` — for example `file::src/week3/extractor.py`.

### Step 2 — BFS upstream traversal

Starting from the matched node, the attributor performs breadth-first search
following edges in reverse (upstream direction). The Week 4 lineage graph uses
`READS`, `PRODUCES`, `IMPORTS`, `CALLS`, `CONSUMES`, `WRITES` edge types.
The traversal stops at `max_hops=5` or when it reaches a node with no predecessors
(a source node — no incoming edges).

For each visited node, the attributor records `(node_id, hop_distance)`. Nodes
closer to the failing column (smaller hop distance) receive higher confidence scores.

### Step 3 — Git blame per upstream file

For each upstream file identified, the attributor runs:
```
git log --follow --since="14 days ago" --format="%H|%ae|%ai|%s" -- {file_path}
```

This returns commits that touched the file in the past 14 days. Each commit is
a candidate for having introduced the violation.

### Step 4 — Confidence scoring

Each candidate is scored as:
```
confidence_score = max(0.0, 1.0 - (days_since_commit × 0.1) - (lineage_distance × 0.2))
```

A commit made today on a directly upstream file scores `1.0`. A commit made 5 days
ago on a file 2 hops away scores `max(0.0, 1.0 - 0.5 - 0.4) = 0.1`.

Candidates are ranked by confidence descending, truncated to top 5. If no git
candidates are found (e.g. git is unavailable), a synthetic candidate with
`confidence_score: 0.0` and `commit_hash: "unknown"` is returned to satisfy the
minimum blame chain length of 1.

### Step 5 — Blast radius from contract

The blast radius is read directly from `lineage.downstream[]` in the contract YAML
(pre-computed by the ContractGenerator at contract generation time). This avoids
re-traversing the lineage graph at attribution time and ensures the blast radius
is consistent with the contract's view of the world.

---

## Question 4: LangSmith Trace Contract

```yaml
kind: DataContract
apiVersion: v3.0.0
id: langsmith-trace-records
info:
  title: LangSmith Trace Records
  version: 1.0.0
  owner: data-contract-enforcer
  description: >
    One record per LangSmith run (LLM call, chain, tool, retriever, or embedding).
    Exported via LangSmith API to outputs/traces/runs.jsonl.

schema:
  # --- Structural clauses ---
  id:
    type: string
    format: uuid
    required: true
    unique: true
    description: Primary key. UUIDv4. Stable across re-exports.

  run_type:
    type: string
    required: true
    enum: [llm, chain, tool, retriever, embedding]
    description: >
      Type of LangSmith run. BREAKING if new values added without updating
      downstream consumers that filter by run_type.

  start_time:
    type: string
    format: date-time
    required: true

  end_time:
    type: string
    format: date-time
    required: true

  total_tokens:
    type: integer
    required: false
    minimum: 0

  prompt_tokens:
    type: integer
    required: false
    minimum: 0

  completion_tokens:
    type: integer
    required: false
    minimum: 0

  total_cost:
    type: number
    required: false
    minimum: 0.0
    description: Cost in USD. Must be >= 0. Negative values indicate a billing error.

  # --- Statistical clause ---
  total_cost_stats:
    type: number
    description: >
      Statistical contract: baseline mean cost per run established on first validation.
      WARN if current mean deviates > 2 stddev from baseline (model pricing change or
      runaway agent). FAIL if > 3 stddev (likely billing anomaly or infinite loop).

  # --- AI-specific clause ---
  token_conservation:
    type: object
    description: >
      AI-specific invariant: total_tokens MUST equal prompt_tokens + completion_tokens
      for every record where all three fields are non-null. Violation indicates a
      LangSmith export bug or a model that reports tokens inconsistently.
      Check: total_tokens == prompt_tokens + completion_tokens.

quality:
  type: SodaChecks
  specification:
    checks for traces:
      - missing_count(id) = 0
      - duplicate_count(id) = 0
      - min(total_cost) >= 0
      - row_count >= 1

lineage:
  upstream: []
  downstream:
    - id: week7-ai-contract-extensions
      description: AI extensions validate trace schema and compute cost drift
      fields_consumed: [run_type, total_tokens, prompt_tokens, completion_tokens, total_cost]
      breaking_if_changed: [run_type, total_tokens]
```

---

## Question 5: Contract Staleness in Production

### The most common failure mode

The most common production failure mode is **contract drift**: the contract is
written once, reflects the schema at that moment, and is never updated as the
producing system evolves. After 3–6 months, the contract no longer reflects
reality. Enforcement is disabled ("too many false positives") or the contract
is deleted. The system reverts to the original state — no contracts, silent failures.

The root cause is that contracts are treated as documentation rather than as
executable tests. Documentation rots because there is no automated feedback loop
that tells you when it's wrong.

### Why contracts get stale

1. **No snapshot discipline.** Without timestamped snapshots, you can detect that
   a schema changed but not when. The blame chain becomes unreliable.

2. **Manual contract authoring.** Hand-written contracts are updated when someone
   remembers to update them — which is never after the first month.

3. **No CI integration.** Contracts not enforced in CI are not enforced at all.
   A schema change that passes all unit tests will be deployed without triggering
   any contract check.

4. **Blast radius invisibility.** Without lineage context in the contract, a
   producer doesn't know which consumers will break. The incentive to update the
   contract is low because the cost of not updating it is invisible until something
   breaks downstream.

### How this architecture prevents staleness

**Snapshot-on-every-run discipline.** The ContractGenerator writes a timestamped
snapshot to `schema_snapshots/{contract_id}/{timestamp}.yaml` on every run. The
SchemaEvolutionAnalyzer diffs consecutive snapshots automatically. Schema changes
are detected the moment the generator runs on new data — not when someone remembers
to check.

**Auto-generated contracts.** The ContractGenerator infers contracts from actual
data profiles. When the producing system changes its output, the next generator
run produces a new contract that reflects the new reality. The diff between the
old and new contract is the change classification.

**Lineage-embedded blast radius.** Every contract includes `lineage.downstream[]`
populated from the Week 4 lineage graph. A producer can see exactly which consumers
will break before deploying a change. This makes the cost of a breaking change
visible at contract generation time, not at incident time.

### Schema deviations found during migration (real contract violations)

The migration scripts discovered these violations in the actual Week 1–5 systems:

| System | Field | Violation | Severity |
|--------|-------|-----------|----------|
| Week 2 | `overall_verdict` | Field absent — replaced by `overall_score` float | CRITICAL |
| Week 2 | `verdict_id` | Field absent — no UUID primary key | HIGH |
| Week 3 | `doc_id` | Filename string, not UUID | HIGH |
| Week 3 | `extracted_facts[*].confidence` | No confidence column in SQLite — sentinel 0.85 used | CRITICAL |
| Week 3 | `entities[]` | Empty — no entity extraction implemented | MEDIUM |
| Week 4 | `node_id` | Plain path format, not `file::path` | HIGH |
| Week 4 | `git_commit` | Not stored in graph — derived at migration time | HIGH |
| Week 4 | `edge.relationship` | Named `type` not `relationship` | MEDIUM |
| Week 5 | `occurred_at` | Field absent — set equal to `recorded_at` | HIGH |
| Week 5 | `aggregate_id` | Business key (e.g. `APEX-0001`), not UUID | HIGH |
| Week 5 | `sequence_number` | Named `stream_position` | LOW |

These 11 violations were found in 48 hours of running the migration scripts —
before a single line of enforcement code ran. This is the value of treating
past-you as a third party and writing down what you promised.
