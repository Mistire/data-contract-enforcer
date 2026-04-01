# DOMAIN_NOTES.md — Data Contract Enforcer

*Minimum 800 words. All five questions must be answered with concrete examples
from your own Weeks 1–5 systems. General answers score 2/5; system-specific
answers with correct Bitol YAML score 5/5.*

---

## Question 1: Backward-Compatible vs. Breaking Schema Changes

TODO: Answer with 3 examples of each from your own week 1–5 schemas.

---

## Question 2: The Confidence Scale Change

TODO: Include actual output of the confidence distribution script run against
outputs/week3/extractions.jsonl, and a syntactically valid Bitol YAML clause
that would catch the 0.0–1.0 → 0–100 change.

---

## Question 3: Lineage-Based Blame Chain Construction

TODO: Explain step-by-step how the ViolationAttributor uses the Week 4
lineage graph to produce a blame chain. Include the specific BFS traversal logic.

---

## Question 4: LangSmith Trace Contract

TODO: Write a Bitol-compatible YAML contract for the trace_record schema
with at least one structural clause, one statistical clause, and one AI-specific clause.

---

## Question 5: Contract Staleness in Production

TODO: Explain the most common production failure mode, why contracts get stale,
and how the snapshot-on-every-run discipline in this architecture prevents it.
