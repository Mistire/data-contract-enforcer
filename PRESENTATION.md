# Presentation: Data Contract Enforcer

## 1. The Problem: Silent Failures
In modern data stacks, changes in upstream systems (e.g., a field being scaled from 0.0-1.0 to 0-100) often go unnoticed until they break downstream analytics or models.
- **Silent Failures**: Data keeps flowing, but the *meaning* of the data has changed.
- **Broken Ownership**: When a field breaks, who owns the fix? Is it the producer, the pipeline, or the consumer?

---

## 2. Our Solution: The Enforcer
The **Data Contract Enforcer** is a proactive observability platform that treats data as an API.

### Core Pillars
1. **Contract Ingestion**: Ingests Bitol/dbt YAML contracts and enforces them against live JSONL streams.
2. **AI-Driven Enrichment**: Uses LLMs to automatically document ambiguous columns and detect non-deterministic failures (embedding drift).
3. **The Blame Chain**: Automatically attributes failures to specific Git commits and authors by traversing the system lineage.
4. **Blast Radius Analysis**: Quantifies the impact of a failure by identifying all downstream consumers and calculating affected record counts.

---

## 3. How It Works (The Lifecycle)
1. **Inference**: System scans existing data to suggest a baseline contract.
2. **Enrichment**: LLMs add semantic meaning to column names (e.g., "confidence" → "AI confidence score").
3. **Deployment**: Contracts are versioned and stored.
4. **Enforcement**: The **Runner** validates every record.
5. **Attribution**: If a contract is violated, the **Attributor** finds the breaking commit.
6. **Evolution**: The **Schema Analyzer** detects breaking changes between versions.

---

## 4. Technical Highlights
- **Bitol Standard**: We use the Industry-standard Bitol specification for contract definitions.
- **Lineage Integration**: Integrates with Week 4 Lineage Snapshots for graph-based impact analysis.
- **Self-Correcting**: Injects metadata back into dbt schemas to ensure documentation stays in sync with code.

---

## 5. Demo Script (Dashboard Walkthrough)

*Switch to "AI Risk" or "Schema Evolution".*
- "Finally, we monitor **Embedding Drift**. If an LLM starts producing different types of answers, our system detects the shift in vector space before it reaches the customer."
