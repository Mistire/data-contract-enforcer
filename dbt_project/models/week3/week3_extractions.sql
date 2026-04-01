-- week3_extractions.sql
-- Source: outputs/week3/extractions.jsonl (loaded as seed)
-- Contract: generated_contracts/week3-document-refinery-extractions.yaml
-- One row per processed document from the Week 3 Document Intelligence Refinery.

{{ config(materialized='view', tags=['week3', 'data_contract']) }}

select
    doc_id,
    source_path,
    source_hash,
    extraction_model,
    processing_time_ms,
    extracted_at
from {{ ref('extractions') }}
