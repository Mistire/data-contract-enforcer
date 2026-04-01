-- week3_extracted_facts.sql
-- Explodes extracted_facts[] array to one row per fact.
-- Contract clause: extracted_facts[*].confidence MUST be float [0.0, 1.0]
-- This is the primary contract enforcement target for Week 3.

{{ config(materialized='view', tags=['week3', 'data_contract']) }}

select
    f.value ->> 'fact_id'        as fact_id,
    e.doc_id,
    (f.value ->> 'confidence')::float as confidence,
    f.value ->> 'text'           as text,
    (f.value ->> 'page_ref')::int as page_ref,
    f.value ->> 'source_excerpt' as source_excerpt
from {{ ref('extractions') }} e,
     json_array_elements(e.extracted_facts::json) as f(value)
