-- week5_events.sql
-- Source: outputs/week5/events.jsonl (loaded as seed)
-- Contract: generated_contracts/week5-event-records.yaml
-- Append-only event log. Events are immutable.

{{ config(materialized='view', tags=['week5', 'data_contract']) }}

select
    event_id,
    event_type,
    aggregate_id,
    aggregate_type,
    sequence_number,
    schema_version,
    occurred_at,
    recorded_at,
    -- Flatten metadata fields for easier testing
    metadata ->> 'correlation_id'  as metadata__correlation_id,
    metadata ->> 'causation_id'    as metadata__causation_id,
    metadata ->> 'user_id'         as metadata__user_id,
    metadata ->> 'source_service'  as metadata__source_service
from {{ ref('events') }}
