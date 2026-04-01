-- week5_sequence_integrity.sql
-- Audit model: flags sequence number gaps within each aggregate stream.
-- Contract clause: sequence_number is monotonically increasing per aggregate_id.
-- This model should return ZERO rows when the event store is healthy.
-- A non-zero row count means the contract is violated.

{{ config(materialized='view', tags=['week5', 'data_contract', 'audit']) }}

with ordered as (
    select
        event_id,
        aggregate_id,
        sequence_number,
        lag(sequence_number) over (
            partition by aggregate_id
            order by sequence_number
        ) as prev_sequence
    from {{ ref('week5_events') }}
),
gaps as (
    select
        event_id,
        aggregate_id,
        sequence_number,
        prev_sequence,
        coalesce(prev_sequence, 0) + 1 as expected_sequence,
        sequence_number - coalesce(prev_sequence, 0) - 1 as gap_size
    from ordered
    where prev_sequence is not null
)
select *
from gaps
where gap_size != 0
