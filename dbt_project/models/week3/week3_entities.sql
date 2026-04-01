-- week3_entities.sql
-- Explodes entities[] array to one row per entity.
-- Contract clause: entity.type MUST be one of {PERSON, ORG, LOCATION, DATE, AMOUNT, OTHER}

{{ config(materialized='view', tags=['week3', 'data_contract']) }}

select
    e_item.value ->> 'entity_id'       as entity_id,
    e.doc_id,
    e_item.value ->> 'name'            as name,
    e_item.value ->> 'type'            as type,
    e_item.value ->> 'canonical_value' as canonical_value
from {{ ref('extractions') }} e,
     json_array_elements(e.entities::json) as e_item(value)
