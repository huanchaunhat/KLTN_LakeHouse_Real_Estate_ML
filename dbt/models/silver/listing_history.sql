{{ config(
    materialized='view',
    schema='silver'
) }}

-- Public name of the valid observation history. A view, not a copy: the data
-- is already materialized once in int_listing_changes.
select *
from {{ ref('int_listing_changes') }}
