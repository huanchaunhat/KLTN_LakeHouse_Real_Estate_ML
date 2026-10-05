{{ config(
    materialized='table',
    file_format='delta',
    schema='silver'
) }}

select *
from {{ ref('int_listing_changes') }}
