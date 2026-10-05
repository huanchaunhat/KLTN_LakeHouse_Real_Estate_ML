{{ config(
    materialized='table',
    file_format='delta',
    schema='silver'
) }}

select *
from {{ ref('stg_nhatot_properties') }}
where is_valid_observation = false
