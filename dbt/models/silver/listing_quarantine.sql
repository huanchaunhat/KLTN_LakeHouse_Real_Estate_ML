{{ config(
    materialized='view',
    schema='silver'
) }}

-- A view, not a copy: staging is partitioned by crawl_date, so filters on
-- crawl_date stay cheap, and the view can never drift from staging.

select *
from {{ ref('stg_nhatot_properties') }}
where is_valid_observation = false
