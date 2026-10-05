with counts as (
    select
        (select count(*) from {{ ref('stg_nhatot_properties') }}) as staging_count,
        (select count(*) from {{ ref('listing_history') }}) as history_count,
        (select count(*) from {{ ref('listing_quarantine') }}) as quarantine_count
)

select *
from counts
where staging_count <> history_count + quarantine_count
