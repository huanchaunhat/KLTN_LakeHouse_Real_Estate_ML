-- Gold must not lose or invent listings or observations relative to Silver.
with counts as (
    select
        (select count(*) from {{ ref('listing_current') }}) as silver_listing_count,
        (select count(*) from {{ ref('fct_listing') }}) as gold_listing_count,
        (select count(*) from {{ ref('listing_history') }}) as silver_observation_count,
        (select count(*) from {{ ref('fct_listing_snapshot') }}) as gold_observation_count
)

select *
from counts
where silver_listing_count <> gold_listing_count
   or silver_observation_count <> gold_observation_count
