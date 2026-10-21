-- Lifecycle events must agree with the accumulating listing fact:
-- exactly one NEW per listing, and one REMOVED per listing that is no longer active.
with counts as (
    select
        (select count(*) from {{ ref('fct_listing') }}) as listing_count,
        (select count(*) from {{ ref('fct_listing') }} where not is_active) as inactive_listing_count,
        (select count(*) from {{ ref('fct_listing_event') }} where event_type = 'NEW') as new_event_count,
        (select count(*) from {{ ref('fct_listing_event') }} where event_type = 'REMOVED') as removed_event_count,
        (select count(*) from {{ ref('fct_listing_snapshot') }} where change_type = 'PRICE_CHANGED') as snapshot_price_change_count,
        (select count(*) from {{ ref('fct_listing_event') }} where event_type = 'PRICE_CHANGED') as event_price_change_count
)

select *
from counts
where listing_count <> new_event_count
   or inactive_listing_count <> removed_event_count
   or snapshot_price_change_count <> event_price_change_count
