{{ config(
    materialized='table',
    file_format='delta',
    schema='silver'
) }}

-- One row per crawl run (snapshot). Single source of truth for snapshot order,
-- completeness and the initial load, shared by Silver and Gold.
--
-- Snapshots are ordered by start time, never by snapshot_id: legacy runs are
-- named legacy_yyyyMMdd_HHmmss and would sort after yyyyMMdd_HHmmss.
--
-- Completeness counts every listing the run reached that is still online
-- (any observation except 404/410), so a run hit by rate limiting is not
-- mistaken for a smaller market. A partial run (e.g. a retry that only
-- re-fetched a few listings) is not full and never concludes a removal.

with observations as (
    select
        snapshot_id,
        listing_key,
        crawl_timestamp,
        coalesce(http_status in (404, 410), false) as is_gone,
        is_valid_observation
    from {{ ref('stg_nhatot_properties') }}
),

stats as (
    select
        snapshot_id,
        min(crawl_timestamp) as snapshot_started_at,
        max(crawl_timestamp) as snapshot_finished_at,
        count(distinct case when not is_gone then listing_key end) as online_listing_count,
        count(distinct case when is_valid_observation then listing_key end) as valid_listing_count,
        count(distinct case when is_gone then listing_key end) as gone_listing_count
    from observations
    group by snapshot_id
),

flagged as (
    select
        *,
        online_listing_count >= {{ var('full_snapshot_min_ratio') }} * max(online_listing_count) over ()
            as is_full_snapshot
    from stats
),

ordered as (
    select
        *,
        row_number() over (order by snapshot_started_at, snapshot_id) as snapshot_seq,
        -- The next full run after this one: where a listing last seen online in
        -- this run is concluded removed.
        first(case when is_full_snapshot then snapshot_id end, true) over (
            order by snapshot_started_at, snapshot_id
            rows between 1 following and unbounded following
        ) as next_full_snapshot_id,
        first(case when is_full_snapshot then snapshot_started_at end, true) over (
            order by snapshot_started_at, snapshot_id
            rows between 1 following and unbounded following
        ) as next_full_snapshot_started_at
    from flagged
),

initial_load as (
    select min(snapshot_started_at) as initial_load_started_at
    from ordered
    where is_full_snapshot
)

select
    o.*,
    -- Every run up to and including the first full one crawled stock that was
    -- already on the market before the pipeline started.
    coalesce(o.snapshot_started_at <= i.initial_load_started_at, false) as is_initial_load
from ordered o
cross join initial_load i
