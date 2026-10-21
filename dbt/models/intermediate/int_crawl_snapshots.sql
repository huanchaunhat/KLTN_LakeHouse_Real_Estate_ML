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
--
-- The counts come from int_crawl_snapshot_stats, which only recounts new
-- runs; this model only orders and flags the (few) runs, so it stays a table.

with stats as (
    select
        snapshot_id,
        snapshot_started_at,
        snapshot_finished_at,
        online_listing_count,
        valid_listing_count,
        gone_listing_count
    from {{ ref('int_crawl_snapshot_stats') }}
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
