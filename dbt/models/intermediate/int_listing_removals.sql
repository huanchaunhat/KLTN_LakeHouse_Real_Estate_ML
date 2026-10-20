{{ config(
    materialized='table',
    file_format='delta',
    schema='silver'
) }}

-- When a listing left the market. Grain: one row per removed listing.
--
-- A listing is "online" in a run when the run reached it with any response
-- other than 404/410. That includes rate-limited fetches and observations
-- quarantined for data quality (e.g. the price was removed), because the
-- listing is still advertised: neither may be read as a removal.
--
-- After the last run in which the listing was online, it is removed at the
-- earliest of:
--   GONE_STATUS                the source answered 404/410 for it, or
--   ABSENT_FROM_FULL_SNAPSHOT  the next full run did not reach it.
-- Gold is rebuilt on every run, so a listing that comes back online is no
-- longer removed.

with snapshots as (
    select *
    from {{ ref('int_crawl_snapshots') }}
),

observations as (
    select
        o.listing_key,
        o.snapshot_id,
        s.snapshot_started_at,
        coalesce(o.http_status in (404, 410), false) as is_gone
    from {{ ref('stg_nhatot_properties') }} o
    inner join snapshots s
        on o.snapshot_id = s.snapshot_id
),

last_online as (
    select
        listing_key,
        max_by(snapshot_id, snapshot_started_at) as last_online_snapshot_id,
        max(snapshot_started_at) as last_online_at
    from observations
    where not is_gone
    group by listing_key
),

candidates as (
    select
        l.listing_key,
        s.next_full_snapshot_id as removed_snapshot_id,
        s.next_full_snapshot_started_at as removed_at,
        'ABSENT_FROM_FULL_SNAPSHOT' as removal_evidence
    from last_online l
    inner join snapshots s
        on l.last_online_snapshot_id = s.snapshot_id
    where s.next_full_snapshot_id is not null

    union all

    select
        o.listing_key,
        o.snapshot_id as removed_snapshot_id,
        o.snapshot_started_at as removed_at,
        'GONE_STATUS' as removal_evidence
    from observations o
    inner join last_online l
        on o.listing_key = l.listing_key
    where o.is_gone
      and o.snapshot_started_at > l.last_online_at
),

-- The 404 usually arrives in that same next full run; the explicit answer
-- from the source is the stronger evidence, so it wins ties.
ranked as (
    select
        *,
        case removal_evidence when 'GONE_STATUS' then 1 else 2 end as evidence_rank
    from candidates
)

select
    listing_key,
    min_by(removed_snapshot_id, struct(removed_at, evidence_rank)) as removed_snapshot_id,
    min(removed_at) as removed_at,
    min_by(removal_evidence, struct(removed_at, evidence_rank)) as removal_evidence
from ranked
group by listing_key
