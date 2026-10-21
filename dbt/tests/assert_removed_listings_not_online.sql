-- A listing marked removed must not be online (reached with any response other
-- than 404/410, quarantined or not) in a run that started at or after its removal.
select
    r.listing_key,
    r.removed_at,
    o.snapshot_id as online_snapshot_id
from {{ ref('int_listing_removals') }} r
inner join {{ ref('stg_nhatot_properties') }} o
    on r.listing_key = o.listing_key
inner join {{ ref('int_crawl_snapshots') }} s
    on o.snapshot_id = s.snapshot_id
where not coalesce(o.http_status in (404, 410), false)
  and s.snapshot_started_at >= r.removed_at
