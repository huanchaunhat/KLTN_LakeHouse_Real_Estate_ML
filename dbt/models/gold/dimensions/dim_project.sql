-- Real-estate project (khu dân cư / chung cư) a listing belongs to.
-- Grain: one row per project name, plus one member for listings outside any project.
-- A project is placed in the district where most of its listings are, since a
-- few listings carry a wrong district.

with project_locations as (
    select
        project_name,
        province,
        district_id,
        district,
        count(distinct listing_key) as district_listing_count
    from {{ ref('int_listing_enriched') }}
    group by project_name, province, district_id, district
),

ranked as (
    select
        *,
        row_number() over (
            partition by project_name
            order by district_listing_count desc, district_id
        ) as location_rank
    from project_locations
)

select
    {{ project_key('project_name') }} as project_key,
    coalesce(project_name, 'Không thuộc dự án') as project_name,
    project_name is not null as is_project,
    -- the "no project" member spans the whole city, so it has no district
    case when project_name is not null then province end as province,
    case when project_name is not null then district_id end as district_id,
    case when project_name is not null then coalesce(district, 'Không rõ') end as district
from ranked
where location_rank = 1
