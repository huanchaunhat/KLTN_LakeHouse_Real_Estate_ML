-- Administrative location hierarchy: province > district > ward.
-- Grain: one row per (province_id, district_id, ward_id) seen in valid history.
-- Names are taken from the latest observation in case the source renames them.

with locations as (
    select
        province_id,
        province,
        district_id,
        district,
        ward_id,
        ward,
        row_number() over (
            partition by province_id, district_id, ward_id
            order by crawl_timestamp desc, observation_id desc
        ) as name_rank
    from {{ ref('int_listing_enriched') }}
)

select
    {{ location_key('province_id', 'district_id', 'ward_id') }} as location_key,
    province_id,
    coalesce(province, 'Không rõ') as province,
    district_id,
    coalesce(district, 'Không rõ') as district,
    case
        when district like 'Quận%' then 'Quận'
        when district like 'Huyện%' then 'Huyện'
        when district like 'Thành phố%' then 'Thành phố'
        else 'Không rõ'
    end as district_type,
    -- Huyện are the suburban districts of HCMC; Quận and Thủ Đức city are urban.
    case
        when district like 'Huyện%' then 'Ngoại thành'
        when district like 'Quận%' or district like 'Thành phố%' then 'Nội thành'
        else 'Không rõ'
    end as area_zone,
    ward_id,
    coalesce(ward, 'Không rõ') as ward
from locations
where name_rank = 1
