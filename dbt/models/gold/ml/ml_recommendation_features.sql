{{ config(materialized='table', file_format='delta', partition_by=['transaction_type']) }}

-- One row per current advertisement, NOT one row per physical property.
-- Retain ineligible rows for auditing; the model/serving layer enforces the flag.
with source as (
    select
        c.*,
        r.removed_at,
        r.removal_evidence,
        r.listing_key is null as not_removed,
        coalesce(c.last_seen_at, c.crawl_timestamp) as observed_at,
        case trim(c.property_type)
            when 'Nhà ở' then 'HOUSE'
            when 'Căn hộ/Chung cư' then 'APARTMENT'
            when 'Đất' then 'LAND'
            when 'Phòng trọ' then 'ROOM'
            when 'Văn phòng, Mặt bằng kinh doanh' then 'COMMERCIAL'
            else 'OTHER'
        end as property_category,
        case
            when c.transaction_type = 'sale' then 'total'
            when lower(c.price_raw) rlike 'tháng|/th|month' then 'month'
            when lower(c.price_raw) rlike 'ngày|day' then 'day'
            when lower(c.price_raw) rlike 'năm|year' then 'year'
            else 'unknown'
        end as price_period,
        coalesce(
            c.ward_id not in ({{ var('ml_unreliable_ward_ids', [11221]) | join(', ') }})
            and not lower(concat_ws(' ', c.title, c.address))
                rlike '{{ var("ml_outside_legacy_hcmc_places", ["bình dương"]) | join("|") }}',
            false
        ) as location_trusted
    from {{ ref('listing_current') }} c
    left join {{ ref('int_listing_removals') }} r on c.listing_key = r.listing_key
    where c.transaction_type in ('sale', 'rent')
),

features as (
    select
        listing_key, observation_id, source, source_listing_id, url,
        transaction_type, property_category, trim(property_type) as property_type,
        property_subtype, title, description, project_name,
        cast(price_vnd as double) as price_vnd, price_period,
        cast(area_m2 as double) as area_m2,
        case when price_vnd > 0 then ln(cast(price_vnd as double)) end as log_price_vnd,
        case when area_m2 > 0 then ln(area_m2) end as log_area_m2,
        case when area_m2 > 0 then price_vnd / area_m2 end as price_per_m2,
        bedrooms, bathrooms, total_floors, floor_number,
        living_area_m2, width_m, length_m,
        province_id, district_id, ward_id, province, district, ward,
        location_trusted,
        case when location_trusted and latitude between -90 and 90
             and longitude between -180 and 180 then latitude end as latitude,
        case when location_trusted and latitude between -90 and 90
             and longitude between -180 and 180 then longitude end as longitude,
        legal_status, furnishing, house_direction, handover_status,
        has_car_alley, is_frontage, is_planning_affected,
        posted_at, observed_at as last_seen_at,
        source_status, source_state, data_quality_flag,
        removed_at, removal_evidence,
        coalesce(
            is_market_eligible and is_valid_observation and not_removed
            and source = 'nhatot' and lower(source_status) = 'active'
            and lower(source_state) in ('accepted', 'shop_accepted')
            and price_vnd > 0 and area_m2 > 0
            and property_category <> 'OTHER'
            and observed_at is not null and observed_at <= current_timestamp(),
            false
        ) as is_recommendable,
        trim(regexp_replace(concat_ws(' ',
            title, description, property_type, property_subtype, project_name,
            legal_status, furnishing, house_direction,
            case when has_car_alley then 'hẻm xe hơi' end,
            case when is_frontage then 'mặt tiền' end
        ), '[\\s]+', ' ')) as content_text,
        current_timestamp() as feature_built_at
    from source
)

select
    *,
    -- Conservative identical-content group. It is NOT verified entity resolution.
    case when length(trim(coalesce(description, ''))) >= 40 then
        sha2(to_json(named_struct(
            'transaction', transaction_type, 'type', property_category,
            'province', province_id, 'district', district_id, 'ward', ward_id,
            'price', price_vnd, 'period', price_period, 'area', area_m2,
            'text', lower(trim(regexp_replace(description, '[\\s]+', ' ')))
        )), 256)
    else sha2(listing_key, 256) end as duplicate_group
from features
