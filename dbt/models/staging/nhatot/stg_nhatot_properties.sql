{{ config(
    materialized='incremental',
    file_format='delta',
    unique_key='observation_id',
    incremental_strategy='merge',
    schema='silver'
) }}

with source_data as (
    select *
    from {{ source('raw_lakehouse', 'raw_properties') }}

    {% if is_incremental() %}
    where _ingested_at >= (
        select coalesce(max(_ingested_at), cast('1970-01-01' as timestamp))
        from {{ this }}
    )
    {% endif %}
),

typed as (
    select
        sha2(concat_ws('||', source, cast(source_listing_id as string)), 256) as listing_key,
        sha2(concat_ws('||', source, cast(source_listing_id as string), snapshot_id), 256) as observation_id,

        source,
        cast(source_listing_id as string) as source_listing_id,
        snapshot_id,
        crawl_date,
        crawl_timestamp,
        http_status,
        url,
        error as source_error,

        payload.ad.subject as title,
        payload.ad.body as description,

        case
            when payload.ad.type = 's' then 'sale'
            when payload.ad.type = 'u' then 'rent'
            else 'unknown'
        end as transaction_type,
        payload.ad.type as transaction_type_raw,

        cast(payload.ad.price as string) as price_source_raw,
        cast(payload.ad.price as bigint) as price_vnd,
        cast(payload.ad.price as double) / 1000000000.0 as price_in_billions,
        payload.ad.price_string as price_raw,

        cast(payload.ad.size as string) as area_source_raw,
        cast(payload.ad.size as double) as area_m2,

        payload.ad.category_name as property_type,
        coalesce(
            payload.ad_params.apartment_type.value,
            payload.ad_params.house_type.value,
            payload.ad_params.land_type.value
        ) as property_subtype,

        cast(payload.ad.rooms as int) as bedrooms,
        cast(payload.ad.toilets as int) as bathrooms,
        cast(payload.ad.floors as int) as total_floors,
        cast(payload.ad.floornumber as int) as floor_number,

        coalesce(payload.ad_params.address.value, payload.ad.detail_address) as address_raw,
        case
            when trim(coalesce(payload.ad_params.address.value, payload.ad.detail_address)) <> ''
                then trim(coalesce(payload.ad_params.address.value, payload.ad.detail_address))
        end as address,
        case when trim(payload.ad.street_name) <> '' then trim(payload.ad.street_name) end as street,
        case when trim(payload.ad.ward_name) <> '' then trim(payload.ad.ward_name) end as ward,
        case when trim(payload.ad.area_name) <> '' then trim(payload.ad.area_name) end as district,
        case when trim(payload.ad.region_name) <> '' then trim(payload.ad.region_name) end as province,
        cast(payload.ad.ward as bigint) as ward_id,
        cast(payload.ad.area_v2 as bigint) as district_id,
        cast(payload.ad.region_v2 as bigint) as province_id,

        cast(payload.ad.latitude as string) as latitude_source_raw,
        cast(payload.ad.latitude as double) as latitude,
        cast(payload.ad.longitude as string) as longitude_source_raw,
        cast(payload.ad.longitude as double) as longitude,

        -- A few listings carry raw option ids ('1', '3', '6') instead of a label.
        case
            when trim(payload.ad_params.property_legal_document.value) <> ''
             and not payload.ad_params.property_legal_document.value rlike '^[0-9]+$'
                then trim(payload.ad_params.property_legal_document.value)
        end as legal_status,
        cast(payload.ad.deposit as bigint) as deposit_vnd,
        -- Nhà Tốt sends '' for listings outside a project.
        case when trim(payload.ad.pty_project_name) <> '' then trim(payload.ad.pty_project_name) end as project_name,

        -- Lot shape and building attributes (strong price drivers).
        cast(payload.ad.width as double) as width_m,
        cast(payload.ad.length as double) as length_m,
        cast(payload.ad.living_size as double) as living_area_m2,
        case when trim(payload.ad_params.direction.value) <> '' then trim(payload.ad_params.direction.value) end as house_direction,
        coalesce(payload.ad_params.furnishing_sell.value, payload.ad_params.furnishing_rent.value) as furnishing,
        payload.ad_params.property_status.value as handover_status,
        case
            when trim(payload.ad_params.pty_characteristics.value) <> ''
                then transform(split(payload.ad_params.pty_characteristics.value, ','), x -> trim(x))
            else cast(array() as array<string>)
        end as property_characteristics,
        cast(payload.ad.company_ad as boolean) as is_broker,

        payload.ad.images as image_urls,
        cast(payload.ad.number_of_images as int) as image_count,
        case
            when payload.ad.phone is not null and trim(payload.ad.phone) <> ''
                then sha2(trim(payload.ad.phone), 256)
        end as phone_hash,

        payload.ad.status as source_status,
        payload.ad.state as source_state,

        to_timestamp(from_unixtime(cast(payload.ad.orig_list_time / 1000 as bigint))) as posted_at,
        to_timestamp(from_unixtime(cast(payload.ad.list_time / 1000 as bigint))) as source_updated_at,

        _source_file,
        _ingested_at
    from source_data
),

enriched as (
    select
        *,
        case
            when area_m2 > 0 and price_vnd is not null
                then cast(price_vnd as double) / area_m2
        end as price_per_m2,

        array_contains(property_characteristics, 'Hẻm xe hơi') as has_car_alley,
        array_contains(property_characteristics, 'Mặt tiền')
            or coalesce(property_subtype = 'Nhà mặt phố, mặt tiền', false) as is_frontage,
        array_contains(property_characteristics, 'Nhà nở hậu')
            or array_contains(property_characteristics, 'Nở hậu') as is_wide_back,
        array_contains(property_characteristics, 'Nhà tóp hậu') as is_narrow_back,
        array_contains(property_characteristics, 'Nhà nát') as is_dilapidated,
        array_contains(property_characteristics, 'Nhà chưa hoàn công') as is_not_completed,
        array_contains(property_characteristics, 'Nhà dính quy hoạch / lộ giới') as is_planning_affected,

        sha2(
            concat_ws(
                '||',
                coalesce(title, ''),
                coalesce(description, ''),
                coalesce(transaction_type, ''),
                coalesce(property_type, ''),
                coalesce(property_subtype, ''),
                coalesce(cast(price_vnd as string), ''),
                coalesce(cast(area_m2 as string), ''),
                coalesce(cast(bedrooms as string), ''),
                coalesce(cast(bathrooms as string), ''),
                coalesce(cast(total_floors as string), ''),
                coalesce(cast(floor_number as string), ''),
                coalesce(address, ''),
                coalesce(street, ''),
                coalesce(ward, ''),
                coalesce(district, ''),
                coalesce(province, ''),
                coalesce(cast(latitude as string), ''),
                coalesce(cast(longitude as string), ''),
                coalesce(legal_status, ''),
                coalesce(project_name, ''),
                coalesce(cast(width_m as string), ''),
                coalesce(cast(length_m as string), ''),
                coalesce(cast(living_area_m2 as string), ''),
                coalesce(house_direction, ''),
                coalesce(furnishing, ''),
                coalesce(handover_status, ''),
                array_join(property_characteristics, ','),
                coalesce(source_status, ''),
                coalesce(source_state, '')
            ),
            256
        ) as content_hash
    from typed
),

quality_checked as (
    select
        *,
        filter(
            array(
                case
                    when http_status is null or http_status <> 200 then 'HTTP_ERROR'
                end,
                case
                    when source_listing_id is null or trim(source_listing_id) = '' then 'MISSING_ID'
                end,
                case
                    when crawl_timestamp is null then 'MISSING_TIMESTAMP'
                end,
                case
                    when transaction_type = 'unknown' then 'UNKNOWN_TRANSACTION'
                end,
                case
                    when price_source_raw is null or trim(price_source_raw) = '' then 'MISSING_PRICE'
                end,
                case
                    when price_source_raw is not null
                     and trim(price_source_raw) <> ''
                     and price_vnd is null then 'INVALID_PRICE_FORMAT'
                end,
                case
                    when price_vnd < 0 then 'INVALID_PRICE'
                end,
                case
                    when area_source_raw is null or trim(area_source_raw) = '' then 'MISSING_AREA'
                end,
                case
                    when area_source_raw is not null
                     and trim(area_source_raw) <> ''
                     and area_m2 is null then 'INVALID_AREA_FORMAT'
                end,
                case
                    when area_m2 <= 0 then 'INVALID_AREA'
                end,
                case
                    when province is null or district is null then 'MISSING_LOCATION'
                end,
                case
                    when latitude_source_raw is not null
                     and trim(latitude_source_raw) <> ''
                     and (latitude is null or latitude < -90 or latitude > 90)
                        then 'INVALID_LATITUDE'
                end,
                case
                    when longitude_source_raw is not null
                     and trim(longitude_source_raw) <> ''
                     and (longitude is null or longitude < -180 or longitude > 180)
                        then 'INVALID_LONGITUDE'
                end,
                case
                    when bedrooms < 0 then 'INVALID_BEDROOMS'
                end,
                case
                    when bathrooms < 0 then 'INVALID_BATHROOMS'
                end,
                case
                    when total_floors < 0 then 'INVALID_FLOORS'
                end,
                case
                    when floor_number < 0 then 'INVALID_FLOOR_NUMBER'
                end
            ),
            x -> x is not null
        ) as quality_errors
    from enriched
),

classified as (
    select
        *,
        case
            when size(quality_errors) = 0 then 'VALID'
            else element_at(quality_errors, 1)
        end as primary_quality_flag,
        size(quality_errors) = 0 as is_valid_observation
    from quality_checked
),

final as (
    select
        *,
        primary_quality_flag as data_quality_flag,

        coalesce(
            (
                is_valid_observation
                and transaction_type in ('sale', 'rent')
                and price_vnd > 0
                and area_m2 > 0
                and price_per_m2 > 0
                and province_id = 13000
            ),
            false
        ) as is_market_eligible,

        coalesce(
            (
                is_valid_observation
                and transaction_type = 'sale'
                and price_vnd > 0
                and area_m2 > 0
                and price_per_m2 > 0
                and province_id = 13000
                and district_id is not null
                and property_type is not null
            ),
            false
        ) as is_price_training_eligible,

        current_timestamp() as transformed_at
    from classified
)

select *
from final
