-- Property category hierarchy: property_type > property_subtype.
-- Grain: one row per (property_type, property_subtype) seen in valid history.


with types as (
    select distinct
        property_type,
        property_subtype
    from {{ ref('int_listing_enriched') }}
)

select
    {{ property_type_key('property_type', 'property_subtype') }} as property_type_key,
    coalesce(property_type, 'Không rõ') as property_type,
    coalesce(property_subtype, property_type, 'Không rõ') as property_subtype,
    case property_type
        when 'Nhà ở' then 'HOUSE'
        when 'Căn hộ/Chung cư' then 'APARTMENT'
        when 'Đất' then 'LAND'
        when 'Phòng trọ' then 'ROOM'
        when 'Văn phòng, Mặt bằng kinh doanh' then 'COMMERCIAL'
        else 'OTHER'
    end as property_category,
    property_type in ('Nhà ở', 'Căn hộ/Chung cư', 'Phòng trọ') as is_residential
from types