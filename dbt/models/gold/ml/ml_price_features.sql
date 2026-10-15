-- Feature table for the sale-price prediction model (read by MLflow training).
-- Grain: one row per sale listing, using its latest state.
-- Keeps only price-training-eligible, non-outlier listings.
--
-- Only attributes a user can provide when asking for a price are used as
-- features. Listing-behaviour fields (days on market, image count, ...) are
-- left out because they are unknown at prediction time.
-- Aggregated area features (e.g. district median price) must be computed
-- inside the training split to avoid target leakage, so they are not here.

select
    f.listing_key,
    to_date(f.last_seen_at) as observed_date,

    -- location
    loc.district_id,
    loc.district,
    loc.district_type,
    loc.area_zone,
    loc.ward_id,
    loc.ward,
    o.street,
    o.latitude,
    o.longitude,
    o.project_name,
    o.project_name is not null as is_in_project,

    -- property type
    pt.property_category,
    pt.property_type,
    pt.property_subtype,

    -- size and shape (implausible values set to NULL, not dropped)
    o.area_m2,
    case when o.living_area_m2 > 0 and o.living_area_m2 <= 20000 then o.living_area_m2 end as living_area_m2,
    case when o.width_m > 0 and o.width_m <= 100 then o.width_m end as width_m,
    case when o.length_m > 0 and o.length_m <= 300 then o.length_m end as length_m,
    o.bedrooms,
    o.bathrooms,
    o.total_floors,
    o.floor_number,

    -- condition and position
    o.house_direction,
    o.furnishing,
    o.handover_status,
    o.has_car_alley,
    o.is_frontage,
    o.is_wide_back,
    o.is_narrow_back,
    o.is_dilapidated,
    o.is_not_completed,
    o.is_planning_affected,

    -- legal
    ls.legal_category,
    ls.has_certificate,

    -- seller
    o.is_broker,

    -- targets
    o.price_vnd,
    ln(o.price_vnd) as log_price_vnd,
    o.price_per_m2
from {{ ref('fct_listing') }} f
inner join {{ ref('int_listing_enriched') }} o
    on f.latest_observation_id = o.observation_id
inner join {{ ref('dim_location') }} loc
    on f.location_key = loc.location_key
inner join {{ ref('dim_property_type') }} pt
    on f.property_type_key = pt.property_type_key
inner join {{ ref('dim_legal_status') }} ls
    on f.legal_status_key = ls.legal_status_key
where f.transaction_type = 'sale'
  and f.is_price_training_eligible
  and not f.is_price_outlier
