-- Explicit dependencies let dbt select this test with the feature model.
select listing_key
from {{ ref('ml_recommendation_features') }}
where (transaction_type = 'sale' and price_period <> 'total')
   or (transaction_type = 'rent' and price_period = 'total')
   or (is_recommendable and (
       price_vnd is null or price_vnd <= 0 or isnan(price_vnd)
       or area_m2 is null or area_m2 <= 0 or isnan(area_m2)
       or last_seen_at is null or last_seen_at > feature_built_at
       or removed_at is not null or source_status <> 'active'
       or source_state not in ('accepted', 'shop_accepted')
   ))
   or (not location_trusted and (latitude is not null or longitude is not null))
