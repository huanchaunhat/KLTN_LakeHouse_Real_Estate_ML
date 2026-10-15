-- The ML feature table must only contain eligible, non-outlier sale listings.
select f.listing_key
from {{ ref('ml_price_features') }} m
inner join {{ ref('fct_listing') }} f
    on m.listing_key = f.listing_key
where f.transaction_type <> 'sale'
   or not f.is_price_training_eligible
   or f.is_price_outlier
