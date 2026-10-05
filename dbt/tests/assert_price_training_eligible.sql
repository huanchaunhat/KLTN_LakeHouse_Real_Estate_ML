select *
from {{ ref('stg_nhatot_properties') }}
where is_price_training_eligible = true
  and (
        is_valid_observation <> true
        or transaction_type <> 'sale'
        or price_vnd is null
        or price_vnd <= 0
        or area_m2 is null
        or area_m2 <= 0
        or price_per_m2 is null
        or price_per_m2 <= 0
        or province_id <> 13000
        or district_id is null
        or property_type is null
      )
