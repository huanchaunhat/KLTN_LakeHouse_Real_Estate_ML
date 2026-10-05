select *
from {{ ref('listing_current') }}
where is_valid_observation <> true
   or data_quality_flag <> 'VALID'
