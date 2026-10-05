select
    h.observation_id
from {{ ref('listing_history') }} h
inner join {{ ref('listing_quarantine') }} q
    on h.observation_id = q.observation_id
