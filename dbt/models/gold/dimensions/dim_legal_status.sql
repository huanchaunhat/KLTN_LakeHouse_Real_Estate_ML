-- Legal document status of a listing, grouped into analysis-friendly categories.
-- Grain: one row per cleaned legal status, plus one "unknown" member.

with statuses as (
    select distinct legal_status as legal_status
    from {{ ref('int_listing_enriched') }}
)

select
    {{ legal_status_key('legal_status') }} as legal_status_key,
    coalesce(legal_status, 'Không rõ') as legal_status,
    case
        when legal_status in ('Đã có sổ', 'Sổ hồng riêng', 'Sổ chung / công chứng vi bằng')
            then 'Có sổ'
        when legal_status = 'Đang chờ sổ'
            then 'Đang chờ sổ'
        when legal_status in ('Hợp đồng mua bán', 'Hợp đồng đặt cọc', 'Giấy tờ viết tay', 'Giấy tờ khác')
            then 'Giấy tờ khác'
        when legal_status = 'Không có sổ'
            then 'Không có sổ'
        else 'Không rõ'
    end as legal_category,
    coalesce(
        legal_status in ('Đã có sổ', 'Sổ hồng riêng', 'Sổ chung / công chứng vi bằng'),
        false
    ) as has_certificate
from statuses
