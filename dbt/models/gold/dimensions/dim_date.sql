-- Calendar dimension generated from a fixed date spine (see vars in dbt_project.yml).
-- Grain: one row per calendar day.

with spine as (
    select explode(
        sequence(
            to_date('{{ var("date_spine_start") }}'),
            to_date('{{ var("date_spine_end") }}'),
            interval 1 day
        )
    ) as date_day
)

select
    {{ date_key('date_day') }} as date_key,
    date_day,
    year(date_day) as year,
    quarter(date_day) as quarter,
    concat(cast(year(date_day) as string), '-Q', cast(quarter(date_day) as string)) as year_quarter,
    month(date_day) as month,
    date_format(date_day, 'yyyy-MM') as year_month,
    weekofyear(date_day) as week_of_year,
    dayofmonth(date_day) as day_of_month,
    -- ISO day of week: 1 = Monday ... 7 = Sunday
    ((dayofweek(date_day) + 5) % 7) + 1 as day_of_week,
    date_format(date_day, 'EEEE') as day_name,
    dayofweek(date_day) in (1, 7) as is_weekend,
    date_day = last_day(date_day) as is_month_end
from spine
