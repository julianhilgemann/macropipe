{{
    config(
        materialized='table'
    )
}}

{#
    Intermediate layer: parse time_period into a proper date,
    cast types, and prepare for downstream consumption / forecasting.
#}

select
    series_name,
    time_period,
    -- Convert quarterly (2023-Q1) and monthly (2023-01) strings to dates
    case
        when time_period like '%-Q%' then
            cast(
                cast(left(time_period, 4) as int) || '-' ||
                lpad(cast((cast(replace(right(time_period, 2), 'Q', '') as int) - 1) * 3 + 1 as varchar), 2, '0') ||
                '-01'
            as date)
        else
            cast(time_period || '-01' as date)
    end as period_date,
    value,
    _loaded_at
from {{ ref('stg_bundesbank') }}
where value is not null
