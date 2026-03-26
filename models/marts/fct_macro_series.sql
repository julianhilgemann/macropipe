{{
    config(
        materialized='table'
    )
}}

{#
    Marts layer: final BI-ready fact table.
    One row per series per period, ready for PowerBI consumption.
#}

select
    series_name,
    period_date,
    time_period     as period_label,
    value,
    'actual'        as value_type,
    _loaded_at
from {{ ref('int_series_cleaned') }}
order by series_name, period_date
