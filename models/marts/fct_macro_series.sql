{{
    config(
        materialized='table',
        pre_hook="CREATE SCHEMA IF NOT EXISTS forecast; CREATE TABLE IF NOT EXISTS forecast.hl_vol_forecasts (series_name VARCHAR, period_date DATE, value DOUBLE, model_name VARCHAR, ci_lower_90 DOUBLE, ci_upper_90 DOUBLE, ci_lower_95 DOUBLE, ci_upper_95 DOUBLE, forecast_run_at VARCHAR);"
    )
}}

{#
    Marts layer: BI-ready fact table.
    Unions actuals from the intermediate layer with forecasts from the
    forecast schema (written by python/forecast.py).
    Includes confidence bands for forecast rows.
#}

select
    series_name,
    period_date,
    time_period     as period_label,
    value,
    'actual'        as value_type,
    null::varchar   as model_name,
    null::double    as ci_lower_90,
    null::double    as ci_upper_90,
    null::double    as ci_lower_95,
    null::double    as ci_upper_95,
    _loaded_at      as record_timestamp
from {{ ref('int_series_cleaned') }}

union all

select
    f.series_name,
    f.period_date,
    strftime(f.period_date, '%Y-%m') as period_label,
    f.value,
    'forecast'      as value_type,
    f.model_name,
    f.ci_lower_90,
    f.ci_upper_90,
    f.ci_lower_95,
    f.ci_upper_95,
    cast(f.forecast_run_at as timestamp) as record_timestamp
from forecast.hl_vol_forecasts f
