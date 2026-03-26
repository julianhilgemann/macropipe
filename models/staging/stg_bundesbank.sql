{{
    config(
        materialized='view'
    )
}}

{#
    Staging layer: union all raw tables into a single tidy format.
    Strips SDMX metadata columns, keeps only time_period, value, series identity.
#}

{% set series = [
    ('gdp',              'GDP'),
    ('inflation',        'Inflation'),
    ('ecb_mro',          'ECB_MRO'),
    ('ecb_lower',        'ECB_Lower'),
    ('ecb_upper',        'ECB_Upper'),
    ('yield_2y',         'Yield_2Y'),
    ('euribor_3m',       'EURIBOR_3M'),
    ('yield_10y',        'Yield_10Y'),
    ('hl_rate_total',    'HL_Rate_Total'),
    ('hl_rate_float',    'HL_Rate_Float'),
    ('hl_rate_1_5y',     'HL_Rate_1_5Y'),
    ('hl_rate_5_10y',    'HL_Rate_5_10Y'),
    ('hl_rate_10y',      'HL_Rate_10Y'),
    ('hl_vol_total',     'HL_Vol_Total'),
    ('hl_vol_float',     'HL_Vol_Float'),
    ('hl_vol_1_5y',      'HL_Vol_1_5Y'),
    ('hl_vol_5_10y',     'HL_Vol_5_10Y'),
    ('hl_vol_10y',       'HL_Vol_10Y'),
    ('nfi_rate_total',   'NFI_Rate_Total'),
    ('nfi_rate_sm_float','NFI_Rate_SM_Float'),
    ('nfi_rate_sm_1_5y', 'NFI_Rate_SM_1_5Y'),
    ('nfi_rate_sm_5y',   'NFI_Rate_SM_5Y'),
    ('nfi_rate_lg_float','NFI_Rate_LG_Float'),
    ('nfi_rate_lg_1_5y', 'NFI_Rate_LG_1_5Y'),
    ('nfi_rate_lg_5y',   'NFI_Rate_LG_5Y'),
    ('nfi_vol_total',    'NFI_Vol_Total'),
    ('nfi_vol_sm_float', 'NFI_Vol_SM_Float'),
    ('nfi_vol_sm_1_5y',  'NFI_Vol_SM_1_5Y'),
    ('nfi_vol_sm_5y',    'NFI_Vol_SM_5Y'),
    ('nfi_vol_lg_float', 'NFI_Vol_LG_Float'),
    ('nfi_vol_lg_1_5y',  'NFI_Vol_LG_1_5Y'),
    ('nfi_vol_lg_5y',    'NFI_Vol_LG_5Y'),
] %}

{% for table, label in series %}
select
    '{{ label }}'  as series_name,
    time_period,
    value,
    _loaded_at
from {{ source('raw', table) }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
