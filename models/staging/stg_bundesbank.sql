{{
    config(
        materialized='view'
    )
}}

{#
    Staging layer: union all raw tables into a single tidy format.
    Strips SDMX metadata columns, keeps only time_period, value, and series identity.
#}

{% set series = [
    ('gdp',        'GDP'),
    ('inflation',  'Inflation'),
    ('ecb_mro',    'ECB_MRO'),
    ('ecb_lower',  'ECB_Lower'),
    ('ecb_upper',  'ECB_Upper'),
    ('yield_2y',   'Yield_2Y'),
    ('euribor_3m', 'EURIBOR_3M'),
    ('yield_10y',  'Yield_10Y'),
] %}

{% for table, label in series %}
select
    '{{ label }}'                                       as series_name,
    time_period,
    value,
    _loaded_at
from {{ source('raw', table) }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
