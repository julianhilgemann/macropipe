# macropipe

End-to-end data pipeline that fetches German macro and banking time series from the Bundesbank SDMX API, transforms them through a three-layer dbt pipeline on DuckDB, runs time series forecasting with cross-validation-based model selection, and serves the results through a PowerBI semantic model.

## Architecture

```
Bundesbank SDMX API
        │
        ▼
  python/fetch.py          ← HTTP GET → lxml XML parse → DataFrame
        │
        ▼
   DuckDB (raw.*)          ← 30 series stored as raw tables
        │
        ▼
   dbt pipeline            ← staging (view) → intermediate (table) → marts (table)
        │
        ▼
  python/forecast.py       ← statsforecast CV → best model per series → 12m forecast
        │
        ▼
   DuckDB (forecast.*)     ← forecasts + CV metrics + run metadata
        │
        ▼
   dbt marts rebuild       ← unions actuals + forecasts into fct_macro_series
        │
        ├──▶ dashboard.html    ← standalone HTML dashboard (prototype / first draft)
        │
        └──▶ PowerBI (.pbip)   ← TMDL semantic model + report (production target)
```

## HTML Dashboard (Prototype)

A self-contained HTML dashboard (`dashboard.html`) serves as the first-draft wireframe for the project. It reads `data/dashboard_data.json` directly and renders three pages — Macro Overview, Housing Loan Forecast, and Forecast Diagnostics — with interactive filters (date range, CI level). No build step or dependencies required; open in any browser while serving the repo directory.

The PowerBI semantic model and report definition are the production target and mirror the same three-page layout with full DAX time intelligence and calculation groups.

![Housing Loan Forecast — 3Y view](docs/forecast_3y.png)

## Data Sources

30 Bundesbank time series from 2010 onwards, fetched via the SDMX 2.1 Generic XML API:

| Segment | Series | Source |
|---------|--------|--------|
| Macro context | GDP, Inflation (HICP), ECB MRO/Deposit/Marginal rates, EURIBOR 3M, Svensson 2Y/10Y yields | BBNZ1, BBDP1, BBIN1, BBIG1, BBSIS |
| Housing loans (households) | 5 rates (APRC total + 4 maturity buckets) + 5 volumes (total + 4 maturity buckets) | BBIM1 (SUD131, SUD116-119, SUD231, SUD216-219) |
| NFI loans (non-financial corps) | 6 rates (total + 3 size x 2 maturity) + 6 volumes (total + 3 size x 2 maturity) | BBIM1 (SUD939A, SUD124-129, SUD949A, SUD224-229) |

**Note:** Housing loan rate total is APRC (SUD131), not the pure interest rate (SUD131Z). APRC includes fees/costs and is a fundamentally different metric from the rate buckets, so it cannot be validated as a weighted average of the maturity sub-buckets. Volume totals are validated as the sum of their buckets.

## Pipeline Steps

### 1. Fetch (`python/fetch.py`)
- Hits the Bundesbank REST endpoint per series defined in `python/config.py`
- Parses SDMX 2.1 Generic XML with `lxml.etree` and namespace-aware XPath
- Stores each series as a separate table in the DuckDB `raw` schema

### 2. Transform (dbt)

Three-layer dbt architecture on `dbt-duckdb`:

- **Staging** (`stg_bundesbank.sql`): Unions all 30 raw tables into a single tidy format with `series_name`, `time_period`, `value`
- **Intermediate** (`int_series_cleaned.sql`): Parses quarterly (`2023-Q1`) and monthly (`2023-01`) period strings into proper `DATE` columns, filters nulls
- **Marts** (`fct_macro_series.sql`): BI-ready fact table that unions actuals from intermediate with forecasts from the forecast schema. Includes confidence bands for forecast rows. Uses `pre_hook` to ensure the forecast schema/table exists on first run.

### 3. Forecast (`python/forecast.py`)

Forecasts 5 housing loan new-business volume series (total + 4 maturity buckets):

- **6 candidate models**: AutoARIMA, AutoETS, AutoTheta, AutoCES, MSTL, SeasonalNaive
- **Expanding-window cross-validation**: `min_train=60`, `step=6`, `h=12` months
- **Metrics**: MAE, RMSE, MAPE, SMAPE per (series, model) across all CV folds
- **Model selection**: lowest RMSE, tie-break on MAE — per series
- **Forecast output**: 12-month point forecast with 90% and 95% prediction intervals
- **Storage**: `forecast.hl_vol_forecasts`, `forecast.hl_vol_cv_metrics`, `forecast.hl_vol_run_metadata`

### 4. Validate (`python/validate.py`)
- Checks volume totals equal sum of maturity buckets (1% tolerance)
- Checks NFI rate total approximates volume-weighted average of rate buckets (10 bps tolerance)
- Documents that HL rate total (APRC) cannot be validated against rate buckets by design

## PowerBI Semantic Model

TMDL-based semantic model in `powerbi/macropipe.SemanticModel/`:

- **Fact table** (`fct_macro_series`): Connects to DuckDB via `DuckDB.Contents()` M connector, 11 columns including CI bands
- **Date dimension** (`synth_dim_date`): DAX-calculated table generated from `MIN/MAX(fct_macro_series[period_date])` with Year Month, Quarter, sort columns
- **Relationship**: `fct_macro_series.period_date` → `synth_dim_date.Date` (both-directions cross-filter)
- **Measures table** (`_Measures`): 20+ DAX measures organized in display folders:
  - Base: `Value ACT`, `Value FCT`, `Value ACT|FCT`, CI bands
  - Time Intelligence: YTD, QTD, Rolling 12M
  - Comparisons: YoY/MoM absolute and percentage changes
  - Forecast Diagnostics: `FCT vs Last ACT Δ%`
  - Display Formatting: directional arrows, auto-format K/M/B, selected period
- **Calculation group** (`CG - Time Intelligence`): 15 items (Current, MTD/QTD/YTD, PY variants, YoY/MoM deltas, Rolling 12M)

### Report Wireframe

Three pages in `powerbi/macropipe.Report/`:

| Page | Visuals |
|------|---------|
| **Macro Overview** | 4 KPI cards (ECB MRO, Inflation, EURIBOR 3M, 10Y Yield) + ECB rates line chart + yield curve line chart + HL APRC rates line chart + HL volume stacked bar |
| **Housing Loan Forecast** | 4 line charts (Float, 1-5Y, 5-10Y, 10Y+) each showing ACT + FCT with 95% CI bands |
| **Forecast Diagnostics** | Forecast summary table (series/model/value) + combined HL volume overlay with 90% CI |

## Setup

```bash
# Create virtual environment and install dependencies
make setup

# Run the full pipeline: fetch → dbt → forecast → dbt (rebuild) → test
make full

# Or run individual steps
make fetch
make transform
make forecast
make test

# Clean generated artifacts
make clean
```

### Requirements

- Python 3.10+
- DuckDB connector for Power BI Desktop (to open the `.pbip` file)
- Internet access for Bundesbank API

### Dependencies

```
duckdb==1.2.1
dbt-duckdb==1.9.1
pandas==2.2.3
requests==2.32.3
lxml==5.3.1
statsforecast==2.0.1
numpy==1.26.4
```

## Project Structure

```
macropipe/
├── orchestrate.py              # CLI pipeline orchestrator (fetch|transform|forecast|test|full)
├── dashboard.html              # Standalone HTML dashboard (prototype wireframe)
├── data/
│   ├── macropipe.duckdb        # DuckDB analytical warehouse
│   └── dashboard_data.json     # JSON export for the HTML dashboard
├── python/
│   ├── config.py               # Series registry (30 series) + DB path config
│   ├── fetch.py                # Bundesbank SDMX fetcher + DuckDB raw storage
│   ├── forecast.py             # CV-based forecasting (statsforecast)
│   └── validate.py             # Volume/rate total validation
├── models/
│   ├── staging/
│   │   ├── stg_bundesbank.sql  # Union 30 raw tables → tidy format
│   │   └── schema.yml          # Source definitions for all raw tables
│   ├── intermediate/
│   │   ├── int_series_cleaned.sql  # Date parsing + null filtering
│   │   └── schema.yml
│   └── marts/
│       ├── fct_macro_series.sql    # Actuals ∪ forecasts (with CI bands)
│       └── schema.yml
├── powerbi/
│   ├── macropipe.pbip              # PowerBI project entry point
│   ├── macropipe.SemanticModel/    # TMDL semantic model definition
│   │   └── definition/
│   │       ├── model.tmdl
│   │       ├── tables/             # fct_macro_series, synth_dim_date, _Measures, CG
│   │       └── relationships/      # rel_fct_date
│   └── macropipe.Report/           # Report wireframe (3 pages, 14 visuals)
│       └── definition/pages/
├── docs/
│   └── forecast_3y.png            # Dashboard screenshot (Housing Loan Forecast, 3Y)
├── dbt_project.yml
├── profiles.yml
├── requirements.txt
└── Makefile
```
