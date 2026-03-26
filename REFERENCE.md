# macropipe — Technical Reference

Condensed technical summary of the full project for developer onboarding and context recovery.

---

## Pipeline Overview

Bundesbank SDMX 2.1 Generic XML → DuckDB (`raw` → `staging` → `intermediate` → `marts` + `forecast`) → PowerBI TMDL semantic model + PBIP report wireframe. 30 series: 8 macro context + 10 housing loan (rate+volume) + 12 NFI loan (rate+volume). Data from 2010 onwards. Forecasting on 5 HL volume series with expanding-window CV, 6 candidate models, best model per series. PowerBI: calculation groups for time intelligence, ACT/FCT distinction, confidence bands, synthetic date table.

---

## Key Technical Concepts

- **SDMX 2.1 Generic XML API** — Bundesbank REST endpoint at `https://api.statistiken.bundesbank.de/rest/data/{flow_ref}/{key}?startPeriod={start}&format=sdmx_generic_xml`
- **lxml etree** XML parsing with namespaces: `message` = `http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message`, `generic` = `http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic`
- **DuckDB** as local analytical warehouse — schemas: `raw`, `main_staging`, `main_intermediate`, `main_marts`, `forecast`
- **dbt-duckdb** adapter v1.9.1 (must use venv version, not system dbt)
- **dbt 3-layer architecture**: staging (views) → intermediate (tables) → marts (tables)
- **statsforecast**: AutoARIMA, AutoETS, AutoTheta, AutoCES, MSTL, SeasonalNaive
- **Expanding-window CV**: `min_train=60`, `step=6`, `h=12` — model selection by lowest RMSE with MAE tie-break
- **Prediction intervals**: 90% and 95% confidence levels
- **PowerBI TMDL** (Tabular Model Definition Language) for semantic model definition in PBIP format
- **DAX calculation groups** for time intelligence — 15 items applied to any base measure via `SELECTEDMEASURE()`
- **DuckDB.Contents()** M connector — relative path `data/macropipe.duckdb`, schema `main_marts`

---

## Files and Code Details

### `python/config.py` — Central Configuration

```python
SERIES_REGISTRY: dict[str, tuple[str, str, str]]  # table_name → (flow_ref, key, start_period)
```

- 30 entries. Naming: `hl_` = housing loans, `nfi_` = non-financial corps, `_rate_`/`_vol_` = metric, `_sm_`/`_lg_` = loan size, `_float`/`_1_5y`/`_5_10y`/`_10y` = maturity
- HL rate total = APRC (SUD131 via `M.DE.B.A2C.A.C.A.2250.EUR.N`), NOT pure rate SUD131Z
- DB_PATH = `PROJECT_ROOT / "data" / "macropipe.duckdb"`, RAW_SCHEMA = `"raw"`

### `python/fetch.py` — SDMX Fetcher

- `fetch_series(flow_ref, key, start)` → HTTP GET with `sdmx_generic_xml` format → parse XML with `etree.fromstring()` → extract `generic:SeriesKey/generic:Value` metadata + `generic:Obs` observations → DataFrame
- Metadata keys lowercased. Adds `_loaded_at = pd.Timestamp.now(tz="UTC")`
- `store_raw(df, table_name)` → `CREATE OR REPLACE TABLE raw.{table_name} AS SELECT * FROM df`
- `ingest()` → iterates `SERIES_REGISTRY`

### `python/forecast.py` — CV-Based Forecasting

- **Series**: `HL_Vol_Total`, `HL_Vol_Float`, `HL_Vol_1_5Y`, `HL_Vol_5_10Y`, `HL_Vol_10Y`
- **6 models**: AutoARIMA, AutoETS, AutoTheta, AutoCES, MSTL, SeasonalNaive (all `season_length=12`)
- `_load_series()` — reads from `main_intermediate.int_series_cleaned` with statsforecast column convention: `unique_id`, `ds`, `y`
- `_run_crossvalidation()` — `StatsForecast.cross_validation()` with `n_windows = max(1, (len - 60 - 12) // 6 + 1)`
- `_compute_cv_metrics()` — MAE, RMSE, MAPE, SMAPE per (series, model). Exclude set: `{"unique_id", "ds", "y", "cutoff", "index"}`
- `_select_best_model()` — sort by `[rmse, mae]`, `groupby("series_name").first()`
- `_produce_forecasts()` — model map built as `{repr(m).split("(")[0]: m for m in _build_models()}` to match statsforecast column names (e.g. `AutoCES` class → `"CES"` column). Renames CI columns from `{model}-lo-{level}` to `ci_lower_{level}`
- `_store_results()` → `forecast.hl_vol_forecasts`, `forecast.hl_vol_cv_metrics`, `forecast.hl_vol_run_metadata`
- **Results**: MSTL won 4/5 series, CES won HL_Vol_10Y

### `python/validate.py` — Total vs Bucket Validation

- `check_volume_total()` — checks `sum(buckets)` within 1% relative tolerance of reported total
- `check_rate_weighted()` — checks `sum(rate_i * vol_i) / sum(vol_i)` within 10 bps of reported total rate
- HL rate total (APRC/SUD131) is a different metric from pure rate buckets → skip rate validation for HL
- Results: HL vol max diff 1.0 Mn EUR (PASS), NFI vol max diff 0.0 (PASS), NFI rate max diff 0.8 bps (PASS)

### `orchestrate.py` — CLI Pipeline Orchestrator

- Steps: `fetch`, `transform`, `forecast`, `test`, `full`
- `full` runs: fetch → dbt run → forecast → dbt run (rebuild marts with forecasts) → dbt test
- PATH override to ensure venv dbt: `env = {**os.environ, "PATH": f"{VENV_BIN}:{os.environ['PATH']}"}`

### `models/staging/stg_bundesbank.sql`

- Jinja `{% set series = [...] %}` with 30 `(table, label)` tuples → `{% for %}` UNION ALL
- No SQL `--` comments inside Jinja set block (causes parse error)

### `models/intermediate/int_series_cleaned.sql`

- Converts `2023-Q1` → `2023-01-01` (quarterly: `(Q-1)*3+1` month) and `2023-01` → `2023-01-01` (monthly: append `-01`)
- `WHERE value IS NOT NULL`

### `models/marts/fct_macro_series.sql`

- Unions actuals from `ref('int_series_cleaned')` + forecasts from `forecast.hl_vol_forecasts`
- `pre_hook`: `CREATE SCHEMA IF NOT EXISTS forecast; CREATE TABLE IF NOT EXISTS forecast.hl_vol_forecasts (...)`
- Columns: `series_name`, `period_date`, `period_label`, `value`, `value_type` (actual|forecast), `model_name`, `ci_lower_90`, `ci_upper_90`, `ci_lower_95`, `ci_upper_95`, `record_timestamp`

### dbt Config

- `dbt_project.yml`: schemas — `staging` (views), `intermediate` (tables), `marts` (tables)
- `profiles.yml`: `type: duckdb`, `path: data/macropipe.duckdb`, `threads: 4`
- `schema.yml` tests: not_null on key columns, `accepted_values` on `value_type` for `['actual', 'forecast']`

---

## PowerBI Semantic Model (TMDL)

### `model.tmdl`
```
culture: en-US, sourceQueryCulture: de-DE, powerBI_V3
refs: fct_macro_series, synth_dim_date, _Measures, 'CG - Time Intelligence', rel_fct_date
```

### `fct_macro_series.tmdl`
- 11 columns with `lineageTag`, `dataType`, `summarizeBy`
- M partition: `DuckDB.Contents("data/macropipe.duckdb")` → `Source{[Schema="main_marts"]}[Data]` → `marts{[Name="fct_macro_series"]}[Data]`

### `synth_dim_date.tmdl`
- DAX calculated table from `CALENDAR(MIN(...), MAX(...))` with `ADDCOLUMNS`
- Columns: Date (isKey), Year, Month, Month Name (sortBy Month), Year Month (sortBy Year Month Sorter = Y*100+M), Quarter Number, Quarter Year (sortBy QY Sorter = Y*10+Q), Start of Month, Day
- `dataCategory: Time`

### `_Measures.tmdl` — 20+ DAX Measures

| Folder | Measures |
|--------|----------|
| 00 - Base | Value ACT, Value FCT, Value ACT\|FCT, Value Type Flag, CI Lower/Upper 90/95, CI Band Width 95 |
| 01 - Time Intelligence | Value ACT YTD (`TOTALYTD`), QTD (`TOTALQTD`), R12M (`DATESINPERIOD -12 MONTH`) |
| 02 - Comparisons\YoY | Value Δ YoY, Δ% YoY, YTD Δ% YoY |
| 02 - Comparisons\MoM | Value Δ MoM, Δ% MoM |
| 03 - Forecast Diagnostics | FCT vs Last ACT Δ% |
| 99 - Display Formatting | Δ% YoY/MoM Display (▲/▼ arrows), Value ACT AF (K/M/B auto-format), Selected Period |

All measures have `///` documentation comments. Percentage measures use `+0.0%;-0.0%;0.0%` format.

### `CG - Time Intelligence.tmdl` — Calculation Group

15 calculation items using `SELECTEDMEASURE()` on `synth_dim_date[Date]`:
Current, MTD, QTD, YTD, PY, PY MTD, PY QTD, PY YTD, Δ YoY, Δ% YoY, Δ% YoY MTD, Δ% YoY QTD, Δ% YoY YTD, MoM (Prior Month), Δ% MoM, Rolling 12 Months.
Percentage items have `formatStringDefinition = "+0.0%;-0.0%;0.0%"`.

### `rel_fct_date.tmdl`
```
fromColumn: fct_macro_series.period_date → toColumn: synth_dim_date.Date
crossFilteringBehavior: bothDirections
```

---

## Report Wireframe (3 pages, 14 visuals)

### Page 1: Macro Overview
- **4 KPI cards**: ECB MRO (`filter: 'ECB_MRO'`), Inflation, EURIBOR 3M, 10Y Yield — each filtered to single series via `fct_macro_series.series_name`
- **Line chart**: ECB Policy Rates & EURIBOR 3M — series filter: `ECB_MRO, ECB_Lower, ECB_Upper, EURIBOR_3M`
- **Line chart**: Svensson Yield Curve — series filter: `Yield_2Y, Yield_10Y`
- **Line chart**: HL Rates by Maturity — series filter: `HL_Rate_Total, HL_Rate_Float, HL_Rate_1_5Y, HL_Rate_5_10Y, HL_Rate_10Y`
- **Stacked bar**: HL Volume by Maturity Bucket — series filter: `HL_Vol_Float, HL_Vol_1_5Y, HL_Vol_5_10Y, HL_Vol_10Y`
- All use Category=`synth_dim_date.Year Month`, Y=`_Measures.Value ACT`, Series=`fct_macro_series.series_name`

### Page 2: Housing Loan Forecast
- **4 line charts** (Float, 1-5Y, 5-10Y, 10Y+) — each filtered to single HL volume series
- Y-axis: `Value ACT|FCT`, `CI Upper 95`, `CI Lower 95`
- Category: `synth_dim_date.Year Month`

### Page 3: Forecast Diagnostics
- **Table**: `fct_macro_series.series_name`, `model_name`, `Value ACT` — filtered to `value_type = 'forecast'`
- **Line chart**: All HL volume series overlay with `Value ACT|FCT`, `CI Upper 90`, `CI Lower 90`

---

## Known Issues and Fixes Applied

1. **DuckDB lock from DBeaver** — DBeaver holds exclusive write lock. Close DBeaver before running pipeline.
2. **Jinja `--` comments in `{% set %}` block** — SQL comments inside Jinja set block cause `expected token ','` parse error. Remove all `--` comments from inside Jinja blocks.
3. **`run_query` in dbt-duckdb** — `run_query()` returns `None` at parse time → `'None' has no attribute 'table'`. Use `pre_hook` instead.
4. **dbt version mismatch** — System dbt 1.8 vs venv dbt 1.9/1.11. `orchestrate.py` prepends `VENV_BIN` to PATH.
5. **`KeyError: 'CES'`** — `AutoCES.__class__.__name__` = `"AutoCES"` but statsforecast columns use `repr(model)` = `"CES"`. Fix: `repr(m).split("(")[0]`.
6. **Ghost 'index' column in CV metrics** — `cv_results.reset_index()` creates spurious `"index"` column treated as model. Add to exclude set.

---

## Validation Results

- ~5,800 total observations across 30 series
- HL volume total vs buckets: max diff 1.0 Mn EUR — **PASS**
- NFI volume total vs buckets: max diff 0.0 — **PASS**
- NFI rate weighted average: max diff 0.8 bps — **PASS**
- HL rate total (APRC) ≠ weighted average of rate buckets — **by design** (different metric)
- Forecast CV: 21 folds × 6 models × 5 series. MSTL won 4/5, CES won HL_Vol_10Y
- Full pipeline: all 10 dbt tests PASS, 60 forecast rows with CI bands
