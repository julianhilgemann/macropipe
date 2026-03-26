"""
Forecasting module for housing loan new business volumes.

Approach:
  1. For each series, run multiple models (AutoARIMA, AutoETS, AutoTheta,
     CES, MSTL, Seasonal Naive as baseline).
  2. Time series cross-validation with expanding window:
     - Training starts at 60 observations minimum
     - Test window = 12 months (= forecast horizon)
     - Step = 6 months between CV folds
  3. Compare models on MAE, RMSE, MAPE, SMAPE across all CV folds.
  4. Select the best model per series (lowest RMSE, tie-break on MAE).
  5. Refit best model on full history, produce 12-month forecast with
     90% and 95% prediction intervals.
  6. Store forecasts + CV metrics + metadata in DuckDB forecast schema.
"""

import json
from datetime import datetime, timezone

import duckdb
import numpy as np
import pandas as pd
from statsforecast import StatsForecast
from statsforecast.models import (
    AutoARIMA,
    AutoCES,
    AutoETS,
    AutoTheta,
    MSTL,
    SeasonalNaive,
)

from python.config import DB_PATH

# ── Configuration ────────────────────────────────────────────────────────────

FORECAST_HORIZON = 12       # months ahead
CV_MIN_TRAIN = 60           # minimum observations before first CV fold
CV_STEP = 6                 # months between CV folds
SEASON_LENGTH = 12          # monthly seasonality
CONFIDENCE_LEVELS = [90, 95]

FORECAST_SCHEMA = "forecast"

# Series to forecast (housing loan new-business volume buckets)
HL_VOL_SERIES = [
    "HL_Vol_Total",
    "HL_Vol_Float",
    "HL_Vol_1_5Y",
    "HL_Vol_5_10Y",
    "HL_Vol_10Y",
]


def _build_models():
    """Return the candidate model list."""
    return [
        AutoARIMA(season_length=SEASON_LENGTH),
        AutoETS(season_length=SEASON_LENGTH),
        AutoTheta(season_length=SEASON_LENGTH),
        AutoCES(season_length=SEASON_LENGTH),
        MSTL(season_length=SEASON_LENGTH),
        SeasonalNaive(season_length=SEASON_LENGTH),
    ]


# ── CV and model selection ───────────────────────────────────────────────────

def _load_series(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Load the housing volume series from the intermediate layer."""
    placeholders = ", ".join(["?"] * len(HL_VOL_SERIES))
    df = con.execute(
        f"SELECT series_name AS unique_id, period_date AS ds, value AS y "
        f"FROM main_intermediate.int_series_cleaned "
        f"WHERE series_name IN ({placeholders}) "
        f"ORDER BY series_name, period_date",
        HL_VOL_SERIES,
    ).fetchdf()
    df["ds"] = pd.to_datetime(df["ds"])
    return df


def _run_crossvalidation(df: pd.DataFrame) -> pd.DataFrame:
    """Run expanding-window time series CV across all candidate models."""
    models = _build_models()
    sf = StatsForecast(models=models, freq="MS", n_jobs=1)

    n_windows = max(1, (len(df[df["unique_id"] == df["unique_id"].iloc[0]]) - CV_MIN_TRAIN - FORECAST_HORIZON) // CV_STEP + 1)

    cv_results = sf.cross_validation(
        df=df,
        h=FORECAST_HORIZON,
        step_size=CV_STEP,
        n_windows=n_windows,
    )
    return cv_results.reset_index()


def _compute_cv_metrics(cv: pd.DataFrame) -> pd.DataFrame:
    """Compute error metrics per (series, model) from CV results."""
    exclude = {"unique_id", "ds", "y", "cutoff", "index"}
    model_cols = [
        c for c in cv.columns
        if c not in exclude
        and not c.startswith("lo-")
        and not c.startswith("hi-")
    ]

    rows = []
    for uid in cv["unique_id"].unique():
        cv_uid = cv[cv["unique_id"] == uid]
        for model in model_cols:
            y_true = cv_uid["y"].values
            y_pred = cv_uid[model].values
            mask = np.isfinite(y_true) & np.isfinite(y_pred)
            y_t, y_p = y_true[mask], y_pred[mask]
            if len(y_t) == 0:
                continue

            errors = y_t - y_p
            abs_errors = np.abs(errors)
            pct_errors = abs_errors / np.maximum(np.abs(y_t), 1e-8)
            smape_vals = 2 * abs_errors / (np.abs(y_t) + np.abs(y_p) + 1e-8)

            rows.append({
                "series_name": uid,
                "model_name": model,
                "mae": float(np.mean(abs_errors)),
                "rmse": float(np.sqrt(np.mean(errors ** 2))),
                "mape": float(np.mean(pct_errors) * 100),
                "smape": float(np.mean(smape_vals) * 100),
                "cv_folds": int(cv_uid["cutoff"].nunique()),
                "cv_observations": int(len(y_t)),
            })

    metrics = pd.DataFrame(rows)
    return metrics


def _select_best_model(metrics: pd.DataFrame) -> pd.DataFrame:
    """Pick best model per series: lowest RMSE, tie-break on MAE."""
    best = (
        metrics
        .sort_values(["series_name", "rmse", "mae"])
        .groupby("series_name")
        .first()
        .reset_index()
    )
    return best


# ── Final forecast ───────────────────────────────────────────────────────────

def _produce_forecasts(
    df: pd.DataFrame, best: pd.DataFrame
) -> tuple[pd.DataFrame, dict]:
    """Refit best model per series on full data, produce forecasts with PIs."""
    all_forecasts = []
    model_params = {}

    for _, row in best.iterrows():
        uid = row["series_name"]
        model_name = row["model_name"]
        series_df = df[df["unique_id"] == uid].copy()

        # Re-instantiate only the winning model
        # statsforecast names columns by repr(model), e.g. AutoCES -> "CES"
        model_map = {repr(m).split("(")[0]: m for m in _build_models()}
        chosen_model = model_map[model_name]

        sf = StatsForecast(
            models=[chosen_model],
            freq="MS",
            n_jobs=1,
        )
        forecast = sf.forecast(
            df=series_df,
            h=FORECAST_HORIZON,
            level=CONFIDENCE_LEVELS,
        ).reset_index()

        forecast["series_name"] = uid
        forecast["model_name"] = model_name

        # Rename model column to 'value'
        forecast = forecast.rename(columns={model_name: "value", "ds": "period_date"})

        # Rename confidence interval columns to standard names
        for lvl in CONFIDENCE_LEVELS:
            lo_col = f"{model_name}-lo-{lvl}"
            hi_col = f"{model_name}-hi-{lvl}"
            if lo_col in forecast.columns:
                forecast = forecast.rename(columns={
                    lo_col: f"ci_lower_{lvl}",
                    hi_col: f"ci_upper_{lvl}",
                })

        keep_cols = ["series_name", "period_date", "value", "model_name"]
        keep_cols += [c for c in forecast.columns if c.startswith("ci_")]
        forecast = forecast[[c for c in keep_cols if c in forecast.columns]]

        all_forecasts.append(forecast)
        model_params[uid] = {
            "model_name": model_name,
            "rmse": float(row["rmse"]),
            "mae": float(row["mae"]),
            "mape": float(row["mape"]),
            "smape": float(row["smape"]),
        }

    return pd.concat(all_forecasts, ignore_index=True), model_params


# ── Persistence ──────────────────────────────────────────────────────────────

def _store_results(
    con: duckdb.DuckDBPyConnection,
    forecasts: pd.DataFrame,
    cv_metrics: pd.DataFrame,
    model_params: dict,
    run_ts: str,
) -> None:
    """Write forecast results, CV metrics, and run metadata to DuckDB."""
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {FORECAST_SCHEMA}")

    # 1. Forecasts
    forecasts["forecast_run_at"] = run_ts
    con.execute(
        f"CREATE OR REPLACE TABLE {FORECAST_SCHEMA}.hl_vol_forecasts "
        f"AS SELECT * FROM forecasts"
    )

    # 2. CV metrics
    cv_metrics["forecast_run_at"] = run_ts
    con.execute(
        f"CREATE OR REPLACE TABLE {FORECAST_SCHEMA}.hl_vol_cv_metrics "
        f"AS SELECT * FROM cv_metrics"
    )

    # 3. Run metadata
    meta_rows = []
    for series, params in model_params.items():
        meta_rows.append({
            "series_name": series,
            "forecast_run_at": run_ts,
            "horizon": FORECAST_HORIZON,
            "season_length": SEASON_LENGTH,
            "cv_min_train": CV_MIN_TRAIN,
            "cv_step": CV_STEP,
            "confidence_levels": json.dumps(CONFIDENCE_LEVELS),
            "selected_model": params["model_name"],
            "model_rmse": params["rmse"],
            "model_mae": params["mae"],
            "model_mape": params["mape"],
            "model_smape": params["smape"],
        })
    meta_df = pd.DataFrame(meta_rows)
    con.execute(
        f"CREATE OR REPLACE TABLE {FORECAST_SCHEMA}.hl_vol_run_metadata "
        f"AS SELECT * FROM meta_df"
    )


# ── Entry point ──────────────────────────────────────────────────────────────

def run_hl_vol_forecast() -> None:
    """Full forecast pipeline for housing loan volume series."""
    run_ts = datetime.now(timezone.utc).isoformat()
    con = duckdb.connect(str(DB_PATH))

    try:
        print("Loading housing loan volume series...")
        df = _load_series(con)
        print(f"  {df['unique_id'].nunique()} series, {len(df)} total observations")

        print("Running time series cross-validation...")
        cv = _run_crossvalidation(df)

        print("Computing CV error metrics...")
        cv_metrics = _compute_cv_metrics(cv)
        best = _select_best_model(cv_metrics)

        print("\nBest model per series:")
        for _, row in best.iterrows():
            print(f"  {row['series_name']:20s}  →  {row['model_name']:15s}  "
                  f"RMSE={row['rmse']:8.1f}  MAE={row['mae']:8.1f}  "
                  f"MAPE={row['mape']:5.1f}%  SMAPE={row['smape']:5.1f}%")

        print(f"\nProducing {FORECAST_HORIZON}-month forecasts with "
              f"{CONFIDENCE_LEVELS} prediction intervals...")
        forecasts, model_params = _produce_forecasts(df, best)

        print("Storing results in DuckDB forecast schema...")
        _store_results(con, forecasts, cv_metrics, model_params, run_ts)

        print(f"\nDone. {len(forecasts)} forecast rows written to "
              f"{FORECAST_SCHEMA}.hl_vol_forecasts")
        print(f"CV metrics in {FORECAST_SCHEMA}.hl_vol_cv_metrics")
        print(f"Run metadata in {FORECAST_SCHEMA}.hl_vol_run_metadata")

    finally:
        con.close()


if __name__ == "__main__":
    run_hl_vol_forecast()
