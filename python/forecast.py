"""Forecasting module — called by dbt Python models via dbt-duckdb."""

import pandas as pd
from statsforecast import StatsForecast
from statsforecast.models import AutoARIMA, AutoETS, AutoTheta


FORECAST_HORIZON = 12  # periods ahead


def run_forecast(
    df: pd.DataFrame,
    id_col: str = "series_name",
    date_col: str = "period_date",
    value_col: str = "value",
    season_length: int = 12,
    freq: str = "MS",
    horizon: int = FORECAST_HORIZON,
) -> pd.DataFrame:
    """Run AutoARIMA, AutoETS, AutoTheta on a tidy time series DataFrame.

    Returns a DataFrame with one row per (series, future date, model).
    """
    models = [
        AutoARIMA(season_length=season_length),
        AutoETS(season_length=season_length),
        AutoTheta(season_length=season_length),
    ]

    sf_df = df[[id_col, date_col, value_col]].rename(
        columns={id_col: "unique_id", date_col: "ds", value_col: "y"}
    )
    sf_df["ds"] = pd.to_datetime(sf_df["ds"])
    sf_df = sf_df.dropna(subset=["y"]).sort_values(["unique_id", "ds"])

    sf = StatsForecast(models=models, freq=freq, n_jobs=1)
    forecasts = sf.forecast(df=sf_df, h=horizon).reset_index()

    forecasts = forecasts.rename(columns={"unique_id": id_col, "ds": date_col})
    return forecasts
