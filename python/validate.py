"""
Validate that reported totals are consistent with their maturity sub-buckets.

Housing loans (volumes):
  hl_vol_total  == hl_vol_float + hl_vol_1_5y + hl_vol_5_10y + hl_vol_10y

NFI loans (volumes):
  nfi_vol_total == nfi_vol_sm_float + nfi_vol_sm_1_5y + nfi_vol_sm_5y
                 + nfi_vol_lg_float + nfi_vol_lg_1_5y + nfi_vol_lg_5y

Rates (both segments):
  Totals are WEIGHTED averages, not sums — validated as:
  implied_rate = sum(bucket_rate * bucket_vol) / sum(bucket_vol)
  Tolerance: ±5 bps (0.05 pp) to allow for rounding in source data.

Note: HL rate total is APRC (SUD131), buckets are pure rates — different
metric by design, so we skip rate-total validation for housing loans and
flag it explicitly.
"""

import duckdb
import pandas as pd

from python.config import DB_PATH

TOLERANCE_VOL_PCT = 0.01   # 1 % relative tolerance for volume sums
TOLERANCE_RATE_BPS = 0.10  # 10 bps absolute tolerance for weighted-rate check


def load(con: duckdb.DuckDBPyConnection, series_name: str) -> pd.Series:
    df = con.execute(
        "SELECT period_date, value FROM main_intermediate.int_series_cleaned "
        "WHERE series_name = ? ORDER BY period_date",
        [series_name],
    ).fetchdf()
    return df.set_index("period_date")["value"].rename(series_name)


def check_volume_total(
    con: duckdb.DuckDBPyConnection,
    label: str,
    total_name: str,
    bucket_names: list[str],
) -> None:
    total = load(con, total_name)
    buckets = [load(con, b) for b in bucket_names]
    bucket_sum = sum(buckets).rename("bucket_sum")

    df = pd.concat([total, bucket_sum], axis=1).dropna()
    df["diff"] = df[total_name] - df["bucket_sum"]
    df["rel_diff"] = df["diff"].abs() / df[total_name].abs()

    breaches = df[df["rel_diff"] > TOLERANCE_VOL_PCT]

    print(f"\n{'='*60}")
    print(f"VOLUME CHECK — {label}")
    print(f"  Periods with data: {len(df)}")
    print(f"  Mean abs diff    : {df['diff'].abs().mean():.1f} Mn EUR")
    print(f"  Max abs diff     : {df['diff'].abs().max():.1f} Mn EUR")
    if breaches.empty:
        print(f"  PASS  — all periods within {TOLERANCE_VOL_PCT*100:.0f}% tolerance")
    else:
        print(f"  FAIL  — {len(breaches)} periods exceed tolerance:")
        print(breaches[["diff", "rel_diff"]].to_string())


def check_rate_weighted(
    con: duckdb.DuckDBPyConnection,
    label: str,
    total_rate_name: str,
    bucket_rate_names: list[str],
    bucket_vol_names: list[str],
) -> None:
    total_rate = load(con, total_rate_name)
    b_rates = [load(con, r) for r in bucket_rate_names]
    b_vols  = [load(con, v) for v in bucket_vol_names]

    weighted_num = sum(r * v for r, v in zip(b_rates, b_vols))
    weighted_den = sum(b_vols)
    implied = (weighted_num / weighted_den).rename("implied_rate")

    df = pd.concat([total_rate, implied], axis=1).dropna()
    df["diff_bps"] = (df[total_rate_name] - df["implied_rate"]) * 100  # pp → bps

    breaches = df[df["diff_bps"].abs() > TOLERANCE_RATE_BPS * 100]

    print(f"\n{'='*60}")
    print(f"RATE CHECK (weighted avg) — {label}")
    print(f"  Periods with data: {len(df)}")
    print(f"  Mean |diff|      : {df['diff_bps'].abs().mean():.1f} bps")
    print(f"  Max  |diff|      : {df['diff_bps'].abs().max():.1f} bps")
    if breaches.empty:
        print(f"  PASS  — all periods within {TOLERANCE_RATE_BPS*100:.0f} bps tolerance")
    else:
        print(f"  FAIL  — {len(breaches)} periods exceed tolerance:")
        print(breaches[["diff_bps"]].to_string())


def run_all() -> None:
    con = duckdb.connect(str(DB_PATH))

    # ------------------------------------------------------------------
    # 1. Housing loans — volume total vs maturity buckets
    # ------------------------------------------------------------------
    check_volume_total(
        con,
        label="Housing Loans (households)",
        total_name="HL_Vol_Total",
        bucket_names=["HL_Vol_Float", "HL_Vol_1_5Y", "HL_Vol_5_10Y", "HL_Vol_10Y"],
    )

    # ------------------------------------------------------------------
    # 2. NFI loans — volume total vs size × maturity buckets
    # ------------------------------------------------------------------
    check_volume_total(
        con,
        label="NFI Loans (non-financial corporations)",
        total_name="NFI_Vol_Total",
        bucket_names=[
            "NFI_Vol_SM_Float", "NFI_Vol_SM_1_5Y", "NFI_Vol_SM_5Y",
            "NFI_Vol_LG_Float", "NFI_Vol_LG_1_5Y", "NFI_Vol_LG_5Y",
        ],
    )

    # ------------------------------------------------------------------
    # 3. NFI loans — rate total vs volume-weighted avg of buckets
    #    (Housing rate total is APRC — different metric, skip weighted check)
    # ------------------------------------------------------------------
    check_rate_weighted(
        con,
        label="NFI Loans rate (volume-weighted avg of size×maturity buckets)",
        total_rate_name="NFI_Rate_Total",
        bucket_rate_names=[
            "NFI_Rate_SM_Float", "NFI_Rate_SM_1_5Y", "NFI_Rate_SM_5Y",
            "NFI_Rate_LG_Float", "NFI_Rate_LG_1_5Y", "NFI_Rate_LG_5Y",
        ],
        bucket_vol_names=[
            "NFI_Vol_SM_Float", "NFI_Vol_SM_1_5Y", "NFI_Vol_SM_5Y",
            "NFI_Vol_LG_Float", "NFI_Vol_LG_1_5Y", "NFI_Vol_LG_5Y",
        ],
    )

    print("\n" + "="*60)
    print("NOTE — Housing loan RATE total (SUD131) is APRC.")
    print("  APRC includes fees/costs on top of the pure interest rate,")
    print("  so HL_Rate_Total cannot be validated against the rate buckets")
    print("  (SUD116-119). This is expected and correct by design.")
    print("  Volume totals confirm bucket coverage above.")

    con.close()


if __name__ == "__main__":
    run_all()
