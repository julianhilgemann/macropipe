from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "macropipe.duckdb"
RAW_SCHEMA = "raw"

BUNDESBANK_BASE_URL = "https://api.statistiken.bundesbank.de/rest/data"

# Series registry: table_name -> (flow_ref, key, start_period)
# Naming convention:
#   hl_  = housing loans (households)
#   nfi_ = loans to non-financial corporations
#   _rate_ = effective interest rate / APRC
#   _vol_  = new business volume
#   _total = aggregate across all buckets
#   _float = floating rate or initial fixation ≤ 1Y
#   _1_5y  = initial rate fixation > 1Y and ≤ 5Y
#   _5_10y = initial rate fixation > 5Y and ≤ 10Y
#   _10y   = initial rate fixation > 10Y
#   _sm_   = loan size ≤ EUR 1M
#   _lg_   = loan size > EUR 1M

SERIES_REGISTRY: dict[str, tuple[str, str, str]] = {

    # ------------------------------------------------------------------
    # Macro context
    # ------------------------------------------------------------------
    "gdp":               ("BBNZ1", "Q.DE.Y.H.0000.A",                                 "2010-Q1"),
    "inflation":         ("BBDP1", "M.DE.Y.HVPI.C.A00000.I.A",                        "2010-01"),
    "ecb_mro":           ("BBIN1", "M.D0.ECB.ECBMIN.EUR.ME",                           "2010-01"),
    "ecb_lower":         ("BBIN1", "M.D0.ECB.ECBFAC.EUR.ME",                           "2010-01"),
    "ecb_upper":         ("BBIN1", "M.D0.ECB.ECBREF.EUR.ME",                           "2010-01"),
    "yield_2y":          ("BBSIS", "M.I.ZST.ZI.EUR.S1311.B.A604.R02XX.R.A.A._Z._Z.A", "2010-01"),
    "euribor_3m":        ("BBIG1", "M.D0.EUR.MMKT.EURIBOR.M03.AVE.MA",                "2010-01"),
    "yield_10y":         ("BBSIS", "M.I.ZST.ZI.EUR.S1311.B.A604.R10XX.R.A.A._Z._Z.A", "2010-01"),

    # ------------------------------------------------------------------
    # Housing loans to households — effective interest rate, new business
    # Total is APRC (SUD131); buckets are pure rates (SUD116-119)
    # Note: APRC total ≠ simple average of rate buckets (different metric)
    # Volume total DOES equal sum of volume buckets
    # ------------------------------------------------------------------
    "hl_rate_total":     ("BBIM1", "M.DE.B.A2C.A.C.A.2250.EUR.N",  "2010-01"),  # SUD131  APRC
    "hl_rate_float":     ("BBIM1", "M.DE.B.A2C.F.R.A.2250.EUR.N",  "2010-01"),  # SUD116  float/≤1Y
    "hl_rate_1_5y":      ("BBIM1", "M.DE.B.A2C.I.R.A.2250.EUR.N",  "2010-01"),  # SUD117  >1Y–5Y
    "hl_rate_5_10y":     ("BBIM1", "M.DE.B.A2C.O.R.A.2250.EUR.N",  "2010-01"),  # SUD118  >5Y–10Y
    "hl_rate_10y":       ("BBIM1", "M.DE.B.A2C.P.R.A.2250.EUR.N",  "2010-01"),  # SUD119  >10Y

    # Housing loans to households — new business volume
    "hl_vol_total":      ("BBIM1", "M.DE.B.A2C.A.B.A.2250.EUR.N",  "2010-01"),  # SUD231  total
    "hl_vol_float":      ("BBIM1", "M.DE.B.A2C.F.B.A.2250.EUR.N",  "2010-01"),  # SUD216  float/≤1Y
    "hl_vol_1_5y":       ("BBIM1", "M.DE.B.A2C.I.B.A.2250.EUR.N",  "2010-01"),  # SUD217  >1Y–5Y
    "hl_vol_5_10y":      ("BBIM1", "M.DE.B.A2C.O.B.A.2250.EUR.N",  "2010-01"),  # SUD218  >5Y–10Y
    "hl_vol_10y":        ("BBIM1", "M.DE.B.A2C.P.B.A.2250.EUR.N",  "2010-01"),  # SUD219  >10Y

    # ------------------------------------------------------------------
    # NFI loans (non-financial corporations) — effective interest rate, new business
    # ------------------------------------------------------------------
    "nfi_rate_total":    ("BBIM1", "M.DE.B.A2A.A.R.A.2240.EUR.N",  "2010-01"),  # SUD939A total
    "nfi_rate_sm_float": ("BBIM1", "M.DE.B.A2A.F.R.0.2240.EUR.N",  "2010-01"),  # SUD124  ≤1M float
    "nfi_rate_sm_1_5y":  ("BBIM1", "M.DE.B.A2A.I.R.0.2240.EUR.N",  "2010-01"),  # SUD125  ≤1M 1–5Y
    "nfi_rate_sm_5y":    ("BBIM1", "M.DE.B.A2A.J.R.0.2240.EUR.N",  "2010-01"),  # SUD126  ≤1M >5Y
    "nfi_rate_lg_float": ("BBIM1", "M.DE.B.A2A.F.R.1.2240.EUR.N",  "2010-01"),  # SUD127  >1M float
    "nfi_rate_lg_1_5y":  ("BBIM1", "M.DE.B.A2A.I.R.1.2240.EUR.N",  "2010-01"),  # SUD128  >1M 1–5Y
    "nfi_rate_lg_5y":    ("BBIM1", "M.DE.B.A2A.J.R.1.2240.EUR.N",  "2010-01"),  # SUD129  >1M >5Y

    # NFI loans — new business volume
    "nfi_vol_total":     ("BBIM1", "M.DE.B.A2A.A.B.A.2240.EUR.N",  "2010-01"),  # SUD949A total
    "nfi_vol_sm_float":  ("BBIM1", "M.DE.B.A2A.F.B.0.2240.EUR.N",  "2010-01"),  # SUD224  ≤1M float
    "nfi_vol_sm_1_5y":   ("BBIM1", "M.DE.B.A2A.I.B.0.2240.EUR.N",  "2010-01"),  # SUD225  ≤1M 1–5Y
    "nfi_vol_sm_5y":     ("BBIM1", "M.DE.B.A2A.J.B.0.2240.EUR.N",  "2010-01"),  # SUD226  ≤1M >5Y
    "nfi_vol_lg_float":  ("BBIM1", "M.DE.B.A2A.F.B.1.2240.EUR.N",  "2010-01"),  # SUD227  >1M float
    "nfi_vol_lg_1_5y":   ("BBIM1", "M.DE.B.A2A.I.B.1.2240.EUR.N",  "2010-01"),  # SUD228  >1M 1–5Y
    "nfi_vol_lg_5y":     ("BBIM1", "M.DE.B.A2A.J.B.1.2240.EUR.N",  "2010-01"),  # SUD229  >1M >5Y
}
