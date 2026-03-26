from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "macropipe.duckdb"
RAW_SCHEMA = "raw"

BUNDESBANK_BASE_URL = "https://api.statistiken.bundesbank.de/rest/data"

# Series registry: table_name -> (flow_ref, key, start_period)
SERIES_REGISTRY: dict[str, tuple[str, str, str]] = {
    "gdp":            ("BBNZ1", "Q.DE.Y.H.0000.A",                              "2010-Q1"),
    "inflation":      ("BBDP1", "M.DE.Y.HVPI.C.A00000.I.A",                     "2010-01"),
    "ecb_mro":        ("BBIN1", "M.D0.ECB.ECBMIN.EUR.ME",                        "2010-01"),
    "ecb_lower":      ("BBIN1", "M.D0.ECB.ECBFAC.EUR.ME",                        "2010-01"),
    "ecb_upper":      ("BBIN1", "M.D0.ECB.ECBREF.EUR.ME",                        "2010-01"),
    "yield_2y":       ("BBSIS", "M.I.ZST.ZI.EUR.S1311.B.A604.R02XX.R.A.A._Z._Z.A", "2010-01"),
    "euribor_3m":     ("BBIG1", "M.D0.EUR.MMKT.EURIBOR.M03.AVE.MA",             "2010-01"),
    "yield_10y":      ("BBSIS", "M.I.ZST.ZI.EUR.S1311.B.A604.R10XX.R.A.A._Z._Z.A", "2010-01"),
}
