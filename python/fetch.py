"""Fetch time series from the Bundesbank SDMX API (XML) and store raw data in DuckDB."""

import duckdb
import pandas as pd
import requests
from lxml import etree

from python.config import BUNDESBANK_BASE_URL, DB_PATH, RAW_SCHEMA, SERIES_REGISTRY

NS = {
    "message": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    "generic": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic",
}


def fetch_series(flow_ref: str, key: str, start: str) -> pd.DataFrame:
    """Fetch a single time series via SDMX generic XML."""
    url = f"{BUNDESBANK_BASE_URL}/{flow_ref}/{key}"
    params = {
        "startPeriod": start,
        "format": "sdmx_generic_xml",
        "lang": "de",
        "detail": "full",
    }

    print(f"  GET {url}")
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()

    tree = etree.fromstring(resp.content)
    rows: list[dict] = []

    for series in tree.xpath("//generic:Series", namespaces=NS):
        metadata = {
            val.get("id"): val.get("value")
            for val in series.xpath("./generic:SeriesKey/generic:Value", namespaces=NS)
        }
        for obs in series.xpath("./generic:Obs", namespaces=NS):
            time_dim = obs.xpath("./generic:ObsDimension", namespaces=NS)[0].get("value")
            obs_val = obs.xpath("./generic:ObsValue", namespaces=NS)[0].get("value")
            row = {
                "time_period": time_dim,
                "value": float(obs_val) if obs_val else None,
            }
            row.update({k.lower(): v for k, v in metadata.items()})
            rows.append(row)

    df = pd.DataFrame(rows)
    df["_loaded_at"] = pd.Timestamp.now(tz="UTC")
    return df


def store_raw(df: pd.DataFrame, table_name: str) -> None:
    """Write a DataFrame into the raw schema in DuckDB."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
        con.execute(
            f"CREATE OR REPLACE TABLE {RAW_SCHEMA}.{table_name} AS SELECT * FROM df"
        )
    finally:
        con.close()


def ingest() -> None:
    """Fetch and store all series defined in the registry."""
    for table_name, (flow_ref, key, start) in SERIES_REGISTRY.items():
        print(f"Fetching {flow_ref}/{key} -> raw.{table_name}")
        df = fetch_series(flow_ref, key, start)
        store_raw(df, table_name)
        print(f"  stored {len(df)} rows")


if __name__ == "__main__":
    ingest()
