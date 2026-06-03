# Helper functions for the Anycast Census dataset (github.com/ut-dacs/anycast-census)
# Data is also accessible via the manycast.net REST API — see repo README for details.

import io
import argparse
from datetime import datetime

import requests
import pandas as pd

"""
Usage example
-------------
import census_helper
from datetime import datetime

# Download a specific date
census = census_helper.download_date(datetime(2026, 2, 3), "v4")

# Or download the latest snapshot
census = census_helper.download_latest("v4")

# Filter to high-confidence anycast prefixes (default)
anycast = census_helper.filter_anycast(census, "v4")

# Or use comprehensive coverage (includes borderline cases)
anycast = census_helper.filter_anycast(census, "v4", confidence="comprehensive")
"""

_CENSUS_START = (2024, 3, 20)

# Confidence filters operate on the per-row max AB and GCD across all protocols.
# high:          (AB > 3) or (GCD > 1)  — recommended when accuracy matters
# comprehensive: (AB > 1) or (GCD > 1)  — recommended when completeness matters
CONFIDENCE_LEVELS = {
    "high":          lambda ab, gcd: (ab > 3) | (gcd > 1),
    "comprehensive": lambda ab, gcd: (ab > 1) | (gcd > 1),
}


def _fetch_parquet(url: str) -> pd.DataFrame:
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to download {url}: HTTP {response.status_code}")
    return pd.read_parquet(io.BytesIO(response.content))


def _max_ab_gcd(df: pd.DataFrame, version: str) -> tuple[pd.Series, pd.Series]:
    """Return the per-row max AB and max GCD across all protocols for the given IP version."""
    ab_cols  = [c for c in df.columns if c.startswith("AB_")  and c.endswith(version)]
    gcd_cols = [c for c in df.columns if c.startswith("GCD_") and c.endswith(version)]
    return df[ab_cols].max(axis=1), df[gcd_cols].max(axis=1)


def download_latest(version: str) -> pd.DataFrame:
    """Download the latest census snapshot.

    Args:
        version (str): 'v4' or 'v6'.

    Returns:
        pd.DataFrame: Latest census data.
    """
    ip_version = "IPv4" if version == "v4" else "IPv6"
    return _fetch_parquet(f"https://manycast.net/api/v1/export/{ip_version}-latest.parquet")


def download_date(date_obj: datetime, version: str) -> pd.DataFrame:
    """Download census data for a specific date.

    Args:
        date_obj (datetime): The day to fetch.
        version (str): 'v4' or 'v6'.

    Returns:
        pd.DataFrame: Census data for the specified date.
    """
    if (date_obj.year, date_obj.month, date_obj.day) < _CENSUS_START:
        raise ValueError(f"Date is before census start date of {'-'.join(str(x) for x in _CENSUS_START)}")
    if datetime(date_obj.year, date_obj.month, date_obj.day) > datetime.now():
        raise ValueError("Date is in the future")

    ip_version = "IPv4" if version == "v4" else "IPv6"
    date_str = f"{date_obj.year:04d}-{date_obj.month:02d}-{date_obj.day:02d}"
    return _fetch_parquet(f"https://manycast.net/api/v1/export/{ip_version}-{date_str}.parquet")


def filter_anycast(df: pd.DataFrame, version: str, confidence: str = "high") -> pd.DataFrame:
    """Filter a census DataFrame to anycast prefixes.

    Args:
        df (pd.DataFrame): Census DataFrame as returned by download_date() or download_latest().
        version (str): 'v4' or 'v6'.
        confidence (str):
            - 'high' (default): (max AB > 3) or (max GCD > 1) — accurate results.
            - 'comprehensive':  (max AB > 1) or (max GCD > 1) — includes borderline cases.

    Returns:
        pd.DataFrame: Rows passing the confidence filter.
    """
    if confidence not in CONFIDENCE_LEVELS:
        raise ValueError(f"Unknown confidence '{confidence}'. Choose from: {list(CONFIDENCE_LEVELS)}")
    ab_max, gcd_max = _max_ab_gcd(df, version)
    return df[CONFIDENCE_LEVELS[confidence](ab_max, gcd_max)]


def store_prefixes_only(ts: datetime, df: pd.DataFrame, version: str, output_path: str,
                        confidence: str = "high") -> None:
    """Write anycast prefixes to a CSV file (one prefix per line, no header).

    Args:
        ts (datetime): Snapshot date (used in the filename).
        df (pd.DataFrame): Full census DataFrame.
        version (str): 'v4' or 'v6'.
        output_path (str): Directory to write the output file.
        confidence (str): 'high' (default) or 'comprehensive'.
    """
    filtered = filter_anycast(df, version, confidence)
    filename = f"{output_path}/anycast_prefixes_{ts.year}_{ts.month:02d}_{ts.day:02d}_{version}_{confidence}.csv"
    filtered["prefix"].to_csv(filename, index=False, header=False)


def main(args):
    if args.date == "latest":
        datetime_obj = None
        df = download_latest(args.ip_version)
    else:
        datetime_obj = datetime.strptime(args.date, "%Y%m%d")
        df = download_date(datetime_obj, args.ip_version)

    output_path = args.output_dir or "."
    date_tag = "latest" if datetime_obj is None else f"{datetime_obj.year}_{datetime_obj.month:02d}_{datetime_obj.day:02d}"

    if args.prefixes_only:
        ts = datetime_obj or datetime.now()
        store_prefixes_only(ts, df, args.ip_version, output_path, args.confidence)
    else:
        filtered = filter_anycast(df, args.ip_version, args.confidence)
        filtered.to_csv(
            f"{output_path}/anycast_census_{date_tag}_{args.ip_version}_{args.confidence}.csv",
            index=False,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and filter Anycast Census snapshots.")
    parser.add_argument("--ip-version", required=True, choices=["v4", "v6"],
                        help="IP version: v4 or v6")
    parser.add_argument("--date", required=True, type=str,
                        help="Snapshot date as YYYYMMDD, or 'latest' for the most recent snapshot")
    parser.add_argument("--output-dir", required=False, type=str,
                        help="Output directory (default: current directory)")
    parser.add_argument("--prefixes-only", action="store_true",
                        help="Write only the anycast prefixes (one per line, no header)")
    parser.add_argument("--confidence", required=False, default="high",
                        choices=list(CONFIDENCE_LEVELS),
                        help="'high' (default, AB>3 or GCD>1) or 'comprehensive' (AB>1 or GCD>1)")
    main(parser.parse_args())
