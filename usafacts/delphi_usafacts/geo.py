# -*- coding: utf-8 -*-
"""Functions for converting geocodes."""
import pandas as pd

from delphi_utils import GeoMapper

INCIDENCE_BASE = 100000

SECONDARY_FIPS = [
    ("51620", ["51093", "51175"]),
    ("51685", ["51153"]),
    ("28039", ["28059", "28041", "28131", "28045", "28059", "28109", "28047"]),
    ("51690", ["51089", "51067"]),
    ("51595", ["51081", "51025", "51175", "51183"]),
    ("51600", ["51059", "51059", "51059"]),
    ("51580", ["51005"]),
    ("51678", ["51163"]),
]
NYC_FIPS = [
    ("00001", ["36061", "36005", "36047", "36081", "36085"])
]
REPLACE_FIPS = [
    ("02158", "02270"),
    ("46102", "46113"),
]


# Valid geographical resolutions output by this indicator.
VALID_GEO_RES = ("county", "state", "msa", "hrr", "hhs", "nation")
# Sensors that report proportions.  For geo resolutions with unallocated cases
# or deaths, we avoid reporting these sensors.
PROP_SENSORS = ("incidence", "cumulative_prop")


def disburse(df: pd.DataFrame, pooled_fips: str, fips_list: list):
    """Disburse counts from POOLED_FIPS equally to the counties in FIPS_LIST.

    Parameters
    ----------
    df: pd.DataFrame
        Columns: fips, timestamp, new_counts, cumulative_counts, ...
    pooled_fips: str
        FIPS of county from which to disburse counts
    fips_list: list[str]
        FIPS of counties to which to disburse counts.

    Results
    -------
    pd.DataFrame
        Dataframe with same schema as df, with the counts disbursed.
    """
    cols = ["new_counts", "cumulative_counts"]
    df = df.copy().sort_values(["fips", "timestamp"])
    for col in cols:
        # Get values from the aggregated county:
        vals = df.loc[df["fips"] == pooled_fips, col].values / len(fips_list)
        for fips in fips_list:
            df.loc[df["fips"] == fips, col] += vals
    return df


def geo_map(df: pd.DataFrame, geo_res: str, sensor: str):
    """
    Map a DataFrame with county level data and aggregate it to the geographic resolution geo_res.

    Parameters
    ----------
    df: pd.DataFrame
        Columns: fips, timestamp, new_counts, cumulative_counts, population ...
    geo_res: str
        Geographic resolution to which to aggregate.  Valid options:
        ("county", "state", "msa", "hrr", "hhs", "nation").
    sensor: str
        sensor type. Valid options:
        ("new_counts", "cumulative_counts",
        "incidence", "cumulative_prop")

    Returns
    -------
    pd.DataFrame
        Columns: geo_id, timestamp, ...
    """
    if geo_res not in VALID_GEO_RES:
        raise ValueError(f"geo_res must be one of {VALID_GEO_RES}")

    # State-level records unassigned to specific counties are coded as fake
    # counties with fips XX000.
    unassigned_counties = df[df["fips"].str.endswith("000")].copy()

    df = df[df["fips"].astype(int) % 1000 != 0].copy()
    # Disburse unallocated cases/deaths in NYC to NYC counties
    df = disburse(df, NYC_FIPS[0][0], NYC_FIPS[0][1])
    df = df[df["fips"] != NYC_FIPS[0][0]]
    gmpr = GeoMapper()
    if geo_res == "county":
        if sensor not in PROP_SENSORS:
            # It is not clear how to calculate the proportion for unallocated
            # cases/deaths, so we exclude them for those sensors.
            df = df.append(unassigned_counties)
        df.rename({"fips": "geo_id"}, inplace=True, axis=1)
    elif geo_res in ("state", "hhs", "nation"):
        state_geo = "state_id" if geo_res == "state" else "state_code"
        df = df.append(unassigned_counties)
        df = gmpr.replace_geocode(df, "fips", state_geo, new_col="geo_id", date_col="timestamp")
        df.drop("population", axis=1, errors="ignore", inplace=True)
        df = gmpr.add_population_column(df, state_geo, geocode_col="geo_id")
        if geo_res in ("hhs", "nation"):
            # for hhs/nation, use reported state populations instead of nation since PR not reported
            df = gmpr.replace_geocode(df, state_geo, geo_res,
                                      from_col="geo_id", date_col="timestamp")
            df.rename({geo_res: "geo_id"}, inplace=True, axis=1)
    else:
        # Map "missing" secondary FIPS to those that are in our canonical set
        for fips, fips_list in SECONDARY_FIPS:
            df = disburse(df, fips, fips_list)
        for usafacts_fips, our_fips in REPLACE_FIPS:
            df.loc[df["fips"] == usafacts_fips, "fips"] = our_fips
        merged = gmpr.replace_geocode(df, "fips", geo_res, new_col="geo_id", date_col="timestamp")
        if "weight" not in merged.columns:
            merged["weight"] = 1
        merged["cumulative_counts"] = merged["cumulative_counts"] * merged["weight"]
        merged["new_counts"] = merged["new_counts"] * merged["weight"]
        merged["population"] = merged["population"] * merged["weight"]
        df = merged.drop(["weight"], axis=1)
    df = df.groupby(["geo_id", "timestamp"]).sum().reset_index()
    df["incidence"] = df["new_counts"] / df["population"] * INCIDENCE_BASE
    df["cumulative_prop"] = df["cumulative_counts"] / df["population"] * INCIDENCE_BASE
    return df
