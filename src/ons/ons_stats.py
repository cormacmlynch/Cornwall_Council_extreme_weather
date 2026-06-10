"""
Functions for cleaning and preprocessing ONS excess mortality data.
Functions included:
    - clean_ons_mortality_data
        Main cleaning function.
        
Author: CL
"""

# Global imports
import os
import polars as pl

def get_ons_mortality_stats():
    """
    Main function for calculating ONS excess mortality statistics.

    Loads ONS excess mortality data from raw xlsx file, filters for Cornwall, 
    and calculates statistics for the filtered data.

    directory.

    Parameters
    -----------

    Returns
    ----------
    stats: dict
        Dictionary containing the following keys:
        excess_deaths_heat_periods: int
            Total excess deaths during heat periods in Cornwall.
        excess_deaths_entire_period: float
            Total excess deaths during the entire period in Cornwall.

    Notes
    ---------
    """
    # Load raw data
    # read column indicies 0-94
    # data for this table ends on row 297
    raw_df = pl.read_excel(
        "data/raw/ons/excessmortalityduringheatperiods.xlsx",
        sheet_name="4",
        read_options={"header_row": 5, "n_rows": 130},
        columns=list(range(95))
        )

    # filter for Cornwall records
    raw_df = raw_df.filter(
        pl.col("Area of usual residence").str.contains("Cornwall")
        )

    # all value columns (entire month)
    value_cols = [c for c in raw_df.columns if c != "Area of usual residence"]
    excess_deaths_entire_period = (
        raw_df
        .select(
            pl.sum_horizontal(
                [pl.col(c).cast(pl.Float64, strict=False).fill_null(0) for c in value_cols]
            ).alias("ROW_TOTAL")
        )
        .select(pl.col("ROW_TOTAL").sum().alias("EXCESS_DEATHS_ENTIRE_PERIOD"))
        .item()
    )

    # columns for heat-period days only
    heat_cols = [col for col in value_cols if "heat-period" in col.lower()]

    if heat_cols:
        excess_deaths_heat_periods = (
            raw_df
            .select(
                pl.sum_horizontal(
                    [pl.col(c).cast(pl.Float64, strict=False).fill_null(0) for c in heat_cols]
                ).alias("ROW_TOTAL_HEAT")
            )
            .select(pl.col("ROW_TOTAL_HEAT").sum().alias("EXCESS_DEATHS_HEAT_PERIODS"))
            .item()
        )
    else:
        excess_deaths_heat_periods = 0.0

    return {
        "excess deaths June-August 2022": excess_deaths_entire_period,
        "excess deaths during heat periods only": excess_deaths_heat_periods,
    }