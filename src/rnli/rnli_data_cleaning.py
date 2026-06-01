"""
Functions for cleaning and preprocessing RNLI lifeboat callout data.

Functions included:
    - clean_rnli_data
        Main cleaning function.
        
Author: CL
"""

# Global imports
import os

import polars as pl
# Local imports
from utils.support_functions import get_data_files

def clean_rnli_data():
    """
    Main cleaning function.

    Filters the lifeboat launch data for records relevant to Cornwall. Adds 
    flags based on whether service has been cancelled and whether delay was
    weather related.
    Saves cleaned data to csv file in processed data directory.

    Parameters
    -----------

    Returns
    ----------
    None

    Notes
    ---------
    """
    # Get list of raw data files
    files = get_data_files("data/raw/rnli")
    
    # Load data from files
    raw_df = pl.read_csv(files[0], infer_schema=False)
    
    # filter for records where station is in Cornwall
    cornwall_stations = pl.read_csv(
        "data/inputs/rnli/cornwall_lifeboat_stations.csv"
        )
    raw_df = raw_df.filter(
        pl.col("LifeboatStationNameProper").is_in(
            cornwall_stations["LIFEBOAT_STATION"]
            )
    )
    # filter for just cols needed (station and data of launch)
    raw_df = raw_df.select(["LifeboatStationNameProper", "Date of Launch"])
    
    # rename cols
    raw_df = raw_df.rename({
        "LifeboatStationNameProper": "STATION",
        "Date of Launch": "DATE"
    })
    # format date column
    raw_df = raw_df.with_columns(
        pl.col("DATE").str.strptime(pl.Date, format="%Y-%m-%d")
    )
    raw_df = raw_df.sort("DATE")
    # format as counts per day
    raw_df = raw_df.group_by("DATE").agg(pl.count("STATION").alias("CALL_OUTS"))
    
    # save cleaned data to csv
    if not os.path.exists("data/processed/rnli"):
        os.makedirs("data/processed/rnli")
    raw_df.write_csv("data/processed/rnli/cleaned_rnli_data.csv")