"""
Functions for cleaning and preprocessing Cornwall Council data sets.

Functions included:
    - clean_cc_collisions_data
        Main cleaning function.
        
Author: CL
"""

# Global imports
import os
import polars as pl

# Local imports
from utils.support_functions import get_data_files, load_data


def clean_cc_collisions_data():
    """
    Main cleaning function.

    Adds flags based on whether the collision might have been weather related.
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
    files = get_data_files("data/raw/cornwall_council/collisions")
    
    # Load data from files
    raw_df = pl.read_ods(files[0], sheet_id=2)
   
    # filter for records where station is in Cornwall
    weather_flags = pl.read_csv(
        "data/inputs/cornwall_council/collisions/weather_flags.csv"
        )
    # Merge with weather flags
    raw_df = raw_df.join(
        weather_flags,
        left_on="Weather",
        right_on="WEATHER_TYPE",
        how="inner"
    )
    
    raw_df = raw_df.select(["date", "IS_ADVERSE_WEATHER"])
    # format as counts per days
    raw_df = raw_df.group_by(
        ("date", "IS_ADVERSE_WEATHER")
        ).agg(pl.count("IS_ADVERSE_WEATHER").alias("COLLISIONS"))
    
    # rename
    raw_df = raw_df.rename({
        "date": "DATE"
    })
    # sort
    raw_df = raw_df.sort("DATE")
    # save cleaned data to csv
    if not os.path.exists("data/processed/cornwall_council/collisions"):
        os.makedirs("data/processed/cornwall_council/collisions")
    raw_df.write_csv("data/processed/cornwall_council/collisions/cleaned_collisions_data.csv")