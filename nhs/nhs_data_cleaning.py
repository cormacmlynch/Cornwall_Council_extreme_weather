"""
Functions for cleaning and preprocessing data from NHS England.
Functions included:
    - clean_iuc_data
        Main cleaning function.
        
Author: CL
"""
# global imports
import os
import polars as pl

# Local imports
from utils.support_functions import get_data_files, load_data


def clean_iuc_data():
    """
    Cleans the raw Integrated Urgent Care data from NHS England.
    
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
    files = get_data_files("nhs/raw_data/iuc")
    
    # Load data from files
    raw_df = load_data(files, data_type = 'iuc')
    
    # Filter for A01 codes (all NHS 111 calls)
    filtered_df = raw_df.filter(pl.col("ITEM_NUMBER") == "A01")
    # Filter for cornwall trust
    cornwall_names = ["Cornwall (HUC)", "Cornwall", "Cornwall 111AF1"]
    filtered_df = filtered_df.filter(
        pl.col("CONTRACT_NAME").is_in(cornwall_names)
        )
    # format date column
    # fix some errors in date column raw data
    date_errors_dict = {
        "44864": "30/10/2022",
        "44865": "31/10/2022"
    }
    # replace errors in date column
    filtered_df = filtered_df.with_columns(
        DATE=pl.col("DATE").replace(date_errors_dict))

    filtered_df = filtered_df.with_columns(
        pl.col("DATE").str.strptime(pl.Date, format="%d/%m/%Y")
    )
    filtered_df = filtered_df.sort("DATE")

    # filter some columns
    filtered_df = filtered_df.select(
        ["DATE", "VALUE"]
    )
    # get data from the minimum dataset <2021
    mds_data = load_iuc_minimum_data()
    # combine with main data
    filtered_df = pl.concat([filtered_df, mds_data]).sort("DATE")
    
    if not os.path.exists("nhs/processed_data/iuc/"):
        os.makedirs("nhs/processed_data/iuc/")
        
    filtered_df.write_csv("nhs/processed_data/iuc/cleaned_iuc_data.csv")


def load_iuc_minimum_data():
    """
    Loads the IUC minimum data set data for period 2017-2021.
    
    This is in a different data format to the main IUC data, hence the 
    new function.
    
    Parameters
    -----------

    Returns
    ----------
    combined_df: DataFrame
        DataFrame containing combined data from all files.

    Notes
    ---------
    
    """
    # Get list of raw data files
    files = get_data_files("nhs/raw_data/iuc_minimum_dataset")
    
    # Load data from files
    df_list = []
    for file in files:
        df = pl.read_csv(file, infer_schema=False)
        # filter
        df = df.select(
            ["Date", "Contract_name", "5_3"]
        )
        # format date column
        df = df.with_columns(
            pl.col("Date").str.strptime(pl.Date, format="%d/%m/%Y")
        )
        df_list.append(df)
    combined_df = pl.concat(df_list).sort("Date")
    # rename columns
    combined_df = combined_df.rename(
        {"Date": "DATE", "Contract_name": "CONTRACT_NAME", "5_3": "VALUE"}
    )
    # filter for cornwall trust
    combined_df = combined_df.filter(
        pl.col("CONTRACT_NAME") == "Cornwall 111AF1"
        )
    combined_df = combined_df.select(["DATE", "VALUE"])
    return combined_df