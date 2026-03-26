"""
Functions for cleaning and preprocessing Network Rail delay data.

Functions included:
    - clean_nr_data
        Main cleaning function.
    - get_nr_files
        Returns list of data files in raw data directory.
    - load_nr_data
        Loads data from a file into a DataFrame.
        
Author: CL
"""
# Global imports
import os
import polars as pl


def clean_nr_data():
    """
    Main cleaning function.

    Filters the delay attribution data for records relevant to Cornwall.

    Parameters
    -----------

    Returns
    ----------
    cleaned_df: DataFrame
        Cleaned DataFrame containing only records relevant to Cornwall.

    Notes
    ---------
    """
    # Get list of raw data files
    files = get_nr_files()
    
    # Load data from files
    raw_df = load_nr_data(files)
    
    # Convert station codes to full station names
    raw_df = get_full_stn_names(raw_df)
    
    # Convert incident codes to full descriptions
    raw_df = get_full_incident_codes(raw_df)
    
    # Only keep files where delay incident was in Cornwall
    # STANOX codes for Cornwall start at 84208 and end at 85739
    raw_df = raw_df.with_columns(
        pl.col("START_STANOX").cast(pl.Int64),
        pl.col("END_STANOX").cast(pl.Int64)
    )
    raw_df = raw_df.filter(
        (pl.col("START_STANOX").is_between(84208, 85739)) |
        (pl.col("END_STANOX").is_between(84208, 85739))
    )
    
    # Add flag for whether train was cancelled 
    cx_events = ["C", "D", "O", "P", "S", "F"]
    raw_df = raw_df.with_columns(
        pl.col("EVENT_TYPE").is_in(cx_events).alias("IS_CANCELLED")
    )
    
    # only keep important cols
    raw_df = raw_df.select(
        "TRAIN_SERVICE_CODE",
        "ORIGIN_DEPARTURE_DATE", 
        "PLANNED_ORIGIN_LOCATION_NAME",
        "PLANNED_DEST_LOCATION_NAME",
        "DELAY_START_STATION",
        "DELAY_END_STATION",
        "PFPI_MINUTES",
        "IS_CANCELLED",
        "INCIDENT_REASON",
        "INCIDENT_DESCRIPTION",
        "is_weather_related",
        "EVENT_TYPE"
    )

    # save tail of df to csv
    raw_df.tail(100).write_csv(
        "network_rail/processed_data/raw_data_tail.csv"
        )
        
    return None

def get_nr_files():
    """
    Returns list of data files in raw data directory.

    Parameters
    -----------

    Returns
    ----------
    files: list
        List of file paths for raw data files.
    """
    # check files in raw data directory and return list of file paths
    files = []
    for filename in os.listdir("network_rail/raw_data"):
        if filename.endswith(".csv"):
            files.append(os.path.join("network_rail/raw_data", filename))
    return files

def load_nr_data(files):
    """
    Loads data from a file into a DataFrame.

    Parameters
    -----------
    files: list
        List of file paths for raw data files.

    Returns
    ----------
    df: DataFrame
        DataFrame containing loaded data from all files.
    """
    # load data from each file and concatenate into a single DataFrame
    df_list = []
    for file in files:
        df = pl.read_csv(file, infer_schema=False)
        df_list.append(df)
    combined_df = pl.concat(df_list)
    return combined_df

def get_full_stn_names(df):
    """
    Converts station codes to full station names.

    Parameters
    -----------
    df: DataFrame
        DataFrame containing station codes.

    Returns
    ----------
    df: DataFrame
        DataFrame containing full station names.
    """
    # Get excel worksheet with station code to name mapping
    supp_file = ("network_rail/supplementary_info/" 
                 "Transparency page Attribution Glossary.xlsx")
    stn_mapping = pl.read_excel(
        supp_file,
        sheet_name="Stanox Codes",
        ).select(
            pl.col("STANOX NO."),
            pl.col("FULL NAME")
        )
    stn_mapping = stn_mapping.rename({
        "STANOX NO.": "stanox_no",
        "FULL NAME": "full_station_name"
    })
    # replace df cols
    df = df.join(
        stn_mapping, 
        left_on="PLANNED_ORIGIN_LOCATION_CODE",
        right_on="stanox_no", 
        how="left"
        ).rename(
            {"full_station_name": "PLANNED_ORIGIN_LOCATION_NAME"}
            )
    df = df.join(
        stn_mapping, 
        left_on="PLANNED_DEST_LOCATION_CODE", 
        right_on="stanox_no", 
        how="left"
        ).rename(
            {"full_station_name": "PLANNED_DEST_LOCATION_NAME"}
            )
    df = df.join(
        stn_mapping, 
        left_on="START_STANOX", 
        right_on="stanox_no", 
        how="left"
        ).rename(
            {"full_station_name": "DELAY_START_STATION"}
            )
    df = df.join(
        stn_mapping, 
        left_on="END_STANOX", 
        right_on="stanox_no", 
        how="left"
        ).rename(
            {"full_station_name": "DELAY_END_STATION"}
            )

    return df

def get_full_incident_codes(df):
    """
    Converts incident codes to full descriptions.

    Parameters
    -----------
    df: DataFrame
        DataFrame containing incident codes.

    Returns
    ----------
    df: DataFrame
        DataFrame containing full incident descriptions.
    """
    # Get excel worksheet with station code to name mapping
    supp_file = ("network_rail/supplementary_info/" 
                 "Transparency page Attribution Glossary.xlsx")
    code_mapping = pl.read_excel(
        supp_file,
        sheet_name="Incident Reason",
        ).select(
            pl.col("Incident Reason"),
            pl.col("Incident Reason Description"),
            pl.col("is_weather_related")
        )
    code_mapping = code_mapping.rename({
        "Incident Reason": "incident_code",
        "Incident Reason Description": "incident_description",
        "is_weather_related": "is_weather_related"
    })
    # replace df cols
    df = df.join(
        code_mapping, 
        left_on="INCIDENT_REASON", 
        right_on="incident_code", 
        how="left"
        ).rename(
            {"incident_description": "INCIDENT_REASON_DESCRIPTION"}
            )
        
    return df