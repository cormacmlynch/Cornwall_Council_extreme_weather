"""
Functions to support loading and cleaning of all data.

Functions included:
    - get_data_files
        Returns list of data files in a specified directory.
    - load_data
        Loads data from a file into a DataFrame.
"""

# global imports
import os
import polars as pl


def get_data_files(directory):
    """
    Returns list of data files in a specified directory.

    Parameters
    -----------
    directory: str
        Path to directory containing data files.

    Returns
    ----------
    List of file paths for data files.

    Notes
    ---------
    """
    files = []
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            files.append(os.path.join(directory, filename))
    return files


def load_data(files, data_type=None):
    """
    Loads data from a file into a DataFrame.

    Parameters
    -----------
    files: list of str
        List of file paths for data files.

    Returns
    ----------
    DataFrame containing loaded data.

    Notes
    ---------
    """
    df_list = []
    for file in files:
        df = pl.read_csv(file, infer_schema=False)
        if data_type == 'iuc':
            # capitalise column names
            df.columns = [col.upper() for col in df.columns]
            # filter for certain cols -- these change across files
            df = df.select(
                ["DATE", "ITEM_NUMBER", "CONTRACT_NAME", "DAY", "VALUE"]
            )
        df_list.append(df)
        # capitalise column names
        df.columns = [col.upper() for col in df.columns]
    combined_df = pl.concat(df_list)
    return combined_df
