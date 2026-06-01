"""
Functions for producing plots with the processed Cornwall Council collisions data.

Functions included:
    - plot_all_collisions
        Shows collision counts by month across the full dataset.
    - plot_collisions_in_month
        Shows collision counts per day for each weather flag for a given month and year,
        with optional annotations for weather events.

Author: CL
"""

# Global imports
import calendar
import polars as pl
import matplotlib.pyplot as plt

# set global font
plt.rcParams["font.family"] = "Outfit"
plt.rcParams["font.size"] = 16

def plot_all_collisions(df):
    """
    Shows total collision counts per month across the full dataset.

    Produces a bar plot of total collisions per month.

    Parameters
    -----------
    df: DataFrame
        Cleaned Cornwall Council collisions data with DATE (pl.Date)
        and COLLISIONS columns.

    Returns
    ----------
    None
    """
    pass