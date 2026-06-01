"""
Script for functions to create plots for IUC NHS data.

Fuctions included:

Author: CL
"""

# global imports
import polars as pl
import matplotlib.pyplot as plt

def plot_avg_calls_dow(df):
    """
    Plots average number of calls per day of week.

    Parameters
    -----------
    df: DataFrame
        Cleaned IUC NHS data.

    Returns
    ----------
    None

    Notes
    ---------
    """
    # get average number of calls per day of week
    df = get_day_of_week(df)
    df = df.group_by("DAY_OF_WEEK").agg(
        pl.mean("VALUE").alias("AVG_CALLS")
    ).sort("DAY_OF_WEEK")
    plt.figure(figsize=(10, 6))
    plt.bar(df["DAY_OF_WEEK"], df["AVG_CALLS"])
    plt.xlabel("Day of Week")
    plt.ylabel("Average Number of Calls")
    plt.title("Average Number of Calls per Day of Week")
    
    
def plot_calls_in_month(df, month, year, annotations=None):
    """
    Plots for each day in the given month,
    the difference from daily average number of calls for a given month and year.

    Parameters
    -----------
    df: DataFrame
        Cleaned IUC NHS data.
    month: int
        Month to plot (1-12).
    year: int
        Year to plot.
    annotations: list of dicts optional (e.g., name, start, end)

    Returns
    ----------
    None

    Notes
    ---------
    """
    # filter for given month and year
    df = df.filter(
        (pl.col("DATE").dt.month() == month) & 
        (pl.col("DATE").dt.year() == year)
    )
    df = get_day_of_week(df)
    # get average number of calls per day of week, excluding outliers
    df_avg = get_day_of_week(df)
    # calculate Q1, Q3, and IQR for each day of week
    df_avg = df_avg.group_by("DAY_OF_WEEK").agg([
        pl.quantile("VALUE", 0.25).alias("Q1"),
        pl.quantile("VALUE", 0.75).alias("Q3")
    ])
    df_avg = df_avg.with_columns(
        (pl.col("Q3") - pl.col("Q1")).alias("IQR")
    )
    # join back to filter outliers
    df_avg_for_calc = df.join(df_avg, on="DAY_OF_WEEK")
    df_avg_for_calc = df_avg_for_calc.filter(
        (pl.col("VALUE") >= pl.col("Q1") - 1.5 * pl.col("IQR")) &
        (pl.col("VALUE") <= pl.col("Q3") + 1.5 * pl.col("IQR"))
    )
    # calculate mean excluding outliers
    df_avg = df_avg_for_calc.group_by("DAY_OF_WEEK").agg(
        pl.mean("VALUE").alias("AVG_CALLS")
    ).sort("DAY_OF_WEEK")
    # get difference from average for each day
    df = df.with_columns(
        pl.col("DAY_OF_WEEK").cast(pl.Int32)
    ).join(df_avg, on="DAY_OF_WEEK")
    
    df = df.with_columns(pl.col("VALUE") - pl.col("AVG_CALLS"))
    # get day of month for plotting
    df = df.with_columns(
        pl.col("DATE").dt.day().alias("DAY")
    )
    
    plt.figure(figsize=(10, 6))
    plt.bar(df["DAY"], df["VALUE"])
    
    if annotations is not None:
        for annotation in annotations:
            plt.axvspan(
                annotation["start"] - .5, annotation["end"] + .5, color='red', 
                alpha=0.2, linestyle='--'
                )
            x_coord = annotation["start"] - 2.3
            plt.text(
                x_coord, 
                plt.ylim()[1]*0.75, 
                annotation["name"], 
                color='red', 
                ha='center',
                fontsize=11
                )
    
    plt.xlabel("Date")
    plt.ylabel("Difference from Average Number of Calls")
    plt.title(f"Difference from Average Number of Calls per Day in {month}/{year}")
    plt.savefig(f"plots/calls_in_month_{month}_{year}.svg", format="svg")
    print(f"Plot saved to plots/calls_in_month_{month}_{year}.svg")


def get_day_of_week(df):
    """
    Converts date to a day of week.

    Parameters
    -----------
    df: DataFrame
        Cleaned IUC NHS data.

    Returns
    ----------
    DataFrame with added column for day of week.

    Notes
    ---------
    """
    df = df.with_columns(
        pl.col("DATE").dt.weekday().alias("DAY_OF_WEEK")
    )
    return df