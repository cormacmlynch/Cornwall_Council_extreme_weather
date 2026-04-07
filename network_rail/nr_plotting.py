"""
Functions for producing plots with the processed Network Rail delay data.

Functions included:
    - plot_all_delays
        Shows delay minutes by month and year for all delays,
        with different colours for weather related and non weather.
    - convert_monthly
        Converts daily delay data to monthly delay data.
Author: CL

"""
# Global imports
import polars as pl
import matplotlib.pyplot as plt

# set global font
plt.rcParams["font.family"] = "Outfit"
plt.rcParams["font.size"] = 16

def plot_all_delays(df):
    """
    Shows delay minutes of all delays.

    Produces a bar plot showing the total delay minutes per day.
    
    Parameters
    -----------
    df: DataFrame
        Cleaned Network Rail delay data.

    Returns
    ----------
    None

    Notes
    ---------
    """
    # Aggregate delay minutes by deoarture day
    df = df.group_by("ORIGIN_DEPARTURE_DATE", "IS_WEATHER_RELATED").agg(
        pl.sum("PFPI_MINUTES").alias("TOTAL_DELAY_MINUTES")
    ).sort("ORIGIN_DEPARTURE_DATE")

    # Aggregate at monthly level
    df = convert_monthly(df)
    
    # get series of non weather related delays and weather related delays for plotting
    delay_weather = df.filter(pl.col("IS_WEATHER_RELATED") == True)
    delay_non_weather = df.filter(pl.col("IS_WEATHER_RELATED") == False)
    delay_weather.write_csv("network_rail/processed_data/weather_delays.csv")

    # stacked bar plot - includes different colour whether delay is weather related
    plt.figure(figsize=(12,6))
    plt.bar(
        delay_non_weather["DATE"].to_list(), 
        delay_non_weather["TOTAL_DELAY_MINUTES"].to_list(), 
        label="Non-weather related delays"
        )
    plt.bar(
        delay_weather["DATE"].to_list(), 
        delay_weather["TOTAL_DELAY_MINUTES"].to_list(),
        bottom=delay_non_weather["TOTAL_DELAY_MINUTES"].to_list(),
        label="Weather related delays"
        )
    plt.ylabel("Delay minutes")
    plt.xticks(rotation=45)
    plt.margins(x=.02)
    plt.title("Delay minutes per month")
    plt.legend()
    plt.savefig("plots/all_delays.png", bbox_inches="tight")   
    print("All delays plot saved to plots/all_delays.png") 


def plot_delays_monthly(df, month, year, annotations = None):
    """
    Shows delay minutes of all delays in a given month

    Produces a bar plot showing the total delay minutes per day.
    
    Parameters
    -----------
    df: DataFrame
        Cleaned Network Rail delay data.
    month: int
        Month to filter the data by (1-12).
    year: int
        Year to filter the data by (e.g. 2023).
    annotations: list of dicts, optional (e.g., name, start, end)

    Returns
    ----------
    None

    Notes
    ---------
    """
    # Aggregate delay minutes by deoarture day
    df = df.group_by("ORIGIN_DEPARTURE_DATE", "IS_WEATHER_RELATED").agg(
        pl.sum("PFPI_MINUTES").alias("TOTAL_DELAY_MINUTES")
    ).sort("ORIGIN_DEPARTURE_DATE")

    # Aggregate at daily level
    df = convert_daily(df)
    # Filter for the specified month and year
    df = df.filter((pl.col("MONTH") == month) & (pl.col("YEAR") == year))
    # set day column as str for plotting
    # df = df.with_columns(
    #     pl.col("DAY").cast(pl.Utf8)
    # )
    # get series of non weather related delays and weather related delays for plotting
    delay_weather = df.filter(pl.col("IS_WEATHER_RELATED") == True)
    delay_non_weather = df.filter(pl.col("IS_WEATHER_RELATED") == False)
    # stacked bar plot - includes different colour whether delay is weather related
    plt.figure(figsize=(12,6))
    plt.bar(
        delay_non_weather["DAY"].to_list(), 
        delay_non_weather["TOTAL_DELAY_MINUTES"].to_list(), 
        label="Non-weather related delays"
        )
    plt.bar(
        delay_weather["DAY"].to_list(), 
        delay_weather["TOTAL_DELAY_MINUTES"].to_list(),
        bottom=delay_non_weather["TOTAL_DELAY_MINUTES"].to_list(),
        label="Weather related delays"
        )
    plt.ylabel("Delay minutes")
    plt.xlabel("Day")
    plt.title(f"Delay minutes for {month}/{year}")
    # plot all x axis labels
    plt.xticks(delay_non_weather["DAY"].to_list())
    
    if annotations is not None:
        for annotation in annotations:
            plt.axvspan(
                annotation["start"] - .5, annotation["end"] + .5, color='red', 
                alpha=0.2, linestyle='--'
                )
            if annotation["name"] == "Storm Chandra":
                x_coord = annotation["end"] + 2.3
            else:                
                x_coord = annotation["start"] - 2.3
            plt.text(
                x_coord, 
                plt.ylim()[1]*0.75, 
                annotation["name"], 
                color='red', 
                ha='center',
                fontsize=11
                )
    plt.legend(loc="upper right")
    plt.margins(x=.01)

    plt.savefig(f"plots/delays_{year}_{month}.png", bbox_inches="tight")
    print(f"Delays for {month}/{year} saved to plots/delays_{year}_{month}.png")

def convert_monthly(df):
    """
    Function to convert the daily delay data to monthly
    
    Parameters
    -----------
    df: DataFrame
        Cleaned Network Rail delay data with ORGIN_DEPARTURE_DATE as datetime 
        and delay minutes aggregated per day.

    Returns
    ----------
    monthly_df: DataFrame
        DataFrame with delay minutes aggregated per month and year. A new
        column is added for the month and year.

    Notes
    ---------
    """
    df = df.with_columns(
        pl.col("ORIGIN_DEPARTURE_DATE").dt.month().alias("MONTH"),
        pl.col("ORIGIN_DEPARTURE_DATE").dt.year().alias("YEAR")
    )
    monthly_df = df.group_by(["YEAR", "MONTH", "IS_WEATHER_RELATED"]).agg(
        pl.sum("TOTAL_DELAY_MINUTES").alias("TOTAL_DELAY_MINUTES")
    )
    # Create date col and set correct order
    monthly_df = monthly_df.with_columns(
        pl.concat_str([pl.col("YEAR").cast(pl.Utf8), 
                       pl.col("MONTH").cast(pl.Utf8)],
                      separator="-").alias("DATE")
        )
    monthly_df = monthly_df.sort(["YEAR", "MONTH", "IS_WEATHER_RELATED"])
    
    # Create all combinations of year, month, and weather flag
    unique_dates = monthly_df.select(
        "DATE", "YEAR", "MONTH").unique().sort(["YEAR", "MONTH"]
        )
    weather_flags = monthly_df.select("IS_WEATHER_RELATED").unique()
    
    all_combinations = unique_dates.join(weather_flags, how="cross")
    
    # Left join to fill missing combinations with 0
    monthly_df = all_combinations.join(
        monthly_df, 
        on=["DATE", "IS_WEATHER_RELATED"],
        how="left"
    ).with_columns(
        pl.col("TOTAL_DELAY_MINUTES").fill_null(0)
    ).sort(["YEAR", "MONTH", "IS_WEATHER_RELATED"])
    
    return monthly_df

def convert_daily(df):
    """
    Function to convert the monthly delay data to daily
    
    Parameters
    -----------
    df: DataFrame
        Cleaned Network Rail delay data with ORGIN_DEPARTURE_DATE as datetime 
        and delay minutes aggregated per month.

    Returns
    ----------
    daily_df: DataFrame
        DataFrame with delay minutes aggregated per day. A new
        column is added for the date.

    Notes
    ---------
    """    
    df = df.with_columns(
        pl.col("ORIGIN_DEPARTURE_DATE").dt.day().alias("DAY"),
        pl.col("ORIGIN_DEPARTURE_DATE").dt.month().alias("MONTH"),
        pl.col("ORIGIN_DEPARTURE_DATE").dt.year().alias("YEAR")
    )
    daily_df = df.group_by(["YEAR", "MONTH", "DAY", "IS_WEATHER_RELATED"]).agg(
        pl.sum("TOTAL_DELAY_MINUTES").alias("TOTAL_DELAY_MINUTES")
    )
    # Create date col and set correct order
    daily_df = daily_df.with_columns(
        pl.concat_str([pl.col("YEAR").cast(pl.Utf8), 
                       pl.col("MONTH").cast(pl.Utf8),
                       pl.col("DAY").cast(pl.Utf8)],
                      separator="-").alias("DATE")
        )
    daily_df = daily_df.sort(["YEAR", "MONTH", "DAY", "IS_WEATHER_RELATED"])
    
    # Create all combinations of year, month, and weather flag so all days are included
    unique_dates = daily_df.select(
        "DATE", "YEAR", "MONTH", "DAY").unique().sort(["YEAR", "MONTH", "DAY"]
        )
    weather_flags = daily_df.select("IS_WEATHER_RELATED").unique()
    all_combinations = unique_dates.join(weather_flags, how="cross")
    # Left join to fill missing combinations with 0
    daily_df = all_combinations.join(
        daily_df, 
        on=["DATE", "IS_WEATHER_RELATED"],
        how="left"
    ).with_columns(
        pl.col("TOTAL_DELAY_MINUTES").fill_null(0)
    ).sort(["YEAR", "MONTH", "DAY", "IS_WEATHER_RELATED"])
    
    return daily_df