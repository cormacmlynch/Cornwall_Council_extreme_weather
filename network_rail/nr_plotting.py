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
    plt.title("Delay minutes per month")
    plt.legend()
    plt.savefig("plots/all_delays.png", bbox_inches="tight")   
    print("All delays plot saved to plots/all_delays.png") 


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
    
    # Create all combinations of year, month, and weather flag to fill missing values
    unique_dates = monthly_df.select("DATE", "YEAR", "MONTH").unique().sort(["YEAR", "MONTH"])
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
