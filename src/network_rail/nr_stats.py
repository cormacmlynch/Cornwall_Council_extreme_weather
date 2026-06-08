"""
Functions for displaying statistics associated with the processed Network Rail
delay data.

Functions included:
    - get_weather_delay_stats
        Calculates the delay minutes associated with weather related delays, 
        and the percentage of total delay minutes that are weather related.

Author: CL

"""
# Global imports
import polars as pl

# Local imports
from network_rail.nr_plotting import convert_daily

def get_weather_delay_stats(df, month=None, year=None, date_range=None):
    """
    Calculates the delay minutes associated with weather related delays, 
    and the percentage of total delay minutes that are weather related.

    Parameters
    -----------
    df: DataFrame
        Cleaned Network Rail delay data.
    month: int, optional
        Month to filter the data by (1-12).
    year: int, optional
        Year to filter the data by (e.g. 2020).
    date_range: tuple of int, optional
        Date range to filter the data by (start_date, end_date).

    Returns
    ----------
    stats: dict
        Dictionary containing the following keys:
        weather_delay_minutes: int
            Total delay minutes associated with weather related delays.
        weather_delay_percentage: float
            Percentage of total delay minutes that are weather related.
    """
    # Aggregate delay minutes by deoarture day
    df = df.group_by("ORIGIN_DEPARTURE_DATE", "IS_WEATHER_RELATED").agg(
        pl.sum("PFPI_MINUTES").alias("TOTAL_DELAY_MINUTES")
    ).sort("ORIGIN_DEPARTURE_DATE")

    # Aggregate at daily level
    df = convert_daily(df)
    
    # Filter for the specified time period
    if month is not None:
        df = df.filter(pl.col("MONTH") == month)
    if year is not None:
        df = df.filter(pl.col("YEAR") == year)
    if date_range is not None:
        start_date, end_date = date_range
        df = df.filter(pl.col("DAY").is_between(start_date, end_date))

    print(df)
    total_delay_minutes = df["TOTAL_DELAY_MINUTES"].sum()
    weather_delay_minutes = df.filter(
        pl.col("IS_WEATHER_RELATED") == True
        )["TOTAL_DELAY_MINUTES"].sum()
    
    weather_delay_percentage = (weather_delay_minutes /
                                total_delay_minutes) * 100
    
    stats = {
        "weather_delay_minutes": weather_delay_minutes,
        "weather_delay_percentage": weather_delay_percentage
    }
    return stats