"""
Functions for producing plots with the processed RNLI callout data.

Functions included:
    - plot_all_callouts
        Shows callout counts by month across the full dataset.
    - plot_callouts_in_month
        Shows callout counts per day for a given month and year,
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


def plot_all_callouts(df):
    """
    Shows total callout counts per month across the full dataset.

    Produces a bar plot of total callouts per month.

    Parameters
    -----------
    df: DataFrame
        Cleaned RNLI callout data with DATE (pl.Date) and CALL_OUTS columns.

    Returns
    ----------
    None
    """
    df = df.with_columns(
        pl.col("DATE").dt.month().alias("MONTH"),
        pl.col("DATE").dt.year().alias("YEAR")
    )
    monthly = df.group_by(["YEAR", "MONTH"]).agg(
        pl.sum("CALL_OUTS").alias("TOTAL_CALLOUTS")
    ).sort(["YEAR", "MONTH"])
    monthly = monthly.with_columns(
        pl.concat_str(
            [pl.col("YEAR").cast(pl.Utf8), pl.col("MONTH").cast(pl.Utf8)],
            separator="-"
        ).alias("DATE")
    )

    plt.figure(figsize=(12, 6))
    plt.bar(monthly["DATE"].to_list(), monthly["TOTAL_CALLOUTS"].to_list())
    plt.ylabel("Number of callouts")
    plt.xticks(rotation=45)
    plt.margins(x=.02)
    plt.title("RNLI callouts per month")
    plt.savefig("plots/rnli_all_callouts.png", bbox_inches="tight")
    print("All callouts plot saved to plots/rnli_all_callouts.png")


def plot_callouts_in_month(df, month, year, annotations=None):
    """
    Shows callout counts per day for a given month and year.

    Produces a bar plot of daily callout counts, with optional shaded
    annotations for weather events.

    Parameters
    -----------
    df: DataFrame
        Cleaned RNLI callout data with DATE (pl.Date) and CALL_OUTS columns.
    month: int
        Month to plot (1-12).
    year: int
        Year to plot (e.g. 2023).
    annotations: list of dicts, optional (e.g., name, start, end)

    Returns
    ----------
    None
    """
    df = df.filter(
        (pl.col("DATE").dt.month() == month) & (pl.col("DATE").dt.year() == year)
    )
    df = df.with_columns(
        pl.col("DATE").dt.day().alias("DAY")
    ).select(["DAY", "CALL_OUTS"])

    # Ensure all days of the month are present, even those with 0 callouts
    n_days = calendar.monthrange(year, month)[1]
    all_days = pl.DataFrame({"DAY": list(range(1, n_days + 1))})
    df = all_days.join(df, on="DAY", how="left").with_columns(
        pl.col("CALL_OUTS").fill_null(0)
    ).sort("DAY")

    plt.figure(figsize=(12, 6))
    plt.bar(df["DAY"].to_list(), df["CALL_OUTS"].to_list())
    plt.ylim(0, df["CALL_OUTS"].max() * 1.1)
    plt.ylabel("Number of callouts")
    plt.xlabel("Day")
    plt.title(f"RNLI callouts for {month}/{year}")
    plt.xticks(df["DAY"].to_list())

    if annotations is not None:
        for annotation in annotations:
            plt.axvspan(
                annotation["start"] - .5, annotation["end"] + .5, color='red',
                alpha=0.2, linestyle='--'
            )
            x_coord = annotation["start"] - 2.3
            plt.text(
                x_coord,
                plt.ylim()[1] * 0.75,
                annotation["name"],
                color='red',
                ha='center',
                fontsize=11
            )

    plt.margins(x=.01)
    plt.savefig(f"plots/rnli_callouts_{year}_{month}.png", bbox_inches="tight")
    print(f"RNLI callouts for {month}/{year} saved to plots/rnli_callouts_{year}_{month}.png")
