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

def plot_all_collisions(df, save_svgs=False):
    """
    Shows total collision counts per month across the full dataset,
    stacked by IS_ADVERSE_WEATHER flag.

    Produces a stacked bar plot of total collisions per month,
    split by IS_ADVERSE_WEATHER categories (no, yes, maybe).

    Parameters
    -----------
    df: DataFrame
        Cleaned Cornwall Council collisions data with DATE (pl.Date),
        COLLISIONS, and IS_ADVERSE_WEATHER columns.
    save_svgs: bool, optional
        Whether to save the plot as an SVG file.
    Returns
    ----------
    None
    """
    # calculate total collisions by month and weather flag
    monthly = (
        df.with_columns(
            pl.col("DATE").dt.month().alias("MONTH"),
            pl.col("IS_ADVERSE_WEATHER").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("WEATHER")
        )
        .group_by(["MONTH", "WEATHER"])
        .agg(pl.sum("COLLISIONS").alias("TOTAL_COLLISIONS"))
    )

    # align categories on a complete 1-12 month index so stacked bars are consistent
    monthly_wide = monthly.pivot(
        values="TOTAL_COLLISIONS",
        index="MONTH",
        on="WEATHER",
        aggregate_function="first"
    )

    full_months = pl.DataFrame({"MONTH": list(range(1, 13))})
    monthly_wide = full_months.join(monthly_wide, on="MONTH", how="left")

    for weather_col in ["no", "yes", "maybe"]:
        if weather_col not in monthly_wide.columns:
            monthly_wide = monthly_wide.with_columns(pl.lit(0.0).alias(weather_col))

    monthly_wide = (
        monthly_wide
        .with_columns(
            pl.col("no").fill_null(0.0),
            pl.col("yes").fill_null(0.0),
            pl.col("maybe").fill_null(0.0)
        )
        .sort("MONTH")
    )

    month_labels = [calendar.month_abbr[m] for m in monthly_wide["MONTH"].to_list()]
    normal_vals = monthly_wide["no"].to_list()
    adverse_vals = monthly_wide["yes"].to_list()
    maybe_vals = monthly_wide["maybe"].to_list()

    # create stacked bar plot
    plt.figure(figsize=(12, 6))
    plt.bar(month_labels, normal_vals, label="No", color="#6AA84F")
    plt.bar(month_labels, adverse_vals, bottom=normal_vals, label="Yes", color="#CC4125")
    plt.bar(
        month_labels,
        maybe_vals,
        bottom=[n + a for n, a in zip(normal_vals, adverse_vals)],
        label="Maybe",
        color="#F1C232"
    )
    plt.xlabel("Month")
    plt.ylabel("Total Collisions (2016-2025)")
    plt.xticks(rotation=45)
    plt.legend(title="Is weather adverse?")
    plt.tight_layout()
    if save_svgs:
        plt.savefig("plots/collisions_by_month.svg", format="svg")    
    
def plot_collisions_in_month(df, month, year, annotations=None, 
                             save_svgs=False):
    """
    Shows collision counts per day for a given month and year.

    Produces a bar plot of daily collision counts, with optional shaded
    annotations for weather events.

    Parameters
    -----------
    df: DataFrame
        Cleaned Cornwall Council collisions data with DATE (pl.Date) and COLLISIONS columns.
    month: int
        Month to plot (1-12).
    year: int
        Year to plot (e.g. 2023).
    annotations: list of dicts, optional (e.g., name, start, end)
    save_svgs: bool, optional
        Whether to save the plot as an SVG file.
    Returns
    ----------
    None
    """
    if month < 1 or month > 12:
        raise ValueError("month must be between 1 and 12")

    # aggregate collisions to day-level for the selected month/year
    daily = (
        df.with_columns(
            pl.col("DATE").dt.day().alias("DAY"),
            pl.col("DATE").dt.month().alias("MONTH"),
            pl.col("DATE").dt.year().alias("YEAR"),
            pl.col("IS_ADVERSE_WEATHER").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("WEATHER")
        )
        .filter((pl.col("MONTH") == month) & (pl.col("YEAR") == year))
        .group_by(["YEAR", "MONTH", "DAY", "WEATHER"])
        .agg(pl.sum("COLLISIONS").alias("TOTAL_COLLISIONS"))
    )

    # include all days and all weather categories so stacked bars stay aligned
    n_days = calendar.monthrange(year, month)[1]
    all_days = pl.DataFrame({
        "DAY": list(range(1, n_days + 1)),
        "MONTH": [month] * n_days,
        "YEAR": [year] * n_days,
    })
    weather_categories = pl.DataFrame({"WEATHER": ["no", "yes", "maybe"]})
    all_combos = all_days.join(weather_categories, how="cross")

    daily = (
        all_combos.join(
            daily.select(["DAY", "MONTH", "YEAR", "WEATHER", "TOTAL_COLLISIONS"]),
            on=["DAY", "MONTH", "YEAR", "WEATHER"],
            how="left"
        )
        .with_columns(pl.col("TOTAL_COLLISIONS").fill_null(0))
        .sort(["DAY", "WEATHER"])
    )

    collisions_no = daily.filter(pl.col("WEATHER") == "no")
    collisions_maybe = daily.filter(pl.col("WEATHER") == "maybe")
    collisions_yes = daily.filter(pl.col("WEATHER") == "yes")

    day_vals = collisions_no["DAY"].to_list()
    no_vals = collisions_no["TOTAL_COLLISIONS"].to_numpy()
    yes_vals = collisions_yes["TOTAL_COLLISIONS"].to_numpy()
    maybe_vals = collisions_maybe["TOTAL_COLLISIONS"].to_numpy()
    totals = no_vals + yes_vals + maybe_vals

    plt.figure(figsize=(12, 6))
    plt.bar(day_vals, no_vals, label="No", color="#007d69")
    plt.bar(day_vals, maybe_vals, bottom=no_vals + yes_vals, label="Maybe", color="#ffc72c")
    plt.bar(day_vals, yes_vals, bottom=no_vals, label="Yes", color="#f9423a")

    max_total = float(totals.max()) if len(totals) > 0 else 0.0
    plt.ylim(0, max_total * 1.1 if max_total > 0 else 1)

    plt.ylabel("Number of Collisions")
    plt.xlabel(f"Day of {calendar.month_name[month]} {year}")
    plt.xticks(day_vals)
    # Force the y-axis to only use integer ticks
    plt.gca().yaxis.get_major_locator().set_params(integer=True)

    
    if annotations is not None:
        for annotation in annotations:
            plt.axvspan(
                annotation["start"] - 0.5,
                annotation["end"] + 0.5,
                color="#e60000",
                alpha=0.2,
                linestyle='--',
                zorder=0
            )
            plt.text(
                (annotation["start"] + annotation["end"]) / 2,
                plt.ylim()[1] * 0.85,
                annotation["name"],
                color="#e60000",
                ha="left",
                fontsize=14
            )

    plt.legend(title="Is weather adverse?")
    plt.margins(x=0.01)
    plt.tight_layout()
    if save_svgs:
        plt.savefig(f"plots/collisions_{year}_{month}.svg", format="svg", 
                    bbox_inches="tight")