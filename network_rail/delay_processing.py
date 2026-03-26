"""
Script to investigate network rail delay attribution data.
"""
import polars as pl
import matplotlib.pyplot as plt

def main():
    # Load the data
    df = pl.read_csv("Transparency 25-26 P11.csv")

    # Show the first few rows of the data
    print(df.head())
    # Filter by train operator
    cornwall_tocs = ["EF", "EH"]
    penzance_code = 85734
    # filter by toc and also whether penzance is in the origin or destination
    df_filtered = df.filter(pl.col("TOC_CODE").is_in(cornwall_tocs) & ((pl.col("PLANNED_ORIGIN_LOCATION_CODE") == penzance_code) | (pl.col("PLANNED_DEST_LOCATION_CODE") == penzance_code)))
    # show column names
    print(df.columns)
    # get sum of delay minutes by origin departure date
    delay_counts_df = df_filtered.group_by("ORIGIN_DEPARTURE_DATE").agg(pl.sum("PFPI_MINUTES").alias("count")).sort("ORIGIN_DEPARTURE_DATE")
    # # plot the counts    
    plt.bar(delay_counts_df["ORIGIN_DEPARTURE_DATE"], delay_counts_df["count"])
    plt.xlabel("Origin Departure Date")
    plt.ylabel("Count")
    plt.title("Delay minutes of trains in and out of Penzance")
    # rotate x-axis labels for better readability
    plt.xticks(rotation=90)
    # reduce x label size
    plt.xticks(fontsize=8)
    plt.savefig("delay_minutes.png", dpi=300, bbox_inches="tight")
if __name__ == "__main__":    
    main()