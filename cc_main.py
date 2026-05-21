"""
Main entry point for data processing and analysis for the Cornwall Council 
climate change impact assessment project.

Author: CL
"""
import os
import polars as pl

from network_rail.nr_data_cleaning import clean_nr_data
from network_rail.nr_plotting import plot_all_delays, plot_delays_monthly
from nhs.nhs_data_cleaning import clean_iuc_data
from nhs.nhs_plotting import plot_avg_calls_dow, plot_calls_in_month
import matplotlib.pyplot as plt

plt.rcParams['svg.fonttype'] = 'none'

def main():
    # Check if data has already been cleaned 
    if not os.path.exists(
        "network_rail/processed_data/cleaned_nr_data.csv"
        ):
        print("Cleaned data file not found. Cleaning data from raw files...")
        nr_data = clean_nr_data()
        print("Cleaned data saved.")
    else:
        print("Cleaned data file found. Loading cleaned data...")
    
    nr_data = pl.read_csv(
        "network_rail/processed_data/cleaned_nr_data.csv",
        schema_overrides={"ORIGIN_DEPARTURE_DATE": pl.Date,
                          "PFPI_MINUTES": pl.Float32,}
        )
    plot_all_delays(nr_data)
    # January 2026
    plot_delays_monthly(nr_data, month=1, year=2026, 
                        annotations=[{"name": "Storm Goretti", 
                                      "start": 8, "end": 9}, 
                                     {"name": "Storm Chandra", 
                                      "start": 26, "end": 27},
                                     {"name": "Storm Ingrid", 
                                      "start": 23, "end": 24}
                                     ])
    # November 2024
    plot_delays_monthly(nr_data, month=11, year=2024, 
                        annotations=[{"name": "Storm Bert", 
                                      "start": 22, "end": 25}
                                     ])
    
    'NHC IUC data'
    if not os.path.exists(
        "nhs/processed_data/iuc/cleaned_iuc_data.csv"
        ):
        print("Cleaned IUC data file not found. Cleaning data from raw files...")
        clean_iuc_data()
        print("Cleaned IUC data saved.")
    else:
        print("Cleaned IUC data file found. Loading cleaned data...")
    iuc_data = pl.read_csv(
        "nhs/processed_data/iuc/cleaned_iuc_data.csv",
        schema_overrides={"DATE": pl.Date, "VALUE": pl.Int32}
    )
    # plot_avg_calls_dow(iuc_data)
    plot_calls_in_month(iuc_data, month=3, year=2018, 
                        annotations=[{"name": "Beast from the East", 
                                      "start": 1, 
                                      "end": 5}])
    plot_calls_in_month(iuc_data, month=7, year=2022, 
                        annotations=[{"name": "Peak of 2022 heatwave", 
                                      "start": 16, 
                                      "end": 19}])
    plot_calls_in_month(iuc_data, month=11, year=2024, 
                        annotations=[{"name": "Storm Bert", 
                                      "start": 22, 
                                      "end": 25}])
    plot_calls_in_month(iuc_data, month=1, year=2026, 
                        annotations=[{"name": "Storm Goretti", 
                                      "start": 8, "end": 9}, 
                                     {"name": "Storm Chandra", 
                                      "start": 26, "end": 27},
                                     {"name": "Storm Ingrid", 
                                      "start": 23, "end": 24}
                                     ])
    
if __name__ == "__main__":
    main()