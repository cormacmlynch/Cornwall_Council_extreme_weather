"""
Main entry point for data processing and analysis for the Cornwall Council 
climate change impact assessment project.

Author: CL
"""
import os
import polars as pl

from network_rail.nr_data_cleaning import clean_nr_data
from network_rail.nr_plotting import plot_all_delays

def main():
    # Check if data has already been cleaned 
    if not os.path.exists(
        "network_rail/processed_data/cleaned_nr_data.csv"
        ):
        print("Cleaned data file not found. Cleaning data from raw files...")
        data = clean_nr_data()
        print("Cleaned data saved.")
    else:
        print("Cleaned data file found. Loading cleaned data...")
    
    # when reading, ensure ORIGIN_DEPARTURE_DATE is read as datetime
    data = pl.read_csv(
        "network_rail/processed_data/cleaned_nr_data.csv",
        schema_overrides={"ORIGIN_DEPARTURE_DATE": pl.Date}
        )
    plot_all_delays(data)
    
if __name__ == "__main__":
    main()