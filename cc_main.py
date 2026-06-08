"""
Main entry point for data processing and analysis for the Cornwall Council 
climate change impact assessment project.

Author: CL
"""
import argparse
import os
import sys
import polars as pl
import matplotlib.pyplot as plt

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from network_rail.nr_data_cleaning import clean_nr_data
from network_rail.nr_plotting import plot_all_delays, plot_delays_monthly
from network_rail.nr_stats import get_weather_delay_stats

from nhs.nhs_data_cleaning import clean_iuc_data
from nhs.nhs_plotting import plot_avg_calls_dow, plot_calls_in_month

from rnli.rnli_data_cleaning import clean_rnli_data
from rnli.rnli_plotting import plot_all_callouts, plot_callouts_in_month


from cornwall_council.cc_data_cleaning import clean_cc_collisions_data
from cornwall_council.cc_plotting import plot_all_collisions, plot_collisions_in_month

def main():
    parser = argparse.ArgumentParser(
        description="Cornwall Council extreme weather impact analysis."
    )
    parser.add_argument(
        "--module",
        choices=["train_delays", "nhs_111", "rnli", "collisions"],
        default=None,
        help="Module to run. Omit to run all modules."
    )
    parser.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Force rebuild of processed data from raw files."
    )
    parser.add_argument(
        "--save-svgs",
        action="store_true",
        help="""Save plots as SVG files as well as PNG. Note: this will 
        overwrite any existing SVG files in the plots directory."""
    )
    args = parser.parse_args()

    run_all = args.module is None
    force_rebuild = args.force_rebuild
    save_svgs = args.save_svgs

    if run_all or args.module == "train_delays":
        # Check if data has already been cleaned 
        if force_rebuild or not os.path.exists(
            "data/processed/network_rail/cleaned_nr_data.csv"
            ):
            print("Cleaning data from raw files...")
            clean_nr_data()
            print("Cleaned data saved.")
        else:
            print("Cleaned data file found. Loading cleaned data...")
        
        nr_data = pl.read_csv(
            "data/processed/network_rail/cleaned_nr_data.csv",
            schema_overrides={"ORIGIN_DEPARTURE_DATE": pl.Date,
                              "PFPI_MINUTES": pl.Float32,}
            )
        plot_all_delays(nr_data, save_svgs=save_svgs)
        # January 2026
        plot_delays_monthly(nr_data, month=1, year=2026, 
                            annotations=[{"name": "Storm Goretti", 
                                          "start": 8, "end": 9}, 
                                         {"name": "Storm Chandra", 
                                          "start": 26, "end": 27},
                                         {"name": "Storm Ingrid", 
                                          "start": 23, "end": 24}
                                         ],
                            save_svgs=save_svgs)
        goretti_stats = get_weather_delay_stats(
            nr_data, month=1, year=2026, date_range=(1, 31)
            )
        print(f"January storms delay stats: {goretti_stats}")
        # November 2024
        plot_delays_monthly(nr_data, month=11, year=2024, 
                            annotations=[{"name": "Storm Bert", 
                                          "start": 22, "end": 25}
                                         ],
                            save_svgs=save_svgs)
        bert_stats = get_weather_delay_stats(
            nr_data, month=11, year=2024, date_range=(22, 25)
            )
        print(f"Storm Bert delay stats: {bert_stats}")
        # July 2022
        plot_delays_monthly(nr_data, month=7, year=2022, 
                            annotations=[{"name": "2022 heatwave\npeak", 
                                          "start": 16, "end": 19}
                                         ],
                            save_svgs=save_svgs)
        heatwave_stats = get_weather_delay_stats(
            nr_data, month=7, year=2022, date_range=(16, 19)
            )
        print(f"2022 heatwave delay stats: {heatwave_stats}")

    if run_all or args.module == "nhs_111":
        if force_rebuild or not os.path.exists(
            "data/processed/nhs/iuc/cleaned_iuc_data.csv"
            ):
            print("Cleaning IUC data from raw files...")
            clean_iuc_data()
            print("Cleaned IUC data saved.")
        else:
            print("Cleaned IUC data file found. Loading cleaned data...")
        iuc_data = pl.read_csv(
            "data/processed/nhs/iuc/cleaned_iuc_data.csv",
            schema_overrides={"DATE": pl.Date, "VALUE": pl.Int32}
        )
        # plot_avg_calls_dow(iuc_data)
        plot_calls_in_month(iuc_data, month=3, year=2018, 
                            annotations=[{"name": "Beast from the East", 
                                          "start": 1, 
                                          "end": 5}],
                            save_svgs=save_svgs)
        plot_calls_in_month(iuc_data, month=7, year=2022, 
                            annotations=[{"name": "Peak of 2022 heatwave", 
                                          "start": 16, 
                                          "end": 19}],
                            save_svgs=save_svgs)
        plot_calls_in_month(iuc_data, month=11, year=2024, 
                            annotations=[{"name": "Storm Bert", 
                                          "start": 22, 
                                          "end": 25}],
                            save_svgs=save_svgs)
        plot_calls_in_month(iuc_data, month=1, year=2026, 
                            annotations=[{"name": "Storm Goretti", 
                                          "start": 8, "end": 9}, 
                                         {"name": "Storm Chandra", 
                                          "start": 26, "end": 27},
                                         {"name": "Storm Ingrid", 
                                          "start": 23, "end": 24}
                                         ],
                            save_svgs=save_svgs)
        
    if run_all or args.module == "rnli":
        if force_rebuild or not os.path.exists(
        "data/processed/rnli/cleaned_rnli_data.csv"
        ):
            print("Cleaning RNLI data from raw files...")
            clean_rnli_data()
            print("Cleaned RNLI data saved.")
        else:
            print("Cleaned RNLI data file found. Loading cleaned data...")
        rnli_data = pl.read_csv(
            "data/processed/rnli/cleaned_rnli_data.csv",
            schema_overrides={"DATE": pl.Date}
        )
        plot_all_callouts(rnli_data)
        # Note: no data for 2026 available so not plotting for that year
        plot_callouts_in_month(rnli_data, month=7, year=2022, 
                            annotations=[{"name": "Peak of 2022 heatwave", 
                                          "start": 16, 
                                          "end": 19}],
                            save_svgs=save_svgs)
        plot_callouts_in_month(rnli_data, month=11, year=2024, 
                            annotations=[{"name": "Storm Bert", 
                                          "start": 22, 
                                          "end": 25}],
                            save_svgs=save_svgs)
        
    if run_all or args.module == "collisions":
        if force_rebuild or not os.path.exists(
            "data/processed/cornwall_council/collisions/cleaned_collisions_data.csv"
        ):
            print("Cleaning Cornwall Council collisions data from raw files...")
            clean_cc_collisions_data()
            print("Cleaned Cornwall Council collisions data saved.")
        else:
            print("Cleaned Cornwall Council collisions data file found. Loading cleaned data...")
        collisions_data = pl.read_csv(
            "data/processed/cornwall_council/collisions/cleaned_collisions_data.csv",
            schema_overrides={"DATE": pl.Date}
        )
        plot_all_collisions(collisions_data, save_svgs=save_svgs)
        plot_collisions_in_month(collisions_data, month=7, year=2022,
                            annotations=[{"name": "Peak of 2022 heatwave",
                                            "start": 16,
                                            "end": 19}],
                            save_svgs=save_svgs)
        plot_collisions_in_month(collisions_data, month=11, year=2024,
                            annotations=[{"name": "Storm Bert",
                                            "start": 22,
                                            "end": 25}],
                            save_svgs=save_svgs)
        
        
        

if __name__ == "__main__":
    main()