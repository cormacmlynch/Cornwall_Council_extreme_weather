"""
Main entry point for data processing and analysis for the Cornwall Council 
climate change impact assessment project.

Author: CL
"""
from network_rail.nr_data_cleaning import clean_nr_data

def main():
    # Clean the Network Rail delay data
    clean_nr_data()
    
if __name__ == "__main__":
    main()