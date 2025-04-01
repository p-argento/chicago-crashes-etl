#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 26 10:04:29 2024

@author: pietro
"""

import csv
import time
from scripts.utils import correct_names

# fix lic plate

def fix_lic_plate(row):
    """
    Corrects the "LIC_PLATE_STATE" field in a row based on valid state abbreviations.
    
    Args:
        row (dict): A dictionary representing a row of the dataset.
    
    Returns:
        dict: The updated row with the corrected "LIC_PLATE_STATE" value.
    """
    column_name = "LIC_PLATE_STATE"
    external_data_path = "data/external/state_abbrs.csv"
    return correct_names(row, column_name, external_data_path)

# fix vehicle year

def fix_vehicle_year(row):
    """Correct vehicle year values outside a valid range, replacing them with None."""
    
    column_name = "VEHICLE_YEAR"
    min_value, max_value = 1900, 2019

    try:
        value = int(row[column_name])
        if not (min_value <= value <= max_value):
            row[column_name] = ''
    except (ValueError, TypeError):
        row[column_name] = ''
    
    return row
 

### main function to be called in main ###

def clean_vehicles(input_file="data/raw/Vehicles.csv", output_file="data/cleaned/Vehicles_cleaned.csv"):
    """This function process the vehicles file.
        It includes various steps, optimized into one iteration over the records.
        Some columns are addressed individually or in groups depending on maintainability of code."""

    start_time = time.time()
    print("Starting clean_vehicles()...")    

    with open(input_file, mode='r', encoding='utf-8') as infile, \
         open(output_file, mode='w', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
    
        for row in reader:
            
            # fix wrong or misspelled names
            fix_vehicle_year(row)
            fix_lic_plate(row)
        
    
            writer.writerow(row)
            
    end_time = time.time()
    print(f"Finished clean_vehicles() in {end_time - start_time:.2f} seconds")
            
            
            
            