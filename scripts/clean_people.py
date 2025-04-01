#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 26 10:04:08 2024

@author: pietro
"""

import csv
import time
# from scripts.utils import correct_names, read_correct_names

def fill_and_fix_damage_amount(row):
    damage_amount = row["DAMAGE_AMOUNT"]
    damage_category = row["DAMAGE_CATEGORY"]
    
    try:
        if not damage_amount and damage_category == "$500 OR LESS":
            damage_amount = 0
        if not damage_amount and damage_category != "$500 OR LESS":
            print(f"MISSING DAMAGE AMOUNT in {row}!!!")
    except:
        raise ValueError(f"Error processing damage amount '{row}'")
    
    row["DAMAGE_AMOUNT"] = round(float(damage_amount), 2)
    
    return row



### main function to be called in main ###

def clean_people(input_file="data/raw/People.csv", output_file="data/cleaned/People_cleaned.csv"):
    """This function processes the People file.
    It includes various steps, optimized into one iteration over the records.
    Some columns are addressed individually or in groups depending on maintainability of code."""

    start_time = time.time()
    print("Starting clean_people()...")

    # Load valid city names from the external file once
    # valid_values = read_correct_names("data/external/city_names.csv")

    with open(input_file, mode='r', encoding='utf-8') as infile, \
        open(output_file, mode='w', encoding='utf-8') as outfile:
            
        reader = csv.DictReader(infile)
        
        # Rename the field in the output file
        fieldnames = [
            "DAMAGE_AMOUNT" if field == "DAMAGE" else field
            for field in reader.fieldnames
        ]
        
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
    
        for row in reader:
            # Rename DAMAGE to DAMAGE_AMOUNT
            row["DAMAGE_AMOUNT"] = row.pop("DAMAGE", None)
            
            # Add zero for missing damage amount
            fill_and_fix_damage_amount(row)
            
            # Example placeholder: correct city names (if needed)
            # correct_names(row, 'CITY', valid_values)
    
            writer.writerow(row)

    end_time = time.time()
    print(f"Finished clean_people() in {end_time - start_time:.2f} seconds")






