#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 30 17:03:27 2024

@author: pietro
"""

import csv
from scripts.utils import show_progress, write_csv
from config import DIMENSIONS, MEASURES

### join the three tables ###

# Read People data into a list
def read_csv(file_name):
    with open(file_name, mode='r') as file:
        reader = csv.DictReader(file)
        return list(reader)

def join_people_with_crashes(people_path, crashes_path, people_crashes_path):
    """Write csv joined file with People and Crashes on RD_NO"""

    people_data = read_csv(people_path)
    crashes_data = read_csv(crashes_path)
    
    # Index Crashes data by RD_NO for fast lookup
    crashes_index = {row["RD_NO"]: row for row in crashes_data}

    total_records = len(people_data)
    
    with open(people_crashes_path, mode='w', newline='') as file:
        # Combine column headers
        headers = list(people_data[0].keys()) + [
            col for col in crashes_data[0].keys() if col != "RD_NO"
        ]
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()

        for i, person_row in enumerate(people_data, start=1):
            rd_no = person_row["RD_NO"]
            crash_row = crashes_index.get(rd_no, {})  # Default to empty dict if no match
            combined_row = {**person_row, **{col: crash_row.get(col, None) for col in crash_row if col != "RD_NO"}}
            writer.writerow(combined_row)
            
            show_progress(i, total_records, label="processing join_people_with_crashes")
        
        print("completed join_people_with_crashes")

def join_people_crashes_with_vehicles(people_crashes_path, vehicles_path, joined_path_raw):
    """Write csv with all the 3 files joined together"""
    
    key_col = "VEHICLE_ID"
    
    people_crashes_data = read_csv(people_crashes_path)
    vehicles_data = read_csv(vehicles_path)
    
    def fix_vehicle_id(vehicle_id):
        try:
            return int(float(vehicle_id)) if vehicle_id else None
        except ValueError:
            return None
    
    # Normalize VEHICLE_ID in the index
    vehicle_index = {fix_vehicle_id(row[key_col]): row for row in vehicles_data}

    total_records = len(people_crashes_data)

    
    with open(joined_path_raw, mode='w', newline='') as file:
        # Combine column headers
        headers = list(people_crashes_data[0].keys()) + [
            col for col in vehicles_data[0].keys() if col != key_col
        ]
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()

        for i, person_row in enumerate(people_crashes_data, start=1):
            vehicle_id = fix_vehicle_id(person_row[key_col])
            vehicle_row = vehicle_index.get(vehicle_id, {})  # Default to empty dict if no match
            combined_row = {**person_row, **{col: vehicle_row.get(col, None) for col in vehicle_row if col != key_col}}
            writer.writerow(combined_row)
            
            show_progress(i, total_records, label="processing join_people_crashes_with_vehicles")
            
        print("completed join_people_crashes_with_vehicles")

### validation of data types ###


def validate_row(row):
    """
    Validate a single row of data.
    """
    cleaned_row = {}
    # Process each dimension in DIMENSIONS
    for dimension, columns_with_types in DIMENSIONS.items():
        for column, col_type in columns_with_types.items():
            value = row.get(column)
            try:
                if col_type == "INT":
                    # Convert to integer
                    cleaned_row[column] = int(float(value)) if value not in (None, "", "NaN") else None
                elif col_type == "FLOAT":
                    # Convert to float
                    cleaned_row[column] = float(value) if value not in (None, "", "NaN") else None
                elif col_type.startswith("NVARCHAR"):
                    # Ensure it's a string
                    cleaned_row[column] = str(value).strip() if value not in (None, "", "NaN") else None
                elif col_type == "BIT":
                    # Handle bit (boolean) values (0 or 1)
                    cleaned_row[column] = True if value in ("1", "True", "TRUE", True) else False if value in ("0", "False", "FALSE", False) else None
                else:
                    # Default to keeping the value as is
                    cleaned_row[column] = value
            except (ValueError, TypeError):
                # Log a warning if casting fails
                print(f"Warning: Could not cast {value} to {col_type} for column {column}")
                cleaned_row[column] = None  # Replace invalid value with None (null)
                
    return cleaned_row

def validate_all_joined(input_path, output_path):
    """
    Validate and clean data from a joined CSV file using dimensions schema.
    Args:
        input_path (str): Path to the input CSV file.
        output_path (str): Path to save the cleaned CSV file.
        dimensions (dict): Dictionary mapping dimension names to column types.
    """
    # Collect all columns defined in DIMENSIONS
    
    required_columns = [
        col for dimension_dict in DIMENSIONS.values() \
            for col in dimension_dict
    ] + [col for col in MEASURES.keys()]
    
    # Read raw data and filter only required columns
        
    raw_data = read_csv(input_path)
    csv_columns = [column_name.lower() for column_name in raw_data[0]]
    
    missing_columns = [col for col in required_columns if col not in csv_columns]
    if missing_columns:
        print(f"Warning: The following columns are missing from the CSV: {missing_columns}")
    unused_columns = [col for col in csv_columns if col not in required_columns]
    if unused_columns:
        print(f"Warning: The following columns in the CSV are unused: {unused_columns}")

    print(required_columns)


    # filtered_data = [
    #     {key: row[key] for key in required_columns for row in raw_data}
    # ]
    
    filtered_data = []
    for row in raw_data:
        filtered_row = {} 
        for key in required_columns:
            filtered_row.update({key: row[key.upper()]})
        filtered_data.append(filtered_row)
    
    # add cleaned row
    
    cleaned_data = []
    total_records = len(filtered_data)

    for i, row in enumerate(filtered_data, start=1):
        cleaned_row = validate_row(row)
        cleaned_data.append(cleaned_row)
        show_progress(i, total_records, step=1000, label="Filtering and validating all_joined table: ")
        
    # Write cleaned data to output file
    write_csv(output_path, cleaned_data, required_columns)



### split the table ###

def generate_surrogate_keys(data, unique_columns):
    """Generate surrogate keys for unique rows based on specified columns."""
    unique_map = {}
    surrogate_key = 1
    for row in data:
        unique_tuple = tuple(row[col] for col in unique_columns)
        if unique_tuple not in unique_map:
            unique_map[unique_tuple] = surrogate_key
            surrogate_key += 1
    return unique_map

def extract_unique_rows(data, columns):
    seen = set()
    unique_rows = []
    for row in data:
        unique_tuple = tuple(row[col] for col in columns)
        if unique_tuple not in seen:
            seen.add(unique_tuple)
            unique_rows.append({col: row[col] for col in columns})
    return unique_rows

def load_mapping(file_path, key_column):
    """Load mapping of dimension table surrogate keys."""
    with open(file_path, mode="r") as file:
        reader = csv.DictReader(file)
        return {
            tuple(row[col] for col in reader.fieldnames if col != key_column): int(row[key_column])
            for row in reader
        }

def create_dimensions(joined_path):

    joined_data = read_csv(joined_path)

    # Process each dimension using the DIMENSIONS dictionary
    for dimension, columns_with_types in DIMENSIONS.items():
        print(f"Processing {dimension} dimension...")

        output_file = f"data/datamart/{dimension}_dim.csv"
            
        # Extract unique rows
        unique_rows = extract_unique_rows(joined_data, list(columns_with_types.keys()))

        # Generate surrogate keys
        mapping = generate_surrogate_keys(unique_rows, list(columns_with_types.keys()))

        # Prepare rows for writing to CSV
        rows_with_keys = [
            {f"{dimension}_id": mapping[tuple(row[col] for col in columns_with_types.keys())], **row}
            for row in unique_rows
        ]

        # Write to the corresponding dimension file
        write_csv(output_file, rows_with_keys, [f"{dimension}_id"] + list(columns_with_types.keys()))


def create_fact_table(joined_path):

    fact_output = "data/datamart/damage_fact.csv"
    dimension_base_path = "data/datamart"
    fact_headers = ["damage_id"] + [f"{measure}" for measure in MEASURES.keys()]

    # Read the joined data
    joined_data = read_csv(joined_path)

    # Load all dimension mappings dynamically
    dimension_mappings = {}
    for dimension, columns in DIMENSIONS.items():
        mapping_path = f"{dimension_base_path}/{dimension}_dim.csv"
        dimension_mappings[dimension] = load_mapping(mapping_path, f"{dimension}_id")
        fact_headers.append(f"{dimension}_id")

    # Write fact table
    with open(fact_output, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fact_headers)
        writer.writeheader()

        damage_id = 1  # Start surrogate key for the fact table
        total_records = len(joined_data)

        for i, row in enumerate(joined_data, start=1):
            # Create fact row dynamically based on dimensions
            fact_row = {"damage_id": damage_id, "damage_amount": row["damage_amount"]}
            
            for dimension, columns in DIMENSIONS.items():
                tuple_key = tuple(row[col] for col in columns)
                fact_row[f"{dimension}_id"] = dimension_mappings[dimension].get(tuple_key)

            writer.writerow(fact_row)
            damage_id += 1

            show_progress(i, total_records, step=1000, label="Processing fact table: ")
    print()


def create_data_mart_tables():
    
    people_path = "data/cleaned/People_cleaned.csv"
    crashes_path = "data/cleaned/Crashes_cleaned.csv"
    vehicles_path = "data/cleaned/Vehicles_cleaned.csv"
    
    people_crashes_path = "data/joined/People_Crashes_joined.csv"
    joined_path_raw = "data/joined/All_joined_raw.csv"
    joined_path_preprocessed = "data/joined/All_joined_preprocessed.csv"
    
    # left join people with crashes on RD_NO
    print("Joining People with Crashes...")
    join_people_with_crashes(people_path, crashes_path, people_crashes_path)

    # left join people_crashes with vehicles on VEHICLE_ID
    print("Joining also with Vehicles...")
    join_people_crashes_with_vehicles(people_crashes_path, )    
    
    # pre-process all_joined to match the dimensions type
    print("Preprocessing all_joined")
    validate_all_joined(joined_path_raw, joined_path_preprocessed)
    print()
    
    # obtain dimension tables
    print("Creating dimensions...")
    create_dimensions(joined_path_preprocessed)
    print()

    # # obtain fact table
    print("Creating fact table...")
    create_fact_table(joined_path_preprocessed)
    print()

    print("Data mart tables created successfully!")






