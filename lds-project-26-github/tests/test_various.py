#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 25 13:06:14 2024

@author: pietro
"""

### TESTING ###

import os
# os.chdir("/Users/pietro/_DS/LDS/lds-project")
# os.chdir("C:/Users/LDS/Desktop/lds-project")
os.chdir("C:\\Users\\LDS\\Desktop\\lds-project")
print(os.getcwd())

# from scripts.A2_clean_vehicles import clean_vehicles
# clean_vehicles()

# from scripts.utils import print_target_raw
# r = print_target_raw("data/cleaned/Vehicles_cleaned.csv", 80) # row with XX plate

# from scripts.A2_clean_people import clean_people
# clean_people()

# from scripts.utils import print_target_raw
# r = print_target_raw("data/cleaned/People_cleaned.csv", 693) # row with CHGO


### JOIN ###

# from scripts.A4_data_preparation import join_cleaned_tables
# join_cleaned_tables()

### CREATE DIMENSIONS ###

# from scripts.A4_data_preparation import create_data_mart_tables

# create_data_mart_tables()


### CREATING SCHEMA ON SERVER

# import pyodbc
# from config import connectionString
# from A3_dw_schema import create_dw_schema

# cnxn = pyodbc.connect(connectionString)
# cursor = cnxn.cursor()
    
# create_dw_schema()

# cursor.close()
# cnxn.close()


### POPULATING

import pyodbc
from config import connectionString, DIMENSIONS
# from scripts.A5_data_upload import populate_table, populate_all_tables, populate_fact_table

# # populate_all_tables()


# ## TESTING UPLOAD

# dimensions_with_ids = {}
# for dim_name, columns in DIMENSIONS.items():
#     dimensions_with_ids[dim_name] = {f"{dim_name.upper()}_ID": "INT", **columns}

# dimension = "CRASH"
# column_types = dimensions_with_ids[dimension]
# print(column_types)


# table_name = f"{dimension.lower()}_dim"
# csv_file = f"data/datamart/{table_name}.csv"

# connection = pyodbc.connect(connectionString)
# cursor = connection.cursor()

# # Populate the table
# # populate_table(cursor, table_name, csv_file, column_types, batch_size=1000)
# # print(f"Completed population for table: {table_name}\n")

# # Populate fact table
# populate_fact_table()



# cursor.close()
# connection.close()
# print("connection is closed.")



### VALIDATE CSV TYPES


from scripts.A5_data_upload import validate_and_clean_row
from config import DIMENSIONS
import csv

def process_csv(input_file, output_file, dimension):
    """
    Opens a CSV file, applies a cleaning function to each row, 
    and writes the cleaned rows to a new file.

    Args:
        input_file (str): Path to the input CSV file.
        output_file (str): Path to the output CSV file.
        validate_and_clean_row (function): A function that takes a row (list of values)
                                           and returns a cleaned row.
    """
    dimensions_with_ids = {}
    for dim_name, columns in DIMENSIONS.items():
        dimensions_with_ids[dim_name] = {f"{dim_name.upper()}_ID": "INT", **columns}
    print(dimensions_with_ids[dimension])
    
    
    with open(input_file, mode='r', newline='', encoding='utf-8') as infile:
        with open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            # Process header row
            headers = next(reader)
            writer.writerow(headers)
            
            for row in reader:
                cleaned_row = validate_and_clean_row(row, headers, dimensions_with_ids[dimension])
                if cleaned_row:  # Only write rows that pass validation
                    writer.writerow(cleaned_row)

dim = "INJURIES"
input_file = f"data/datamart/{dim.lower()}_dim.csv"
output_file = f"data/datamart_preprocessed/{dim.lower()}_dim_preprocessed.csv"
process_csv(input_file, output_file, dim)





## SSIS TABLE CREATION

statements = []

# Drop and create a table for each dimension
dimension = "INJURIES"
columns_with_types = DIMENSIONS[dimension]
    # Drop statement
drop_statement = f"IF OBJECT_ID('{dimension}_DIM_SSIS', 'U') IS NOT NULL DROP TABLE [{dimension}_DIM_SSIS];"
statements.append(drop_statement)

# Generate column definitions with types
column_definitions = ", ".join([f"[{col}] {col_type}" for col, col_type in columns_with_types.items()])
create_statement = f"""
CREATE TABLE [{dimension}_DIM_SSIS] (
    [{dimension}_ID] INT PRIMARY KEY,
    {column_definitions}
);
"""
statements.append(create_statement)

connection = pyodbc.connect(connectionString)
cursor = connection.cursor()

try:
    # Generate SQL table creation statements
    table_statements = statements

    # Execute each statement
    for statement in table_statements:
        try:
            cursor.execute(statement)
            print(f"Executed statement successfully:\n{statement}")
        except pyodbc.Error as e:
            print(f"Error executing statement:\n{statement}\n{e}")
            continue

    # Commit changes
    connection.commit()

except Exception as e:
    print(f"Unexpected error: {e}")

finally:
    # Close the connection
    cursor.close()
    connection.close()
















