#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 30 20:04:43 2024

@author: pietro
"""

import csv
import pyodbc
from config import connectionString, DIMENSIONS, MEASURES
from scripts.utils import show_progress


def populate_table(cursor, table_name, csv_file, column_types_dict, batch_size=1000):
    """
    Populate an SQL table with data from a CSV file.
    
    Args:
        cursor (pyodbc.Cursor): The database cursor.
        table_name (str): The name of the table to populate.
        csv_file (str): Path to the CSV file.
        column_types (dict): Dictionary of column types.
        batch_size (int): Number of rows to insert in each batch.
    """
    print(f"Starting population for table: {table_name}")
    
    # Open the CSV file
    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)  # Read the headers
        total_rows = sum(1 for _ in reader)  # Count total rows
        file.seek(0)
        next(reader)  # Reset reader to skip headers again

        # Prepare the SQL insert statement
        placeholders = ", ".join(["?" for _ in headers])
        insert_statement = f"INSERT INTO [{table_name}] ({', '.join(headers)}) VALUES ({placeholders})"

        # Truncate the table before inserting new data
        try:
            cursor.execute(f"TRUNCATE TABLE [{table_name}];")
            print(f"Truncated table: {table_name}")
        except pyodbc.Error as e:
            print(f"Error truncating table {table_name}: {e}")
            print("Proceeding without truncating. Data may be appended.")
        
        # Reset file reader after truncation
        file.seek(0)
        next(reader)  # Skip headers again

        # Batch insert rows
        batch = []
        row_count = 0
        for row in reader:
            
            batch.append(row)
            row_count += 1

            # Execute batch if size is reached
            if len(batch) == batch_size:
                try:
                    cursor.fast_executemany = True # should boost executemany
                    cursor.executemany(insert_statement, batch)
                    cursor.connection.commit()
                except pyodbc.Error as e:
                    print(f"\nError inserting batch into {table_name}: {e}. Logging problematic rows.")
                    for problematic_row in batch:
                        print(f"Failed row: {problematic_row} with error {e}" )
                        print(f"Insert Statement: {insert_statement[:300]}")
                        return
                show_progress(row_count, total_rows, step=batch_size, label=f"Populating {table_name}")
                batch = []  # Clear batch

        # Insert remaining rows
        if batch:
            try:
                cursor.executemany(insert_statement, batch)
                cursor.connection.commit()
            except pyodbc.Error as e:
                print(f"\nError inserting remaining rows into {table_name}: {e}. Logging problematic rows.")
                for problematic_row in batch:
                    print(f"Failed row: {problematic_row}")
            show_progress(row_count, total_rows, step=batch_size, label=f"Populating {table_name}")

    print(f"\nFinished populating table: {table_name}. Total rows: {row_count}.")


def populate_dimensions_tables():
    """
    Populate all tables based on the DIMENSIONS dictionary.
    
    Args:
        connectionString (str): The database connection string.
    """
    
    dimensions_with_ids = {}
    for dim_name, columns in DIMENSIONS.items():
        dimensions_with_ids[dim_name] = {f"{dim_name}_id": "INT", **columns}    
    
    connection = pyodbc.connect(connectionString)
    cursor = connection.cursor()
    try:
        for dimension, columns_with_types in dimensions_with_ids.items():
            
            table_name = f"{dimension}_dim"
            csv_file = f"data/datamart/{table_name}.csv"
            column_types_dict = dimensions_with_ids[dimension]
            
            # Populate the table
            populate_table(cursor, table_name, csv_file, column_types_dict)
            print(f"Completed population for table: {table_name}\n")
    finally:
        cursor.close()
        connection.close()
        print("All tables have been populated and the connection is closed.")



def populate_fact_table(batch_size=1000):
    """
    Populate the fact table from a CSV file.
    
    Args:
        connection_string (str): Database connection string.
        fact_table (dict): Definition of the fact table (columns and types).
        csv_file (str): Path to the CSV file.
        batch_size (int): Number of rows to insert per batch.
    """
    csv_file = "data/datamart/damage_fact.csv"
    
    connection = pyodbc.connect(connectionString)
    cursor = connection.cursor()
    cursor.fast_executemany = True
    
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        headers = next(reader)  # Read headers
        total_rows = sum(1 for _ in reader)
        file.seek(0)
        next(reader)  # Skip headers again

        fact_table_dict = {
            "damage_id": "INT"
        }
        fact_table_dict.update({f"{measure}": "{measure_type}" for measure, measure_type in MEASURES.items()})
        fact_table_dict.update({f"{dimension}_id": "INT" for dimension in DIMENSIONS.keys()})

        placeholders = ", ".join(["?" for _ in headers])
        insert_statement = f"INSERT INTO damage_fact ({', '.join(headers)}) VALUES ({placeholders})"
        
        batch = []
        row_count = 0
        for row in reader:
            
            batch.append(row)
            row_count += 1

            # Insert batch
            if len(batch) == batch_size:
                try:
                    cursor.executemany(insert_statement, batch)
                    connection.commit()
                except pyodbc.Error as e:
                    print(f"\nError inserting batch: {e}")
                    for problematic_row in batch:
                        print(f"Failed row: {problematic_row} with error {e}")
                        return
                batch = []
                show_progress(row_count, total_rows, step=batch_size, label="Populating fact_table")

        # Insert remaining rows
        if batch:
            try:
                cursor.executemany(insert_statement, batch)
                connection.commit()
            except pyodbc.Error as e:
                print(f"\nError inserting batch: {e}")
                for problematic_row in batch:
                    print(f"Failed row: {problematic_row} with error {e}")
                    return
            show_progress(row_count, total_rows, step=batch_size, label="Populating fact_table")
    
    print(f"\nFinished populating fact_table. Total rows: {row_count}")
    cursor.close()
    connection.close()
    

def populate_server_tables():
    
    populate_dimensions_tables()
    populate_fact_table()
    
    return True
    
    
    
    


