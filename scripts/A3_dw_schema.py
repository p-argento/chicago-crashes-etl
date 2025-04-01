#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec  2 13:05:55 2024

@author: pietro

This should create the tables on the db

"""

import pyodbc
from config import DIMENSIONS, MEASURES, connectionString


def create_sql_table_statements():
    """
    Generates SQL statements to create dimension
    and fact tables based on the DIMENSIONS dictionary.

    Returns:
        list of str: SQL create table statements.
    """
    statements = []

    # Create a table for each dimension
    for dimension, columns_with_types in DIMENSIONS.items():
        # Generate column definitions with types
        column_definitions = ", ".join([f"[{col}] {col_type}" for col, col_type in columns_with_types.items()])
        create_statement = f"""
        CREATE TABLE [{dimension}_dim] (
            [{dimension}_id] INT PRIMARY KEY,
            {column_definitions}
        );
        """
        statements.append(create_statement)

    # Create the fact table
    fact_table_columns = [
        "[damage_id] INT PRIMARY KEY"
    ] + [f"[{measure}] {measure_type}" for measure, measure_type in MEASURES.items()] \
      + [f"[{dimension}_id] INT" for dimension in DIMENSIONS.keys()]
    fact_table_statement = f"""
    CREATE TABLE damage_fact (
        {", ".join(fact_table_columns)}
    );
    """
    statements.append(fact_table_statement)

    print(statements)
    return statements

create_sql_table_statements()


def create_dw_schema():
    # Create a connection
    connection = pyodbc.connect(connectionString)
    cursor = connection.cursor()

    try:
        # Generate SQL table creation statements
        table_statements = create_sql_table_statements()

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




