# -*- coding: utf-8 -*-
"""
Created on Wed Dec  4 00:52:16 2024

@author: LDS
"""

import pyodbc
from config import DIMENSIONS, connectionString


def create_sql_table_statements_ssis():
    """
    Generates SQL statements to drop, then create dimension and fact tables based on the DIMENSIONS dictionary.

    Returns:
        list of str: SQL statements to drop and create tables.
    """
    statements = []

    # Drop and create a table for each dimension
    for dimension, columns_with_types in DIMENSIONS.items():
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

    # Drop and create the fact table
    drop_fact_statement = "IF OBJECT_ID('DAMAGE_FACT_SSIS', 'U') IS NOT NULL DROP TABLE [DAMAGE_FACT_SSIS];"
    statements.append(drop_fact_statement)

    fact_table_columns = [
        "[DAMAGE_ID] INT PRIMARY KEY NOT NULL",
        "[DAMAGE_AMOUNT] FLOAT NOT NULL"
    ] + [f"[{dimension}_ID] INT NOT NULL" for dimension in DIMENSIONS.keys()]
    fact_table_statement = f"""
    CREATE TABLE DAMAGE_FACT_SSIS (
        {", ".join(fact_table_columns)}
    );
    """
    statements.append(fact_table_statement)

    return statements





def create_dw_schema_ssis():
    # Create a connection
    connection = pyodbc.connect(connectionString)
    cursor = connection.cursor()

    try:
        # Generate SQL table creation statements
        table_statements = create_sql_table_statements_ssis()

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




