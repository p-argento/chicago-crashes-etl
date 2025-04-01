#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 21 00:46:12 2024

@author: pietro
"""

import time
from scripts.utils import set_environment, log_time
from scripts.A2_data_cleaning import join_cleaned_tables
from scripts.A3_dw_schema import create_dw_schema
from scripts.A4_data_preparation import create_data_mart_tables
from scripts.A5_data_upload import populate_server_tables

def main():
    
    total_start_time = time.time()  # Start total timing
    
    ### SETTING ENVIRONMENT
    print("Setting environment...")
    start_time = time.time()
    project_path = "/Users/pietro/_DS/LDS/lds-project"
    set_environment(project_path)
    log_time(start_time, "Setting environment")
    
    # ### ASSIGNMENT 2 - DATA CLEANING
    print("Processing Assignment 2...")
    start_time = time.time()
    join_cleaned_tables()
    log_time(start_time, "Assignment 2 - Data Cleaning")
    
    # ### ASSIGNMENT 3 - DW SCHEMA
    print("Processing Assignment 3...")
    start_time = time.time()
    # create_dw_schema()
    log_time(start_time, "Assignment 3 - DW Schema")
    
    ### ASSIGNMENT 4 - DATA PREPARATION
    print("Processing Assignment 4...")
    start_time = time.time()
    create_data_mart_tables()
    log_time(start_time, "Assignment 4 - Data Preparation")
    
    # ### ASSIGNMENT 5 - DATA UPLOAD
    print("Processing Assignment 5...")
    start_time = time.time()
    # populate_server_tables()
    log_time(start_time, "Assignment 5 - Data Upload")
    
    # Print total time
    log_time(total_start_time, "Total execution time")
    

if __name__ == "__main__":
    main()

    








