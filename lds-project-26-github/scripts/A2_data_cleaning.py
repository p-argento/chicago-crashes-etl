#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec  2 12:04:55 2024

@author: pietro
"""

from scripts.clean_crashes import clean_crashes
from scripts.clean_people import clean_people
from scripts.clean_vehicles import clean_vehicles

def join_cleaned_tables():
    """Create csv of the 3 joined tables as starting point for split"""
    
    # clean raw tables
    print("Cleaning Crashes...")
    clean_crashes()
    print("Cleaning People...")
    clean_people()
    print("Cleaning Vehicles...")
    clean_vehicles()
    
    return True



