#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 21 00:41:33 2024

@author: pietro
"""

import csv
import time
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from scripts.utils import get_distinct_values
from shapely import wkt, Polygon, MultiPolygon
from collections import defaultdict
from datetime import datetime
import holidays  # pip install holidays
from collections import defaultdict
from typing import List, Dict, Any, Tuple
from datetime import datetime

### fill coordinates ###

# Define Chicago bounding box
CHICAGO_BBOX = {
    "min_lat": 41.644,
    "max_lat": 42.023,
    "min_lon": -87.940,
    "max_lon": -87.524
}

def is_in_chicago(lat_str: str, lon_str: str) -> bool:
    """Check if latitude and longitude are valid and within Chicago bounds."""
    try:
        lat = float(lat_str)
        lon = float(lon_str)
        return (
            CHICAGO_BBOX["min_lat"] <= lat <= CHICAGO_BBOX["max_lat"]
            and CHICAGO_BBOX["min_lon"] <= lon <= CHICAGO_BBOX["max_lon"]
        )
    except (ValueError, TypeError):
        return False

def get_coordinates(street_no, street_dir, street_name):
    """Fetch coordinates given address components."""
    geolocator = Nominatim(user_agent="crashes_geocoder")
    address = f"{street_no} {street_dir} {street_name}, Chicago, IL"
    try:
        time.sleep(1)  # Nominatim Policy
        location = geolocator.geocode(address)
        if location:
            return location.latitude, location.longitude
        else:
            print(f"NOT FOUND for {address}")
            return None, None
    except GeocoderTimedOut:
        print(f"Timeout for address: {address}")
        return None, None

def fill_coordinates(row):
    """Add missing coordinates to row, as dict."""
    
    # maybe I should add a counter for the filled coord / missing coord
    
    lat = row.get('LATITUDE', '').strip()
    lon = row.get('LONGITUDE', '').strip()
    beat = row.get('BEAT_OF_OCCURRENCE', '')

    if not beat and (not lat or not lon or not is_in_chicago(lat, lon)):
        street_no = row.get('STREET_NO', '').strip()
        street_dir = row.get('STREET_DIRECTION', '').strip()
        street_name = row.get('STREET_NAME', '').strip()

        new_lat, new_lon = get_coordinates(street_no, street_dir, street_name)
        row['LATITUDE'] = new_lat if new_lat else row['LATITUDE']
        row['LONGITUDE'] = new_lon if new_lon else row['LONGITUDE']
        row['LOCATION'] = f"POINT ({new_lon} {new_lat})" if new_lat and new_lon else row['LOCATION']
        print(f"Filled coord in {row}")

    # if not lat or not lon or not is_in_chicago(lat, lon):
    #     street_no = row.get('STREET_NO', '').strip()
    #     street_dir = row.get('STREET_DIRECTION', '').strip()
    #     street_name = row.get('STREET_NAME', '').strip()

    #     new_lat, new_lon = get_coordinates(street_no, street_dir, street_name)
    #     row['LATITUDE'] = new_lat if new_lat else row['LATITUDE']
    #     row['LONGITUDE'] = new_lon if new_lon else row['LONGITUDE']
    #     row['LOCATION'] = f"POINT ({new_lon} {new_lat})" if new_lat and new_lon else row['LOCATION']
    
    return row

### fill beats ###

def get_correct_beats():
    correct_beats = get_distinct_values("data/PoliceBeat.csv", "BEAT_NUM")
    correct_beats = {int(beat) for beat in correct_beats if beat.isdigit()}
    return correct_beats

def get_beat(location):
    """"Return the beat obtained from location"""
    
    police_beats = "data/external/PoliceBeat.csv"  # Downloaded from Chicago Data Portal
    location_point = wkt.loads(location)
    
    try:
        with open(police_beats, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Parse the WKT string into a Shapely geometry object
                beat_polygon = wkt.loads(row['the_geom'])
                beat_num = int(row['BEAT_NUM'].strip())
                
                if not isinstance(beat_polygon, (Polygon, MultiPolygon)):  # Check if it's a valid Polygon or MultiPolygon
                    print(f"AREA of beat {beat_num} IS NOT A POLYGON: {beat_polygon}")
                else:
                    if beat_polygon.contains(location_point):
                        return beat_num
                    
    except FileNotFoundError:
        print(f"Error: File {police_beats} not found.")
    except Exception as e:
        print(f"Error reading file {police_beats}: {e}")


def fill_beat(row):
    """Returns the beat number given the location"""
    beat = row.get('BEAT_OF_OCCURRENCE')
    location = row.get('LOCATION')
    # correct_beats = get_correct_beats()
    
    try:
        if not beat and location: # or beat not in correct_beats
            row['BEAT_OF_OCCURRENCE'] = int(get_beat(location))
            # print(f"Filled with beat in {row}")
        if not beat and not location:
            row['BEAT_OF_OCCURRENCE'] = '' # is there a better placeholder? this breaks the int type
            print(f"Unknown beat in {row}")
        else:
            row['BEAT_OF_OCCURRENCE'] = int(float(beat))  # Convert '1935.0' -> 1935
        
    except ValueError:
        # Handle cases where BEAT_OF_OCCURRENCE is invalid or empty
        pass

    return row


### crimes ###

def get_average_crimes():
    """Returns a dictionary where each beat has avg number of crimes from 2016 to 2018"""
    # skipping the  idea to add the n_crimes related to vehicles for now

    crimes_csv = "data/external/Crimes_2016to2018.csv"

    # Initialize dictionary to store crime counts
    crime_counts = defaultdict(lambda: defaultdict(int)) # A defaultdict automatically assigns a default value to a key if it doesn’t exist
    
    # Read the CSV file
    with open(crimes_csv, mode='r') as file:
        reader = csv.DictReader(file)
        
        # Aggregate crimes by Year and Beat
        for row in reader:
            year = int(row['Year'])
            beat = int(row['Beat'])
            crime_counts[beat][year] += 1

    # Dictionary to store average crimes for each beat
    average_crimes = {}
    
    for beat, years in crime_counts.items():
        total_crimes = sum(years.values())
        num_years = len(years)  # Should be 3 if data is complete
        average_crimes[beat] = int(total_crimes / num_years)

    return average_crimes


def add_crimes(row, average_crimes):
    """Returns the updated row with additional column of avg crimes given beat"""
    
    beat = row.get('BEAT_OF_OCCURRENCE')
    
    try:
        row['BEAT_CRIMES_AVERAGE'] = average_crimes[int(beat)]
        
    except ValueError:
        # Handle cases where BEAT_OF_OCCURRENCE is invalid or empty
        pass

    return row


### holidays ###

def is_holiday(date_str):
    """Returns True if the given date string is a holiday in the specified country."""
    # Parse the date using the correct format
    date = datetime.strptime(date_str, '%m/%d/%Y %I:%M:%S %p')
    country='US'
    holiday_list = holidays.CountryHoliday(country)
    return date.date() in holiday_list  # Compare only the date part



# add time columns functions

def split_date_column(date_string: str) -> Dict[str, Any]:
    """
    Split a date string into its components: day, month, year, and hour using strptime.

    Args:
        date_string (str): The date string in the format "MM/DD/YYYY HH:MM:SS AM/PM".

    Returns:
        Dict[str, Any]: A dictionary containing day, month, year, and hour components.
    """
    try:
        # Define the expected date format
        date_format = "%m/%d/%Y %I:%M:%S %p"
        
        # Parse the date string into a datetime object
        parsed_date = datetime.strptime(date_string, date_format)
        
        # Extract components
        date_components = {
            "day": parsed_date.day,
            "month": parsed_date.month,
            "year": parsed_date.year,
            "hour": parsed_date.hour  # Automatically in 24-hour format
        }
        
        return date_components
    except ValueError as e:
        raise ValueError(f"Error processing date '{date_string}': {e}") from e


def add_crash_date_columns(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract the year from the CRASH_DATE column and add it as CRASH_YEAR to the row.

    Args:
        row (Dict[str, Any]): A dictionary representing a single row of data.

    Returns:
        Dict[str, Any]: The updated row with the added CRASH_YEAR column.
    """
    date_column = "CRASH_DATE"
    date_str = row.get(date_column)
    
    
    try:
        # Check if the date column exists in the row
        if date_column not in row:
            raise ValueError(f"Column '{date_column}' not found in the row: {row}")
        
        # Extract the year using split_date_column
        crash_date_dict = split_date_column(date_str)
        row["DAY"] = crash_date_dict["day"]
        row["MONTH"] = crash_date_dict["month"]
        row["YEAR"] = crash_date_dict["year"]
        row["HOUR"] = crash_date_dict["hour"]
        row['IS_HOLIDAY'] = is_holiday(date_str)
        
    except Exception as e:
        raise ValueError(f"Error processing row: {row}. Ensure the date format is correct.") from e
    
    return row


def add_police_notify_columns(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract components from the 'DATE_POLICE_NOTIFIED' column and add them as new columns.

    Args:
        row (Dict[str, Any]): A dictionary representing a single row of data.

    Returns:
        Dict[str, Any]: The updated row with new columns for police notification date components.
    """
    date_column = "DATE_POLICE_NOTIFIED"
    
    try:
        # Check if the date column exists in the row
        if date_column not in row:
            raise ValueError(f"Column '{date_column}' not found in the row: {row}")
        
        # Extract date components
        date_string = row[date_column]
        police_notify_dict = split_date_column(date_string)
        
        # Add new columns to the row
        row["POLICE_NOTIFY_DAY"] = police_notify_dict["day"]
        row["POLICE_NOTIFY_MONTH"] = police_notify_dict["month"]
        row["POLICE_NOTIFY_YEAR"] = police_notify_dict["year"]
        row["POLICE_NOTIFY_HOUR"] = police_notify_dict["hour"]
        
        row["POLICE_NOTIFY_IS_HOLIDAY"] = is_holiday(date_string)
    
    except Exception as e:
        raise ValueError(f"Error processing row: {row}. Ensure the date format is correct.") from e
    
    return row


### main function to be called in main ###

def clean_crashes(input_file="data/raw/Crashes.csv", output_file="data/cleaned/Crashes_cleaned.csv"):
    """This function process the crashes file.
        It includes various steps, optimized into one iteration over the records.
        Some columns are addressed individually or in groups depending on maintainability of code."""
    
    # utils
    average_crimes = get_average_crimes() # small so I decided to cache instead of creating a csv file
    
    start_time = time.time()
    print("Starting clean_crashes()...")
    
    with open(input_file, mode='r', encoding='utf-8') as infile, \
         open(output_file, mode='w', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        additional_time_columns = ['DAY', 'MONTH', 'YEAR', 'HOUR']
        additional_police_notify_columns = ['POLICE_NOTIFY_DAY','POLICE_NOTIFY_MONTH','POLICE_NOTIFY_YEAR','POLICE_NOTIFY_HOUR']
        additional_external_columns = ['BEAT_CRIMES_AVERAGE', 'IS_HOLIDAY', 'POLICE_NOTIFY_IS_HOLIDAY']
        fieldnames = reader.fieldnames + additional_time_columns + additional_police_notify_columns + additional_external_columns
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
    
        for row in reader:            
            
            # add time columns
            add_crash_date_columns(row)
            add_police_notify_columns(row)
            
            # fill geographical missing data
            fill_coordinates(row) # only for missing beat for now
            fill_beat(row)
        
            # additional data
            add_crimes(row, average_crimes)
        
            writer.writerow(row)
            
    end_time = time.time()
    print(f"Finished clean_crashes() in {end_time - start_time:.2f} seconds")







