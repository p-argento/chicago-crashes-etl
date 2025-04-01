#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec  2 13:10:54 2024

@author: pietro
"""

### DATA MART

DIMENSIONS = {
    "place": {
        # "place_dim" : "INT",
        "latitude": "FLOAT", 
        "longitude": "FLOAT", 
        "street_no": "INT", 
        "street_direction": "NVARCHAR(10)", 
        "street_name": "NVARCHAR(100)", 
        "beat_of_occurrence": "INT", 
        "beat_crimes_average": "INT"
    },
    "date": {
        "day": "INT", 
        "month": "INT", 
        "year": "INT", 
        "hour": "INT", 
        "is_holiday": "BIT"
    },
    "crash": {
        "crash_type": "NVARCHAR(50)", 
        "report_type": "NVARCHAR(50)", 
        "num_units": "INT", 
        "first_crash_type": "NVARCHAR(50)", 
        "prim_contributory_cause": "NVARCHAR(100)", 
        "sec_contributory_cause": "NVARCHAR(100)"
    },
    "vehicle": {
        "unit_type": "NVARCHAR(50)", 
        "make": "NVARCHAR(100)", 
        "model": "NVARCHAR(100)", 
        "lic_plate_state": "NVARCHAR(20)", 
        "vehicle_year": "INT", 
        "vehicle_defect": "NVARCHAR(50)", 
        "vehicle_type": "NVARCHAR(50)", 
        "vehicle_use": "NVARCHAR(50)", 
        "travel_direction": "NVARCHAR(50)", 
        "maneuver": "NVARCHAR(50)", 
        "occupant_cnt": "INT", 
        "first_contact_point": "NVARCHAR(50)"
    },
    "person": {
        "city": "NVARCHAR(50)", 
        "state": "NVARCHAR(50)", 
        "sex": "NVARCHAR(10)", 
        "age": "INT"
    },
    "road_condition": {
        "posted_speed_limit": "INT", 
        "traffic_control_device": "NVARCHAR(50)", 
        "device_condition": "NVARCHAR(50)", 
        "road_defect": "NVARCHAR(50)", 
        "trafficway_type": "NVARCHAR(50)", 
        "alignment": "NVARCHAR(50)", 
        "weather_condition": "NVARCHAR(50)", 
        "lighting_condition": "NVARCHAR(50)", 
        "roadway_surface_cond": "NVARCHAR(50)"
    },
    "safety": {
        "safety_equipment": "NVARCHAR(50)", 
        "airbag_deployed": "NVARCHAR(50)", 
        "ejection": "NVARCHAR(50)", 
        "driver_action": "NVARCHAR(50)", 
        "driver_vision": "NVARCHAR(50)", 
        "physical_condition": "NVARCHAR(50)", 
        "bac_result": "NVARCHAR(50)"
    },
    "injuries": {
        "most_severe_injury": "NVARCHAR(50)", 
        "injuries_total": "INT", 
        "injuries_fatal": "INT", 
        "injuries_incapacitating": "INT", 
        "injuries_non_incapacitating": "INT", 
        "injuries_reported_not_evident": "INT", 
        "injuries_no_indication": "INT"
        # "injuries_unknown": "INT"
    }
}

MEASURES = {
    "damage_amount": "FLOAT"
    # "person_type": "NVARCHAR(50)"
}



### CONNECTION

server = 'lds.di.unipi.it' 
database = 'GROUP_ID_26_DB' 
username = 'GROUP_ID_26' 
password = '096AP3V0'
connectionString = 'DRIVER={ODBC Driver 17 for SQL Server};\
    SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password
