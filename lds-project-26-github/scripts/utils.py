
import csv
import random
import difflib
import sys # for show_progress
import os
import time
from datetime import timedelta


def set_environment(project_path):
    """
    Creates the required folder structure for the project.

    Parameters:
    project_path (str): The base directory where the 'raw' folder is located.

    Folders created:
    - cleaned
    - datamart
    - external
    - joined
    """
    os.chdir(project_path)

    ### create data folders (raw should already be non-empty)
    
    data_folders = ["raw", "cleaned", "datamart", "external", "joined"]
    
    for folder in data_folders:
        folder_path = os.path.join(project_path, "data", folder)
        os.makedirs(folder_path, exist_ok=True)
    
    print("Required folders created")
        
    # INSTALL REQUIREMENTS ???

    

def log_time(start_time, task_name):
    """
    Logs the time taken for a task.
    
    Args:
        start_time (float): The start time of the task.
        task_name (str): The name of the task to display.
    """
    elapsed_time = time.time() - start_time
    
    # Redirect stdout to a file
    output_file = "time.log"
    sys.stdout = open(output_file, 'w')
    print(f"{task_name} Done in {timedelta(seconds=elapsed_time)}")
    sys.stdout.close()
    sys.stdout = sys.__stdout__


def read_csv(file_path):
    """Reads a CSV file and returns the header and rows."""
    try:
        with open(file_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            rows = list(reader)
            return reader.fieldnames, rows
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
        return None, []
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None, []

def write_csv(file_path, rows, header=[]):
    """Writes cleaned data to a new CSV file, automatically detecting the header."""
    if not rows:
        print("Error: No data to write.")
        return

    # If not specified Extract header dynamically from the first row's keys
    if header == []:   
        try:
            header = rows[0].keys()
        except AttributeError:
            print("Error: Rows must be dictionaries with consistent keys.")
            return
    # Write rows
    try:
        with open(file_path, mode='w', encoding='utf-8', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Successfully wrote {len(rows)} rows to {file_path}.")
    except Exception as e:
        print(f"Error writing to file {file_path}: {e}")



def get_sample(file_path, n_samples=1000, seed=42):
    """Get a random sample of the data."""
    random.seed(seed)
    sampled_rows = []

    try:
        with open(file_path, 'r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)

            # Fill the reservoir with initial rows
            for _ in range(n_samples):
                try:
                    row = next(reader)
                    sampled_rows.append(row)
                except StopIteration:
                    print(f"File has fewer rows than requested samples ({len(sampled_rows)} rows).")
                    return sampled_rows

            # Reservoir sampling for remaining rows
            for row_num, row in enumerate(reader, start=n_samples):
                random_pick = random.randint(1, row_num)
                if random_pick < n_samples:
                    sampled_rows[random_pick] = row
                    
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
    except Exception as e:
        print(f"Error while sampling data: {e}")
    return sampled_rows


def write_sample_csv(file_path, output_path, n_samples=100, seed=42):
    """Returns the output path of the sampled csv"""
    
    samples = get_sample(file_path, n_samples=n_samples, seed=seed)
    if not samples:
        print("Error: No samples to write.")
        return

    try:
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=samples[0].keys())
            writer.writeheader()
            writer.writerows(samples)
        print(f"Successfully wrote {len(samples)} sampled rows to {output_path}")
    except Exception as e:
        print(f"Error writing sample to file: {e}")
        
    return output_path

    
def get_distinct_values(file_path, column_name):
    """Returns a set of distinct values from a specific column in a CSV file."""
    distinct_values = set()  # Use a set to store unique values

    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            value = row[column_name]
            distinct_values.add(value)  # Add the value to the set

    return distinct_values

def print_target_raw(file_path, row_number):
    
    with open(file_path, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for i, row in enumerate(reader, start=0):
            if i == row_number:
                print(f"Row {i}: {row}")
                break

    return row


def show_progress(current, total, step=1000, label="Progress"):
    """Display progress in the console, even when stdout is redirected."""
    if current % step == 0 or current == total:
        percent = (current / total) * 100
        # Temporarily redirect stdout to the console
        original_stdout = sys.stdout
        sys.stdout = sys.__stdout__  # Redirect to the console
        try:
            sys.stdout.write(f"\r{label}: {current}/{total} ({percent:.2f}%)")
            sys.stdout.flush()
        finally:
            # Restore the original stdout (log file)
            sys.stdout = original_stdout

### cleaning utils

def read_correct_names(external_data_path):
    """
    Loads valid values from an external CSV file.
    
    Args:
        external_data_path (str): Path to the CSV file containing valid values.
    
    Returns:
        set: A set of valid values.
    """
    valid_values = set()
    with open(external_data_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for data_row in reader:
            valid_values.add(data_row["city"].strip())  # Assuming "city" is the correct column name.
    
    return valid_values

def correct_names(row, column_name, valid_values):
    """
    Corrects a value in the specified column of a row based on valid entries from a pre-loaded set.
    
    Args:
        row (dict): A dictionary representing a row of the dataset.
        column_name (str): The name of the column to correct.
        valid_values (set): A set of valid values loaded from the external data.
    
    Returns:
        dict: The updated row with the corrected value in the specified column.
    """
    # Extract the current value from the row
    current_value = row.get(column_name, "").strip()
    
    if not current_value:
        # If the value is missing or empty, return the row as is
        return row
    
    # Find the closest match for the value from the valid values set
    closest_match = difflib.get_close_matches(current_value, valid_values, n=1, cutoff=0.7)
    
    # Update the row with the corrected value if a match is found
    if closest_match:
        row[column_name] = closest_match[0]
    
    return row




'''from fuzzywuzzy import process, fuzz

def correct_names(original_string, correct_names):
    """Correct names with fuzzy matching"""
    
    fixed_string = ''
    
    if original_string:
        
        match = process.extractOne(original_string, correct_names, scorer=fuzz.ratio)
        if match and match[1] >= 25:  # Use a threshold for correction
            fixed_string = match[0]
            print(f"{original_string} -> {match} -> {fixed_string}")
    
            # Preserve original casing
            if original_string.isupper():
                fixed_string = fixed_string.upper()
            elif original_string.istitle():
                fixed_string = fixed_string.title()
                
        else:
            fixed_string = ''
    
    # if original_string:
    #     match = process.extractOne(original_string, correct_names, scorer=fuzz.ratio)
    #     if match:
    #         print(f"{original_string} -> {match}")
    #         fixed_string = match[0] if match[1] < 100 else original_string
    #         if original_string.isupper():
    #             fixed_string = fixed_string.upper()

    return fixed_string'''


















    