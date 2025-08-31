#!/usr/bin/env python3
"""
ERA5 Data Wrangling Script - Long Format Aggregation

This script processes ERA5 GRIB files downloaded by the download script and 
consolidates them into a single long-format CSV file that can be updated daily.

Input: Individual GRIB files per year/month/variable 
       (e.g., era5_2024_01_2m_temperature.grib)
Output: One combined CSV file in long format with all variables
        (era5_weather_data_long.csv.gz)
        
The output format includes columns:
- latitude, longitude, time
- variable_name (e.g., '2m_temperature')
- value (the actual measurement)
- year, month (for tracking)

This long format makes it easy to append new data and handle missing variables.

Author: John Palmer
Date: August 2025
"""

import eccodes
import xarray as xr
import cfgrib
import pandas as pd
from datetime import datetime
import os
import sys
from pathlib import Path

# Configuration
currentMonth = datetime.now().month
currentYear = datetime.now().year

# Set up paths
input_dir = Path.home() / "research" / "weather-data-collector-bangladesh" / "data" / "output"
output_dir = input_dir  # Same directory for output
output_filename = "era5_weather_data_long.csv.gz"
output_path = output_dir / output_filename

def discover_available_data():
    """
    Scan the input directory to discover what years and variables are available
    
    Returns:
        tuple: (sorted list of years, sorted list of variables)
    """
    print("🔍 Scanning directory for available data files...")
    
    if not input_dir.exists():
        print(f"✗ Error: Input directory does not exist: {input_dir}")
        return [], []
    
    years_found = set()
    variables_found = set()
    file_count = 0
    
    # Pattern: era5_YYYY_MM_variable.grib
    for filepath in input_dir.glob("era5_*.grib"):
        filename = filepath.name
        
        # Parse filename: era5_2024_01_2m_temperature.grib
        parts = filename.replace('.grib', '').split('_')
        
        # Expected format: ['era5', 'YYYY', 'MM', 'variable', 'parts...']
        if len(parts) >= 4 and parts[0] == 'era5':
            try:
                year = int(parts[1])
                month = int(parts[2])
                
                # Variable name is everything after the month (rejoin with underscores)
                variable = '_'.join(parts[3:])
                
                # Validate year and month ranges
                if 1900 <= year <= currentYear and 1 <= month <= 12:
                    years_found.add(year)
                    variables_found.add(variable)
                    file_count += 1
                else:
                    print(f"⚠ Skipping file with invalid year/month: {filename}")
                    
            except ValueError:
                print(f"⚠ Skipping file with invalid format: {filename}")
        else:
            print(f"⚠ Skipping file with unexpected format: {filename}")
    
    years_list = sorted(list(years_found))
    variables_list = sorted(list(variables_found))
    
    print(f"✓ Found {file_count} GRIB files")
    print(f"✓ Years available: {years_list}")
    print(f"✓ Variables available: {variables_list}")
    
    return years_list, variables_list

def get_available_months_for_year_variable(year, variable):
    """
    Get list of available months for a specific year and variable
    
    Args:
        year (int): The year to check
        variable (str): The variable name to check
        
    Returns:
        list: Sorted list of available months (1-12)
    """
    months_found = []
    
    for month in range(1, 13):
        filename = f"era5_{year}_{month:02d}_{variable}.grib"
        filepath = input_dir / filename
        
        if check_file_exists(filepath):
            months_found.append(month)
    
    return sorted(months_found)

def check_file_exists(filepath):
    """Check if a file exists and has reasonable size"""
    if not filepath.exists():
        return False
    # Check if file is larger than 1KB (catches empty/corrupt files)
    return filepath.stat().st_size > 1024

def load_grib_to_long_format(filepath, variable_name, year, month):
    """
    Load a GRIB file and convert to long-format pandas DataFrame
    
    Args:
        filepath (Path): Path to the GRIB file
        variable_name (str): Name of the variable for this file
        year (int): Year for this data
        month (int): Month for this data
        
    Returns:
        pd.DataFrame: Long-format DataFrame with the weather data
    """
    try:
        # Load GRIB file using xarray with cfgrib engine
        dataset = xr.load_dataset(filepath, engine='cfgrib')
        
        # Convert to DataFrame
        df = dataset.to_dataframe()
        
        # Reset index to make lat/lon/time regular columns
        df = df.reset_index()
        
        # Get the actual variable column name (it's usually the short name like 't2m', 'tp', etc.)
        variable_columns = [col for col in df.columns if col not in ['latitude', 'longitude', 'time']]
        
        if not variable_columns:
            print(f"⚠ No data columns found in {filepath.name}")
            return pd.DataFrame()
        
        # Convert to long format
        # Melt all variable columns into a single value column
        df_long = df.melt(
            id_vars=['latitude', 'longitude', 'time'],
            value_vars=variable_columns,
            var_name='grib_variable_name',  # The original GRIB variable name
            value_name='value'
        )
        
        # Add our metadata columns
        df_long['variable_name'] = variable_name  # Our standardized variable name
        df_long['year'] = year
        df_long['month'] = month
        
        # Reorder columns for clarity
        df_long = df_long[['latitude', 'longitude', 'time', 'variable_name', 'grib_variable_name', 'value', 'year', 'month']]
        
        return df_long
    
    except Exception as e:
        print(f"✗ Error loading {filepath.name}: {str(e)}")
        return pd.DataFrame()  # Return empty DataFrame on error

def get_existing_data_index():
    """
    Load the existing output file (if it exists) and create an index of what data we already have
    
    Returns:
        set: Set of tuples (year, month, variable_name) for data that already exists
    """
    if not output_path.exists():
        print("📋 No existing output file found - will create new file")
        return set()
    
    try:
        print(f"📋 Loading existing data index from {output_filename}")
        # Read just the metadata columns to build the index
        existing_df = pd.read_csv(output_path, usecols=['year', 'month', 'variable_name'])
        
        # Create set of unique combinations
        existing_combinations = set(
            existing_df[['year', 'month', 'variable_name']].drop_duplicates().itertuples(index=False, name=None)
        )
        
        print(f"✓ Found existing data for {len(existing_combinations)} year/month/variable combinations")
        return existing_combinations
        
    except Exception as e:
        print(f"⚠ Error reading existing file: {str(e)}")
        print("📋 Will treat as new file")
        return set()

def append_to_output_file(df_new):
    """
    Append new data to the output file
    
    Args:
        df_new (pd.DataFrame): New data to append
    """
    if df_new.empty:
        return
    
    if output_path.exists():
        print(f"📝 Appending {len(df_new):,} rows to existing file")
        # Append to existing file
        df_new.to_csv(output_path, mode='a', header=False, index=False, compression='gzip')
    else:
        print(f"📝 Creating new file with {len(df_new):,} rows")
        # Create new file with header
        df_new.to_csv(output_path, index=False, compression='gzip')

def process_all_data(years_available, variables_available):
    """
    Process all available data and append to the combined output file
    
    Args:
        years_available (list): List of available years
        variables_available (list): List of available variables
    """
    print(f"\n📊 Processing all data into long format")
    print("-" * 50)
    
    # Get index of existing data to avoid duplicates
    existing_data = get_existing_data_index()
    
    files_processed = 0
    files_skipped = 0
    files_missing = 0
    total_rows_added = 0
    
    # Process data in batches to manage memory
    batch_data = []
    batch_size = 50  # Process 50 files at a time
    
    # Loop through all year/month/variable combinations
    for year in years_available:
        for variable in variables_available:
            # Get available months for this year/variable combination
            available_months = get_available_months_for_year_variable(year, variable)
            
            if not available_months:
                continue
                
            print(f"  📅 {year} - {variable}: Found months {available_months}")
            
            # Process each available month
            for month in available_months:
                # Check if we already have this data
                if (year, month, variable) in existing_data:
                    print(f"⏭ Skipping: {year}-{month:02d} {variable} (already exists)")
                    files_skipped += 1
                    continue
                
                # Construct the expected filename
                filename = f"era5_{year}_{month:02d}_{variable}.grib"
                filepath = input_dir / filename
                
                if not check_file_exists(filepath):
                    files_missing += 1
                    continue
                
                print(f"✓ Processing: {filename}")
                
                # Load the GRIB file and convert to long format
                df_month = load_grib_to_long_format(filepath, variable, year, month)
                
                if df_month.empty:
                    files_missing += 1
                    continue
                
                batch_data.append(df_month)
                files_processed += 1
                
                # Process batch when it reaches the batch size
                if len(batch_data) >= batch_size:
                    df_batch = pd.concat(batch_data, ignore_index=True)
                    append_to_output_file(df_batch)
                    total_rows_added += len(df_batch)
                    batch_data = []  # Clear the batch
                    print(f"  💾 Processed batch - {total_rows_added:,} total rows added so far")
    
    # Process any remaining data in the final batch
    if batch_data:
        df_batch = pd.concat(batch_data, ignore_index=True)
        append_to_output_file(df_batch)
        total_rows_added += len(df_batch)
        print(f"  💾 Processed final batch")
    
    # Report final file size
    if output_path.exists():
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"📈 Final output file size: {file_size_mb:.1f} MB")
    
    print(f"📈 Processing Summary:")
    print(f"   Files processed: {files_processed}")
    print(f"   Files skipped (already exist): {files_skipped}")
    print(f"   Files missing: {files_missing}")
    print(f"   Total rows added: {total_rows_added:,}")
    
    return files_processed, files_skipped, files_missing

def main():
    """Main execution function"""
    print("ERA5 Data Wrangling - Long Format Consolidation")
    print("=" * 60)
    print(f"Output file: {output_filename}")
    print(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Discover what data is available
    years_available, variables_available = discover_available_data()
    
    if not years_available or not variables_available:
        print("✗ No valid ERA5 GRIB files found in the input directory.")
        print(f"✗ Please check that files exist in: {input_dir}")
        print("✗ Expected format: era5_YYYY_MM_variable.grib")
        sys.exit(1)
    
    print(f"📊 Will process {len(variables_available)} variables across {len(years_available)} years")
    print(f"Input directory: {input_dir}")
    print(f"Output directory: {output_dir}")
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Process all data into one long-format file
    files_processed, files_skipped, files_missing = process_all_data(years_available, variables_available)
    
    # Print final summary
    print("\n" + "=" * 60)
    print("FINAL SUMMARY")
    print("=" * 60)
    print(f"Years processed: {years_available}")
    print(f"Variables available: {variables_available}")
    print(f"Output file: {output_filename}")
    print(f"Files processed: {files_processed}")
    print(f"Files skipped (already existed): {files_skipped}")
    print(f"Files missing: {files_missing}")
    
    total_attempted = files_processed + files_missing
    if total_attempted > 0:
        print(f"Success rate: {(files_processed / total_attempted * 100):.1f}%")
    
    if output_path.exists():
        # Quick peek at the final file structure
        try:
            sample_df = pd.read_csv(output_path, nrows=5)
            print(f"Sample data structure:")
            print(f"  Columns: {list(sample_df.columns)}")
            print(f"  Sample rows: {len(sample_df)}")
        except:
            pass
    
    print("=" * 60)
    print("✓ ERA5 data wrangling completed successfully!")
    print("✓ Ready for daily automated updates")

if __name__ == "__main__":
    main()
