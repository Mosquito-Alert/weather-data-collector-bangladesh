#!/usr/bin/env python3
"""
ERA5 Data Wrangling Script - Monthly Files

This script processes ERA5 GRIB files and creates separate monthly CSV files
in long format. This approach creates manageable file sizes and allows for
incremental processing.

Output structure:
- data/output/processed/YYYY/era5_YYYY_MM_all_variables.csv.gz

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
import json
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import gc
import warnings

# Suppress xarray future warnings about timedelta decoding
warnings.filterwarnings("ignore", message=".*timedelta64.*", category=FutureWarning)

# Configuration
currentMonth = datetime.now().month
currentYear = datetime.now().year

# Performance tuning for HPC
BATCH_SIZE = 6  # Process all variables for one month at a time
MAX_WORKERS = min(6, multiprocessing.cpu_count())  # One worker per variable

# Set up paths
input_dir = Path.home() / "research" / "weather-data-collector-bangladesh" / "data" / "output"
processed_dir = input_dir / "processed"
metadata_file = input_dir / "processing_metadata.json"

def load_metadata():
    """Load processing metadata to track what's been processed"""
    if metadata_file.exists():
        with open(metadata_file, 'r') as f:
            return json.load(f)
    return {"processed_months": [], "last_updated": None}

def save_metadata(metadata):
    """Save processing metadata"""
    metadata["last_updated"] = datetime.now().isoformat()
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)

def discover_available_data():
    """Discover available data organized by year and month"""
    print("🔍 Scanning directory for available data files...")
    
    if not input_dir.exists():
        print(f"✗ Error: Input directory does not exist: {input_dir}")
        return {}
    
    # Organize files by year and month
    data_by_month = {}
    file_count = 0
    
    for filepath in input_dir.glob("era5_*.grib"):
        filename = filepath.name
        parts = filename.replace('.grib', '').split('_')
        
        if len(parts) >= 4 and parts[0] == 'era5':
            try:
                year = int(parts[1])
                month = int(parts[2])
                variable = '_'.join(parts[3:])
                
                if 1900 <= year <= currentYear and 1 <= month <= 12:
                    month_key = f"{year}_{month:02d}"
                    if month_key not in data_by_month:
                        data_by_month[month_key] = {
                            'year': year,
                            'month': month,
                            'variables': {}
                        }
                    data_by_month[month_key]['variables'][variable] = filepath
                    file_count += 1
                    
            except ValueError:
                continue
    
    print(f"✓ Found {file_count} GRIB files organized into {len(data_by_month)} months")
    return data_by_month

def check_file_exists(filepath):
    """Check if a file exists and has reasonable size"""
    if not filepath.exists():
        return False
    return filepath.stat().st_size > 1024

def load_grib_to_long_format(filepath, variable_name, year, month):
    """Load a GRIB file and convert to long-format DataFrame"""
    try:
        # Try optimized loading first (with dask if available)
        try:
            with xr.open_dataset(filepath, engine='cfgrib', chunks={'time': 100}) as dataset:
                df = dataset.to_dataframe()
        except (ImportError, ValueError) as e:
            if "dask" in str(e).lower() or "chunk" in str(e).lower():
                with xr.open_dataset(filepath, engine='cfgrib') as dataset:
                    df = dataset.to_dataframe()
            else:
                raise e
        
        df = df.reset_index()
        
        # Get variable columns (exclude coordinates)
        # These are common ERA5 coordinate/metadata columns that should not be treated as data variables
        potential_coordinate_columns = ['latitude', 'longitude', 'time', 'step', 'valid_time', 'number', 'surface', 'heightAboveGround', 'isobaricInhPa']
        coordinate_columns = [col for col in potential_coordinate_columns if col in df.columns]
        variable_columns = [col for col in df.columns if col not in coordinate_columns]
        
        if not variable_columns:
            return pd.DataFrame()
        
        # Convert to long format
        # Use the best available time coordinate (prefer valid_time, fallback to time)
        time_col = 'valid_time' if 'valid_time' in df.columns else 'time'
        id_vars = ['latitude', 'longitude', time_col]
        
        df_long = pd.melt(
            df,
            id_vars=id_vars,
            value_vars=variable_columns,
            var_name='grib_variable_name',
            value_name='value'
        )
        
        # Rename time column to be consistent
        if time_col != 'time':
            df_long = df_long.rename(columns={time_col: 'time'})
        
        # Add metadata
        df_long = df_long.assign(
            variable_name=variable_name,
            year=year,
            month=month
        )
        
        # Handle data type conversion safely
        df_long['latitude'] = pd.to_numeric(df_long['latitude'], errors='coerce').astype('float32')
        df_long['longitude'] = pd.to_numeric(df_long['longitude'], errors='coerce').astype('float32')
        
        # Handle potential Timedelta objects in value column before numeric conversion
        if df_long['value'].dtype == 'object':
            def convert_value(x):
                if pd.isna(x):
                    return x
                elif hasattr(x, 'total_seconds'):  # Timedelta object
                    return float(x.total_seconds())
                else:
                    return x
            
            df_long['value'] = df_long['value'].apply(convert_value)
        
        # Convert values, handling non-numeric data
        df_long['value'] = pd.to_numeric(df_long['value'], errors='coerce')
        
        # Remove rows with invalid values
        initial_rows = len(df_long)
        df_long = df_long.dropna(subset=['value'])
        if len(df_long) < initial_rows:
            print(f"  ⚠ Removed {initial_rows - len(df_long)} rows with invalid values from {filepath.name}")
        
        if len(df_long) > 0:
            df_long['value'] = df_long['value'].astype('float32')
            df_long['year'] = df_long['year'].astype('int16')
            df_long['month'] = df_long['month'].astype('int8')
        
        # Reorder columns
        df_long = df_long[['latitude', 'longitude', 'time', 'variable_name', 'grib_variable_name', 'value', 'year', 'month']]
        
        return df_long
    
    except Exception as e:
        print(f"✗ Error loading {filepath.name}: {str(e)}")
        return pd.DataFrame()

def process_month(year, month, variables_dict):
    """Process all variables for a specific month"""
    month_key = f"{year}_{month:02d}"
    print(f"\n📅 Processing {year}-{month:02d}")
    print("-" * 40)
    
    # Set up output directory and file
    year_dir = processed_dir / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = year_dir / f"era5_{year}_{month:02d}_all_variables.csv.gz"
    
    # Check if already processed
    if output_file.exists() and output_file.stat().st_size > 1024:
        print(f"✓ Already processed: {output_file.name} ({output_file.stat().st_size / 1024 / 1024:.1f} MB)")
        return True, month_key, 0
    
    # Process all variables for this month
    monthly_data = []
    files_processed = 0
    
    for variable_name, filepath in variables_dict.items():
        if not check_file_exists(filepath):
            print(f"⚠ Missing file: {filepath.name}")
            continue
            
        print(f"  ✓ Processing: {variable_name}")
        df_var = load_grib_to_long_format(filepath, variable_name, year, month)
        
        if not df_var.empty:
            monthly_data.append(df_var)
            files_processed += 1
        else:
            print(f"  ⚠ No data from: {variable_name}")
    
    # Combine all variables for this month
    if monthly_data:
        print(f"  📊 Combining {len(monthly_data)} variables...")
        df_month = pd.concat(monthly_data, ignore_index=True)
        
        # Save monthly file
        print(f"  💾 Saving to: {output_file.name}")
        df_month.to_csv(output_file, index=False, compression='gzip')
        
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        print(f"  ✓ Saved: {len(df_month):,} rows, {file_size_mb:.1f} MB")
        
        # Clean up memory
        del df_month
        del monthly_data
        gc.collect()
        
        return True, month_key, files_processed
    else:
        print(f"  ✗ No data processed for {year}-{month:02d}")
        return False, month_key, 0

def create_recent_combined_file():
    """Create a combined file with the most recent 3 months for quick analysis"""
    print("\n📋 Creating recent data summary...")
    
    # Find the 3 most recent monthly files
    recent_files = []
    for year_dir in sorted(processed_dir.glob("*"), reverse=True):
        if year_dir.is_dir():
            for monthly_file in sorted(year_dir.glob("era5_*.csv.gz"), reverse=True):
                recent_files.append(monthly_file)
                if len(recent_files) >= 3:
                    break
            if len(recent_files) >= 3:
                break
    
    if recent_files:
        combined_file = input_dir / "era5_recent_3months.csv.gz"
        print(f"  📊 Combining {len(recent_files)} recent files...")
        
        # Read and combine recent files
        recent_data = []
        for file in recent_files:
            df = pd.read_csv(file)
            recent_data.append(df)
            print(f"  ✓ Loaded: {file.name} ({len(df):,} rows)")
        
        df_combined = pd.concat(recent_data, ignore_index=True)
        df_combined.to_csv(combined_file, index=False, compression='gzip')
        
        file_size_mb = combined_file.stat().st_size / (1024 * 1024)
        print(f"  💾 Saved recent summary: {len(df_combined):,} rows, {file_size_mb:.1f} MB")

def main():
    """Main execution function"""
    print("ERA5 Data Wrangling - Monthly File Organization")
    print("=" * 60)
    print(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load metadata
    metadata = load_metadata()
    processed_months = set(metadata.get("processed_months", []))
    
    # Discover available data
    data_by_month = discover_available_data()
    
    if not data_by_month:
        print("✗ No valid ERA5 GRIB files found.")
        sys.exit(1)
    
    print(f"📊 Found data for {len(data_by_month)} months")
    print(f"📋 Previously processed: {len(processed_months)} months")
    
    # Process each month
    total_processed = 0
    total_files = 0
    
    for month_key in sorted(data_by_month.keys()):
        month_info = data_by_month[month_key]
        expected_output = processed_dir / str(month_info['year']) / f"era5_{month_info['year']}_{month_info['month']:02d}_all_variables.csv.gz"

        if month_key in processed_months and check_file_exists(expected_output):
            print(f"⏭ Skipping {month_key} (already processed and output exists)")
            continue

        if month_key in processed_months and not check_file_exists(expected_output):
            print(f"↻ Reprocessing {month_key} (metadata marked processed but output missing/invalid)")
            
        success, processed_key, files_count = process_month(
            month_info['year'], 
            month_info['month'], 
            month_info['variables']
        )
        
        if success:
            processed_months.add(processed_key)
            total_processed += 1
            total_files += files_count
    
    # Update metadata
    metadata["processed_months"] = list(processed_months)
    save_metadata(metadata)
    
    # Create recent summary
    create_recent_combined_file()
    
    # Print final summary
    print("\n" + "=" * 60)
    print("FINAL SUMMARY")
    print("=" * 60)
    print(f"Months processed this run: {total_processed}")
    print(f"Files processed this run: {total_files}")
    print(f"Total months available: {len(data_by_month)}")
    print(f"Output directory: {processed_dir}")
    print("=" * 60)
    print("✓ ERA5 monthly processing completed!")

if __name__ == "__main__":
    main()
