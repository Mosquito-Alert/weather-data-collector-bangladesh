#!/usr/bin/env python3
"""
ERA5 Weather Data Download Script

This script downloads ERA5 reanalysis weather data from the Copernicus Climate Data Store (CDS)
for Bangladesh and surrounding areas. It downloads data by variable, year, and month to create
individual GRIB files that can be processed later.

Requirements:
- cdsapi library (install with: pip install cdsapi)
- Valid CDS API credentials in ~/.cdsapirc

Output files: era5_YYYY_MM_variable.grib (e.g., era5_2024_01_2m_temperature.grib)

Author: John Palmer
Date: August 2025
"""

# Import required libraries
import cdsapi      # Copernicus Climate Data Store API for downloading ERA5 data
from datetime import datetime  # For getting current date/time
import os          # For file system operations

# Get current date information for determining what data to download
currentMonth = datetime.now().month  # Current month (1-12)
currentYear = datetime.now().year    # Current year (e.g., 2025)

# Initialize the CDS API client (requires ~/.cdsapirc with API credentials)
c = cdsapi.Client()

# Set up output directory path and create it if it doesn't exist
output_dir = os.path.expanduser('~/research/weather-data-collector-bangladesh/data/output')
os.makedirs(output_dir, exist_ok=True)  # Create directory structure if needed

# Initialize counters to track download progress and results
total_files_needed = 0    # Count of all files we should have
files_already_exist = 0   # Count of files that already exist (skipped)
files_downloaded = 0      # Count of files successfully downloaded
files_failed = 0          # Count of files that failed to download

# Print initial status information
print(f"Starting ERA5 data download check for years {2024}-{currentYear}")
print(f"Current date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Main download loop: iterate through years, months, and variables
for this_year in range(2024, currentYear+1):  # Download from 2024 to current year
    
    # Determine which months to download based on the year
    if this_year == currentYear:
        # For current year: only download months up to current month (inclusive)
        these_months = list(range(1, currentMonth+1, 1))
    else:
        # For past years: download all 12 months
        these_months = list(range(1, 13, 1))
    
    # Loop through each month for this year
    for this_month in these_months:
        
        # Loop through each weather variable we want to download
        for this_variable in [
            "2m_dewpoint_temperature",    # Dew point temperature at 2 meters above surface
            "2m_temperature",             # Air temperature at 2 meters above surface
            "10m_u_component_of_wind",    # East-west wind component at 10 meters
            "10m_v_component_of_wind",    # North-south wind component at 10 meters
            "surface_pressure",           # Atmospheric pressure at surface
            "total_precipitation"         # Total precipitation (rain/snow)
        ]:
            
            # Create the filename for this specific year/month/variable combination
            filename = f"era5_{this_year}_{this_month:02d}_{this_variable}.grib"
            filepath = os.path.join(output_dir, filename)
            
            # Increment counter of total files we need
            total_files_needed += 1
            
            # Check if file already exists and has reasonable size (> 1MB to catch incomplete downloads)
            if os.path.exists(filepath) and os.path.getsize(filepath) > 1024*1024:
                print(f"✓ Already exists: {filename} ({os.path.getsize(filepath):,} bytes)")
                files_already_exist += 1
                continue  # Skip to next file since this one already exists
            
            # File doesn't exist or is too small - need to download it
            print(f"⬇ Downloading: {filename}")
            
            try:
                # Make the API call to download data from Copernicus CDS
                c.retrieve(
                    'reanalysis-era5-land',  # Dataset name (ERA5 land reanalysis)
                    {
                        # Specify which variable to download
                        'variable': this_variable,
                        
                        # Specify the year as a string
                        'year': str(this_year),
                        
                        # Specify the month with zero-padding (e.g., "01", "02", etc.)
                        'month': "{month:02d}".format(month=this_month),
                        
                        # Download all days of the month (ERA5 handles months with <31 days automatically)
                        'day': [
                            '01', '02', '03',
                            '04', '05', '06', 
                            '07', '08', '09',
                            '10', '11', '12',
                            '13', '14', '15',
                            '16', '17', '18',
                            '19', '20', '21',
                            '22', '23', '24',
                            '25', '26', '27',
                            '28', '29', '30',
                            '31'
                        ],
                        
                        # Download all 24 hours of each day (hourly data)
                        'time': [
                            '00:00', '01:00', '02:00',
                            '03:00', '04:00', '05:00',
                            '06:00', '07:00', '08:00', 
                            '09:00', '10:00', '11:00',
                            '12:00', '13:00', '14:00',
                            '15:00', '16:00', '17:00',
                            '18:00', '19:00', '20:00',
                            '21:00', '22:00', '23:00'
                        ],
                        
                        # Geographic bounding box for Bangladesh and surrounding area
                        # Format: [North, West, South, East] in decimal degrees
                        'area': [
                            26.994917, 86.95028,    # North latitude, West longitude
                            20.204056, 93.500042    # South latitude, East longitude
                        ],
                        
                        # File format specifications
                        "data_format": "grib",           # GRIB format (efficient for meteorological data)
                        "download_format": "unarchived"  # Don't compress/archive the download
                    },
                    filepath  # Save the downloaded data to this file path
                )
                
                # Download successful - update counters and report
                files_downloaded += 1
                print(f"✓ Downloaded: {filename} ({os.path.getsize(filepath):,} bytes)")
                
            except Exception as e:
                # Download failed - update counter and report error
                files_failed += 1
                print(f"✗ Failed to download {filename}: {str(e)}")

# Print final summary of download session
print("\n" + "="*60)
print("DOWNLOAD SUMMARY")
print("="*60)
print(f"Total files needed:     {total_files_needed}")      # How many files we expected to process
print(f"Files already existed:  {files_already_exist}")     # How many were already downloaded (skipped)
print(f"Files downloaded:       {files_downloaded}")        # How many we successfully downloaded this run
print(f"Files failed:           {files_failed}")            # How many downloads failed with errors
# Calculate and display success rate (including both existing and newly downloaded files)
print(f"Success rate:           {((files_already_exist + files_downloaded) / total_files_needed * 100):.1f}%")
print("="*60)
