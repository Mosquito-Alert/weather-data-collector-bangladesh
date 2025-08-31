import eccodes
import xarray as xr
import cfgrib
import pandas as pd
from datetime import datetime
import os

currentMonth = datetime.now().month
currentYear = datetime.now().year

for this_year in list(range(2024, currentYear+1, 1)):
  
    if this_year == currentYear:
        these_months = list(range(1, currentMonth+1, 1))
    else:
        these_months = list(range(1, 13, 1))
    
    for this_month in these_months:
        xr.load_dataset(os.path.expanduser('~/research/weather-data-collector-bangladesh/data/output/era5_' + str(this_year) + '_' + f'{this_month:02}' + '.grib'), engine='cfgrib').to_dataframe().to_csv('~/research/weather-data-collector-bangladesh/data/output/era5_' + str(this_year) + '_' + f'{this_month:02}' + '.csv.gz')


