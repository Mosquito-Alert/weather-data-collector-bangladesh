#!/usr/bin/env python3

import cdsapi
from datetime import datetime
import os

currentMonth = datetime.now().month
currentYear = datetime.now().year

c = cdsapi.Client()

for this_year in list(range(2025, currentYear+1, 1)):
    
    if this_year == currentYear:
        these_months = list(range(5, currentMonth, 1))
    else:
        these_months = list(range(9, 13, 1))
    
    for this_month in these_months:
        c.retrieve(
          'reanalysis-era5-land-monthly-means',
          {
              'product_type': 'monthly_averaged_reanalysis',
              'variable': [
                  '10m_u_component_of_wind',  '10m_v_component_of_wind', '2m_dewpoint_temperature',
                  '2m_temperature', 'leaf_area_index_high_vegetation', 'leaf_area_index_low_vegetation',
                  'surface_pressure',
              ],
              'year': this_year,
              'month': this_month,
              'time': '00:00',
              'area': [
                  70.8, -67.7, 29.1,
                  87.5,
              ],
              'format': 'grib',
              "download_format": "unarchived"
          },
          os.path.expanduser('~/research/EuroTiger/data/external_data/era5_' + str(this_year) + '_' + f'{this_month:02}' + '.grib'))
