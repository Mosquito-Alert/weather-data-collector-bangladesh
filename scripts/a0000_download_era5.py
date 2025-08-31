#!/usr/bin/env python3

import cdsapi
from datetime import datetime
import os

currentMonth = datetime.now().month
currentYear = datetime.now().year

c = cdsapi.Client()

for this_year in list(range(2024, currentYear+1, 1)):
    
    if this_year == currentYear:
        these_months = list(range(1, currentMonth, 1))
    else:
        these_months = list(range(1, 13, 1))
    
    for this_month in these_months:
        c.retrieve(
          "reanalysis-era5-land",
          {
            "variable": [
        "2m_dewpoint_temperature",
        "2m_temperature",
        "10m_u_component_of_wind",
        "10m_v_component_of_wind",
        "surface_pressure",
        "total_precipitation",
        "leaf_area_index_high_vegetation",
        "leaf_area_index_low_vegetation"
    ],
              'year': this_year,
              'month': this_month,
              "day": [
        "01", "02", "03",
        "04", "05", "06",
        "07", "08", "09",
        "10", "11", "12",
        "13", "14", "15",
        "16", "17", "18",
        "19", "20", "21",
        "22", "23", "24",
        "25", "26", "27",
        "28", "29", "30",
        "31"
    ],
               "time": [
        "00:00", "01:00", "02:00",
        "03:00", "04:00", "05:00",
        "06:00", "07:00", "08:00",
        "09:00", "10:00", "11:00",
        "12:00", "13:00", "14:00",
        "15:00", "16:00", "17:00",
        "18:00", "19:00", "20:00",
        "21:00", "22:00", "23:00"
    ],
              'area': [26.994917, 86.95028, 20.204056, 93.500042],
              'format': 'grib',
              "download_format": "unarchived"
          },
          os.path.expanduser('~/research/weather-data-collector-bangladesh/data/output/era5_' + str(this_year) + '_' + f'{this_month:02}' + '.grib'))
