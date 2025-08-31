#!/bin/sh

# making sure gdal path is correct
export PATH=/home/soft/gdal-2.3.2/bin/:$PATH
export LD_LIBRARY_PATH=/home/soft/gdal-2.3.2/lib/:$LD_LIBRARY_PATH

# starting in project directory
cd ~/research/EuroTiger

# pull in any pending commits
git pull origin main

source /home/soft/virtenvs/python3/copernicus/bin/activate

python3 scripts/a0000_download_era5.py

python3 scripts/a0000_weather_wrangling.py

R CMD BATCH --no-save --no-restore scripts/a0001_era5_weather_prep.R logs/a0001_era5_weather_prep.out 


# Commit and push the log files from this latest run
git add --all
git commit -m 'log files from cluster run of era5 weather update (automated)'
git pull origin main
git push origin main


# run using:
# qsub -q ceab -pe make 1 -l h_vmem=30G -m bea -M johnrbpalmer@gmail.com ~/research/EuroTiger/get_era5_data.sh
