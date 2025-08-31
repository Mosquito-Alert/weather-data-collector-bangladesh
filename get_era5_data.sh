#!/bin/sh
#SBATCH --partition=ceab
#SBATCH --ntasks=16
#SBATCH --mem=64G
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=johnrbpalmer@gmail.com
#SBATCH --job-name=era5_weather_data
#SBATCH --output=logs/slurm_%j.out
#SBATCH --error=logs/slurm_%j.err

# Track job start
SCRIPT_START_TIME=$(date +%s)

# Load ssh agent since this is no longer done by default on the cluster
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_rsa

# Create logs directory if it doesn't exist
mkdir -p logs

# Simple job tracking function (since we don't have the external script)
log_status() {
    local job_name="$1"
    local status="$2"
    local elapsed="$3"
    local step="$4"
    local message="$5"
    echo "$(date): [$job_name] $status - Step $step - Elapsed: ${elapsed}s - $message"
}

# Track job initialization
log_status "era5_weather_data" "running" 0 5 "Starting ERA5 weather data pipeline"

export HOME=/home/j.palmer
export USER=j.palmer
export LC_CTYPE=C.UTF-8
export LC_COLLATE=C.UTF-8
export LC_TIME=C.UTF-8
export LC_MESSAGES=C.UTF-8
export LC_MONETARY=C.UTF-8
export LC_PAPER=C.UTF-8
export LC_MEASUREMENT=C.UTF-8
export LANG=C.UTF-8

export OMP_NUM_THREADS=1
export NUMEXPR_MAX_THREADS=16

export LD_LIBRARY_PATH=/software/eb/software/PROJ/9.4.1-GCCcore-13.3.0/lib:$LD_LIBRARY_PATH

# Load necessary modules 
log_status "era5_weather_data" "running" $(($(date +%s) - $SCRIPT_START_TIME)) 10 "Loading modules"
module load GDAL/3.10.0-foss-2024a
module load Python/3.12.3-GCCcore-13.3.0
module load PROJ/9.4.1-GCCcore-13.3.0
module load LibTIFF/4.6.0-GCCcore-13.3.0
module load libjpeg-turbo/3.0.1-GCCcore-13.3.0
module load UDUNITS/2.2.28-GCCcore-13.3.0

# Load Miniconda module
module load Miniconda3/24.7.1-0

# Initialize conda for the shell
source /software/eb/software/Miniconda3/24.7.1-0/etc/profile.d/conda.sh

# Activate era5_env environment (create if it doesn't exist)
log_status "era5_weather_data" "running" $(($(date +%s) - $SCRIPT_START_TIME)) 15 "Setting up conda environment"

conda activate era5_env

# Starting in project directory
log_status "era5_weather_data" "running" $(($(date +%s) - $SCRIPT_START_TIME)) 20 "Changing to project directory"
cd ~/research/weather-data-collector-bangladesh

# Pull in any pending commits
log_status "era5_weather_data" "running" $(($(date +%s) - $SCRIPT_START_TIME)) 25 "Pulling latest changes from git"
git pull origin main

# Create output directory if it doesn't exist
mkdir -p data/output

# Run ERA5 data download
log_status "era5_weather_data" "running" $(($(date +%s) - $SCRIPT_START_TIME)) 30 "Starting ERA5 data download"
python3 scripts/a0000_download_era5.py

# Run weather data wrangling
log_status "era5_weather_data" "running" $(($(date +%s) - $SCRIPT_START_TIME)) 80 "Starting to wranglt weather data"
python3 scripts/a0001_wrangle_era5_data.py

# Commit and push the log files from this latest run
log_status "era5_weather_data" "running" $(($(date +%s) - $SCRIPT_START_TIME)) 90 "Committing and pushing results"
git add --all
git commit -m 'log files from cluster run of era5 weather update (automated)'
git pull origin main
git push origin main

# Track job completion
SCRIPT_END_TIME=$(date +%s)
TOTAL_TIME=$((SCRIPT_END_TIME - SCRIPT_START_TIME))
log_status "era5_weather_data" "completed" $TOTAL_TIME 100 "ERA5 weather data pipeline finished successfully"

# Submit using: sbatch get_era5_data.sh
