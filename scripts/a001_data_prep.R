# Preparation of trap data ####
# Written in R 4.0.3

rm(list=ls()) # clearing memory

# Dependencies ####
library(tidyverse)
library(readxl)
library(sf)
library(leaflet)
library(lubridate)
library(RcppRoll)
library(suncalc) 
library(data.table)
library(SPEI)
library(parallel)
library(janitor)

test = fread("data/output/era5_2025_07.csv.gz")

head(test)

weather_era5_long = bind_rows(lapply(these_variables, function(this_variable){
  fread(file = paste0("data/proc/era5_", this_variable, ".csv.gz")) %>% as_tibble() %>% dplyr::select(-step, -number, -surface, -time) %>% pivot_longer(cols = -c(valid_time, latitude, longitude), names_to = "variable") %>% filter(!is.na(value))
})) 

# double checking that each row is distinct
if(!nrow(weather_era5_long) == weather_era5_long %>% distinct() %>% nrow()){
  stop("there is a problem with the number of rows or weather_era5_long")
}

weather_era5_hourly = weather_era5_long %>% pivot_wider(id_cols = c(valid_time), names_from = variable, values_from = value) %>% 
  mutate(
    temp_c = K2C(t2m), 
    dewpoint_2m_c = K2C(d2m), 
    relative_humidity = rh_magnus(temp_2m_c = temp_c, dewpoint_2m_c = dewpoint_2m_c), 
    windspeed_mps = windspeed_from_components(u10, v10), 
    windspeed_kmph = windspeed_mps*60*60/1000, 
    FW = make_FW(wind_speed = windspeed_mps, units = "mps"), 
    FH = make_FH(relative_humidity), 
    FT = make_FT(temp_c), 
    mwi = FT*FH*FW,
    valid_time = with_tz(valid_time, "Asia/Dhaka")) %>% filter(valid_time %within% study_date_range_interval)

weather_era5_hourly %>% pivot_longer(cols = -valid_time, names_to = "variable", values_to = "value") %>% ggplot(aes(x = valid_time, y = value)) + geom_line() + facet_grid(variable~., scale = "free")

# ggplot(weather_era5_hourly, aes(x = valid_time, y =windspeed_kmph)) + geom_line()+ geom_abline(intercept = 6*3.6, slope = 0, color = "red")

# ggplot(weather_era5_hourly, aes(x = valid_time, y =FW)) + geom_line() 

