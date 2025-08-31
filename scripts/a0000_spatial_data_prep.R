
rm(list=ls())

# Dependencies ####

library(tidyverse)
library(sf)
library(lubridate)
library(data.table)
library(terra)
library(exactextractr)
library(parallel)
library(mosquitoR)
library(pbapply)
library(pbmcapply)
library(meteor)

source("src/functions.r")
source("src/parameters.r")

sf::sf_use_s2(FALSE)

ncores = 16

# Loading land cover ####
clc = rast("data/external_data/u2018_clc2018_v2020_20u1_raster100m/DATA/U2018_CLC2018_V2020_20u1.tif") 

this_crs = st_crs(clc)

this_bbox_proj = st_bbox(clc)

this_bbox = st_as_sfc(st_bbox(clc)) %>% st_transform(4326) %>% st_bbox() * 1.2

# Loading and writing gadm4 ####
this_gadm4 = st_make_valid(st_read("data/external_data/gadm1-4_shapes/gadm4_all_planet.shp")) %>% st_crop(this_bbox)

write_rds(this_gadm4, file = "data/proc/gadm4_euro_4326.Rds")

this_gadm4_prj = this_gadm4 %>% st_transform(this_crs) 

write_rds(this_gadm4_prj, file = "data/proc/gadm4_euro_3035.Rds")

gadm4_cents = st_centroid(this_gadm4_prj) %>% mutate(cent_x = st_coordinates(.)[,1], cent_y = st_coordinates(.)[,2]) %>% st_drop_geometry() %>% as_tibble() %>% left_join(
  st_centroid(this_gadm4_prj) %>% st_transform(4326) %>% mutate(cent_lon = st_coordinates(.)[,1], cent_lat = st_coordinates(.)[,2]) %>% st_drop_geometry() %>% as_tibble())

write_rds(gadm4_cents, file = "data/proc/gadm4_euro_cents.Rds")

# Extracting landcover ####
gadm4_landcover = exact_extract(clc, this_gadm4_prj, coverage_area = TRUE,progress = TRUE)

length_gadm4_landcover = length(gadm4_landcover)

gadm_landcover_table = rbindlist(pbmclapply(1:length_gadm4_landcover, function(i) {
  gadm4_landcover[[i]] %>% as_tibble() %>% mutate(gid_4 = this_gadm4_prj$gid_4[i]) %>% group_by(value, gid_4) %>% summarize(coverage_area = sum(coverage_area)) %>% as.data.table()
}, mc.cores = ncores))

clc_key = tibble(value = levels(clc)[[1]]$Value, label = levels(clc)[[1]]$LABEL3) %>% mutate(
  lc = case_when(value == 1 ~ "cont_urban_fabric",
                   value ==2 ~ "discont_urban_fabric",
                 value == 4 ~ "roads_rails",
                 value == 10 ~ "green_urban",
                 value == 11 ~ "sports_leisure",
                 value %in% c(3, 5:9) ~ "other_artificial",
                 value %in% 12:22 ~ "agricultural",
                 value %in% 23:29 ~ "forests_scrub",
                 value %in% 30:34 ~ "open",
                 value %in% 35:36 ~ "inland_wetlands",
                 value %in% 37:39 ~ "marine_wetlands",
                 value %in% 40:41 ~ "inland_water",
                 value %in% 42:44 ~ "marine_water",
                 value == 45 ~ "no_data")
)

write_csv(clc_key, "data/proc/clc_key.csv.gz")

gadm_landcover_table_a = left_join(gadm_landcover_table, clc_key) %>% group_by(gid_4, lc) %>% summarize(area = sum(coverage_area)) %>% ungroup() %>% pivot_wider(id_cols = gid_4, names_from = lc, values_from = area, values_fill = 0) %>% dplyr::select(-`NA`) %>% filter(!gid_4 == "?")

fwrite(gadm_landcover_table_a, "data/proc/gadm4_landcover_table.csv.gz")

# Filtering and saving GADM shapes ####

these_gid_4s = fread("data/proc/gadm4_landcover_table.csv.gz", nThread = 10) %>% mutate(total = rowSums(pick(-gid_4))) %>% filter(total > 0 & !is.na(total)) %>% pull(gid_4)


this_gadm4 = this_gadm4 %>% filter(gid_4 %in% these_gid_4s)

write_rds(this_gadm4, file = "data/proc/gadm4_euro_4326.Rds")

this_gadm4_prj = this_gadm4_prj %>% filter(gid_4 %in% these_gid_4s)

write_rds(this_gadm4_prj, file = "data/proc/gadm4_euro_3035.Rds")


this_gadm1 = st_make_valid(st_read("data/external_data/gadm1-4_shapes/gadm1.shp") %>% filter(gid_1 %in% unique(this_gadm4$gid_1))) 

this_gadm2 = st_make_valid(st_read("data/external_data/gadm1-4_shapes/gadm2_all_planet.shp") %>% filter(gid_2 %in% unique(this_gadm4$gid_2))) 

this_gadm3 = st_make_valid(st_read("data/external_data/gadm1-4_shapes/gadm3_all_planet.shp") %>% filter(gid_3 %in% unique(this_gadm4$gid_3))) 

write_rds(this_gadm1, file = "data/proc/gadm1_euro_4326.Rds")

write_rds(this_gadm2, file = "data/proc/gadm2_euro_4326.Rds")

write_rds(this_gadm3, file = "data/proc/gadm3_euro_4326.Rds")

# Cell key ####

# this_gadm4 = read_rds(file = "data/proc/gadm4_euro_4326.Rds")

xmin = round_down(this_bbox$xmin, cell_mask)
xmax = (round_down(this_bbox$xmax, cell_mask) + cell_mask)
ymin = round_down(this_bbox$ymin, cell_mask)
ymax = (round_down(this_bbox$ymax, cell_mask) + cell_mask)

lons = seq(xmin, xmax-cell_mask, cell_mask)
lats = seq(ymin, ymax-cell_mask, cell_mask)

sampling_cell_raster = rast(resolution = cell_mask, crs = 'epsg:4326', xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax, vals = make_samplingcell_ids(lon = rep(lons, length(lats)), lat = unlist(lapply(rev(lats), function(x) rep(x, length(lons))))))

writeRaster(sampling_cell_raster, "data/proc/sampling_cell_raster_euro.tiff", filetype = "GTiff", overwrite = TRUE)

sampling_cell_raster_key = as.data.table(levels(sampling_cell_raster))

colnames(sampling_cell_raster_key)[2] = "TigacellID"

this_gadm4_sampling_cells_list = exact_extract(sampling_cell_raster, this_gadm4, progress = TRUE)

these_gid_4s = this_gadm4 %>% st_drop_geometry() %>% pull(gid_4)
  
this_gadm4_sampling_cells = rbindlist(pbmclapply(1:length(this_gadm4_sampling_cells_list), function(i) {
  this_gadm4_unit = as.data.table(this_gadm4_sampling_cells_list[[i]])[ , gid_4 := these_gid_4s[i]] 
}, mc.cores = ncores))

setkey(sampling_cell_raster_key, value)
setkey(this_gadm4_sampling_cells, value)

this_gadm4_sampling_cells = this_gadm4_sampling_cells[sampling_cell_raster_key, nomatch=0]
this_gadm4_sampling_cells[, value:=NULL]

fwrite(this_gadm4_sampling_cells, file = "data/proc/gadm4_cell_key.csv.gz")


# Winter ####
gadm4_shortest_days = tidyr::expand_grid(date = seq.Date( as_date("2022-01-01"), as_date("2022-12-31"), by="day"), gid_4 = unique(gadm4_cents$gid_4)) %>% left_join(gadm4_cents %>% dplyr::select(gid_4, cent_lat)) %>% mutate(phperiod = photoperiod(date, cent_lat)) %>% group_by(gid_4) %>% slice(which.min(phperiod)) %>% ungroup() %>% mutate(shortest_day_day = day(date), shortest_day_month = month(date)) %>% dplyr::select(gid_4, shortest_day_day, shortest_day_month)

write_rds(gadm4_shortest_days, "data/proc/gadm4_shortest_days.Rds")

