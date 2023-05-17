# Databricks notebook source
# MAGIC %md # Data Needed
# MAGIC
# MAGIC > Looking at options, starting from https://apps.nationalmap.gov/datasets/, __bolded__ below are the assets used.
# MAGIC
# MAGIC * Soil - [Natural Resources Conservation Service Data - Overview](https://www.nrcs.usda.gov/conservation-basics/natural-resource-concerns/soils/soil-geography)
# MAGIC   * __[NY Gridded National Soil Survey Data](https://nrcs.app.box.com/v/soils/file/1055994723078) (FileGeoDB)__ - [Gridded Soil Survey Geographic (gSSURGO) Overview](https://www.nrcs.usda.gov/resources/data-and-reports/gridded-soil-survey-geographic-gssurgo-database)
# MAGIC   * _Note: Can order some additional data [here](https://gdg.sc.egov.usda.gov/GDGOrder.aspx?order=QuickState)_
# MAGIC * Vegetation
# MAGIC   * __[NY - Download BBox](https://www.mrlc.gov/viewer/?downloadBbox=40.26171,45.12257,-79.83840,-71.52651) (TIFF)__ - this was custom product emailed
# MAGIC   * __[National Land Cover Data](https://s3-us-west-2.amazonaws.com/mrlc/nlcd_2019_land_cover_l48_20210604.zip) (IMG)__ - [Overview](https://www.mrlc.gov/data?f%5B0%5D=category%3Aland%20cover&f%5B1%5D=region%3Aconus)
# MAGIC * Weather
# MAGIC   * __[Gridded Precipitation Daily](https://downloads.psl.noaa.gov/Datasets/cpc_us_precip/) (NetCDF)__ - [Overview](https://psl.noaa.gov/data/gridded/data.unified.daily.conus.html)
# MAGIC * Hydrography
# MAGIC   * [National Hydrography Data](https://www.sciencebase.gov/catalog/item/4f5545cce4b018de15819ca9) - [Overview](https://www.usgs.gov/national-hydrography) 
# MAGIC   * __[NY Hydrography Data](https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHD/State/Shape/NHD_H_New_York_State_Shape.zip) (Shapefile)__ - [Overview](https://www.sciencebase.gov/catalog/item/61f8b8a5d34e622189c328b2)
# MAGIC * Road & Bridge
# MAGIC   * __[NY Road Data](https://www2.census.gov/geo/tiger/TIGER_RD18/LAYER/ROADS/) (Shapefiles | NY is '36')__ - [Overview](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html)
# MAGIC   * __[NY Bridge Data](https://gis.ny.gov/gisdata/fileserver/?DSID=397&file=NYSDOTBridges.zip) (FileGeoDB)__ - [Overview](https://gis.ny.gov/gisdata/inventories/details.cfm?DSID=397)
# MAGIC * Flood Zones
# MAGIC   * [National Flood Hazard Layer](https://www.fema.gov/flood-maps/national-flood-hazard-layer)
# MAGIC   * _Note: [NY NFHL](https://hazards.fema.gov/femaportal/NFHL/searchResult) (Shapefiles) - Search 'New York'_
# MAGIC
# MAGIC __Data Storage (DBFS)__
# MAGIC
# MAGIC > `/dbfs/geospatial/hazard_risk`
# MAGIC
# MAGIC __Database__
# MAGIC
# MAGIC > `hazard_risk`
# MAGIC
# MAGIC ---
# MAGIC __Author:__ Michael Johns <mjohns@databricks.com>  
# MAGIC _Last Modified_: 23 FEB, 2023

# COMMAND ----------

# MAGIC %md ## Setup

# COMMAND ----------

# MAGIC %run "./config_hazard_risk.py"

# COMMAND ----------

# MAGIC %md ## NY Soil (__689MB__)
# MAGIC
# MAGIC > From https://nrcs.app.box.com/v/soils/file/1055994723078 (FileGeoDB)
# MAGIC
# MAGIC __Note: This was manually downloaded then uploaded to DBFS via [CLI](https://docs.databricks.com/dev-tools/cli/index.html).__

# COMMAND ----------

dbutils.fs.mkdirs(f"{os.environ['HAZARD_RISK_DBFS']}/soil-new")
display(dbutils.fs.ls(f"{os.environ['HAZARD_RISK_DBFS']}/soil-new"))

# COMMAND ----------

ls -l --block-size=M $HAZARD_RISK_FUSE/soil-new/gSSURGO_NY.gdb.zip

# COMMAND ----------

# MAGIC %md ## Vegetation (Land Cover)

# COMMAND ----------

dbutils.fs.mkdirs(f"{os.environ['HAZARD_RISK_DBFS']}/vegetation")

# COMMAND ----------

# MAGIC %md ### NY Specific Data (~22GB)
# MAGIC
# MAGIC > The URL can lead you to the map viewer with selected download bbox: https://www.mrlc.gov/viewer/?downloadBbox=40.26171,45.12257,-79.83840,-71.52651 ([data](https://www.mrlc.gov/downloads/sciweb1/shared/mrlc/download-tool/NLCD_Bvbsw5eIUh9dxo3noHj3.zip) was only available for 24 hours) (TIFF)
# MAGIC
# MAGIC __Note: This data is result of a special request at https://www.mrlc.gov/viewer/ emailed to author.__

# COMMAND ----------

# MAGIC %sh wget 
# MAGIC rm -f ny_land_cover_all.zip
# MAGIC wget -O ny_land_cover_all.zip "https://www.mrlc.gov/downloads/sciweb1/shared/mrlc/download-tool/NLCD_Bvbsw5eIUh9dxo3noHj3.zip"
# MAGIC cp ny_land_cover_all.zip $HAZARD_RISK_FUSE/vegetation
# MAGIC ls $HAZARD_RISK_FUSE/vegetation

# COMMAND ----------

ls -l --block-size=M $HAZARD_RISK_FUSE/vegetation/ny_land_cover_all.zip

# COMMAND ----------

# MAGIC %md __Unzip the download into `../vegetation/ny`__
# MAGIC
# MAGIC > Results in __40__ TIFFs (__~500MB per__)

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p $HAZARD_RISK_FUSE/vegetation/ny
# MAGIC unzip $HAZARD_RISK_FUSE/vegetation/ny_land_cover_all.zip -d $HAZARD_RISK_FUSE/vegetation/ny

# COMMAND ----------

display(dbutils.fs.ls(f"{os.environ['HAZARD_RISK_DBFS']}/vegetation/ny"))

# COMMAND ----------

ls -l --block-size=M $HAZARD_RISK_FUSE/vegetation/ny/NLCD_01-19_Land_Cover_Change_First_Disturbance_Date_20210604_Bvbsw5eIUh9dxo3noHj3.tiff # representative file

# COMMAND ----------

ls $HAZARD_RISK_FUSE/vegetation/ny/*.tiff | wc -l # how many files?

# COMMAND ----------

# MAGIC %md ### National Data (2.3GB)
# MAGIC
# MAGIC > From https://s3-us-west-2.amazonaws.com/mrlc/nlcd_2019_land_cover_l48_20210604.zip (IMG + Others)

# COMMAND ----------

# MAGIC %sh wget 
# MAGIC rm -f nlcd_land_cover_2019.zip
# MAGIC wget -O nlcd_land_cover_2019.zip "https://s3-us-west-2.amazonaws.com/mrlc/nlcd_2019_land_cover_l48_20210604.zip"
# MAGIC cp nlcd_land_cover_2019.zip $HAZARD_RISK_FUSE/vegetation
# MAGIC ls $HAZARD_RISK_FUSE/vegetation

# COMMAND ----------

display(dbutils.fs.ls(f"{os.environ['HAZARD_RISK_DBFS']}/vegetation"))

# COMMAND ----------

ls -l --block-size=M $HAZARD_RISK_FUSE/vegetation/nlcd_land_cover_2019.zip

# COMMAND ----------

# MAGIC %md __Unzip the download into `../vegetation/data`__

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p $HAZARD_RISK_FUSE/vegetation/data
# MAGIC unzip $HAZARD_RISK_FUSE/vegetation/nlcd_land_cover_2019.zip -d $HAZARD_RISK_FUSE/vegetation/data

# COMMAND ----------

display(dbutils.fs.ls(f"{os.environ['HAZARD_RISK_DBFS']}/vegetation/data"))

# COMMAND ----------

# MAGIC %md ## National Daily Weather (1991 - 2020 | __42MB__)
# MAGIC
# MAGIC > From https://downloads.psl.noaa.gov/Datasets/cpc_us_precip/precip.V1.0.day.ltm.1991-2020.nc  (NetCDF)

# COMMAND ----------

dbutils.fs.mkdirs(f"{os.environ['HAZARD_RISK_DBFS']}/weather")

# COMMAND ----------

# MAGIC %sh 
# MAGIC rm -f precip.V1.0.day.ltm.1991-2020.nc
# MAGIC wget "https://downloads.psl.noaa.gov/Datasets/cpc_us_precip/precip.V1.0.day.ltm.1991-2020.nc"
# MAGIC cp precip.V1.0.day.ltm.1991-2020.nc $HAZARD_RISK_FUSE/weather
# MAGIC ls $HAZARD_RISK_FUSE

# COMMAND ----------

display(dbutils.fs.ls(f"{os.environ['HAZARD_RISK_DBFS']}/weather"))

# COMMAND ----------

ls -l --block-size=M $HAZARD_RISK_FUSE/weather/precip.V1.0.day.ltm.1991-2020.nc

# COMMAND ----------

# MAGIC %md ## NY Watershed Streams (Hydrography | __821MB__)
# MAGIC
# MAGIC > From https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHD/State/Shape/NHD_H_New_York_State_Shape.zip (Shapefile)

# COMMAND ----------

dbutils.fs.mkdirs(f"{os.environ['HAZARD_RISK_DBFS']}/hydrography")

# COMMAND ----------

# MAGIC %sh 
# MAGIC rm -f ny_hydrography.shp.zip
# MAGIC wget -O ny_hydrography.shp.zip "https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHD/State/Shape/NHD_H_New_York_State_Shape.zip"
# MAGIC cp ny_hydrography.shp.zip $HAZARD_RISK_FUSE/hydrography
# MAGIC ls $HAZARD_RISK_FUSE/hydrography

# COMMAND ----------

display(dbutils.fs.ls(f"{os.environ['HAZARD_RISK_DBFS']}/hydrography"))

# COMMAND ----------

ls -l --block-size=M $HAZARD_RISK_FUSE/hydrography/ny_hydrography.shp.zip

# COMMAND ----------

# MAGIC %md __Unzip the download into `../hydrography/data`__

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p $HAZARD_RISK_FUSE/hydrography/data
# MAGIC unzip $HAZARD_RISK_FUSE/hydrography/ny_hydrography.shp.zip -d $HAZARD_RISK_FUSE/hydrography/data

# COMMAND ----------

display(dbutils.fs.ls(f"{os.environ['HAZARD_RISK_DBFS']}/hydrography/data/Shape"))

# COMMAND ----------

# MAGIC %sh (cd $HAZARD_RISK_FUSE/hydrography/data/Shape/ && ls *.shp | sed 's/\.shp$//') > hydrography_shapes.txt

# COMMAND ----------

cat hydrography_shapes.txt

# COMMAND ----------

# MAGIC
# MAGIC %sh
# MAGIC
# MAGIC rm -rf $HAZARD_RISK_FUSE/hydgrography/shapefile
# MAGIC mkdir -p $HAZARD_RISK_FUSE/hydrography/shapefile
# MAGIC
# MAGIC rm -rf hydrography
# MAGIC mkdir hydrography
# MAGIC
# MAGIC FILES=$(cat hydrography_shapes.txt)
# MAGIC for f in $FILES
# MAGIC do
# MAGIC zip hydrography/$f.shp.zip $(ls $HAZARD_RISK_FUSE/hydrography/data/Shape/$f.*)
# MAGIC echo "copying $f.shp.zip..."
# MAGIC cp hydrography/$f.shp.zip $HAZARD_RISK_FUSE/hydrography/shapefile
# MAGIC done

# COMMAND ----------

display(dbutils.fs.ls(f"{os.environ['HAZARD_RISK_DBFS']}/hydrography/shapefile"))

# COMMAND ----------

dbutils.fs.rm(f"{os.environ['HAZARD_RISK_DBFS']}/hydrography/data", True) # remove the interim unzip

# COMMAND ----------

# MAGIC %md ## NY Roads (TIGERLINES)
# MAGIC
# MAGIC > From https://www2.census.gov/geo/tiger/TIGER_RD18/LAYER/ROADS/ (Shapefiles)

# COMMAND ----------

dbutils.fs.mkdirs(f"{os.environ['HAZARD_RISK_DBFS']}/road/shapefile")

# COMMAND ----------

# MAGIC %md __Get List of Shapefile ZIPs__

# COMMAND ----------

# MAGIC %sh 
# MAGIC rm -f roads.txt
# MAGIC wget -O roads.txt "https://www2.census.gov/geo/tiger/TIGER_RD18/LAYER/ROADS/"
# MAGIC cp roads.txt $HAZARD_RISK_FUSE

# COMMAND ----------

# MAGIC %md __Figure out which rows are within the `<table>` tag and extract the filenames.__
# MAGIC
# MAGIC > Since this is all in one file being read on one node, get consistent ordered id for `row_num` (not always true).

# COMMAND ----------

tbl_start_row = (
  spark.read.text(f"{os.environ['HAZARD_RISK_DBFS']}/roads.txt")
  .withColumn("row_num", F.monotonically_increasing_id())
  .withColumn("tbl_start_row", F.trim("value") == '<table>')
  .filter("tbl_start_row = True")
  .select("row_num")
).collect()[0][0]

tbl_end_row = (
  spark.read.text(f"{os.environ['HAZARD_RISK_DBFS']}/roads.txt")
  .withColumn("row_num", F.monotonically_increasing_id())
  .withColumn("tbl_end_row", F.trim("value") == '</table>')
  .filter("tbl_end_row = True")
  .select("row_num")
).collect()[0][0]

print(f"tbl_start_row: {tbl_start_row}, tbl_end_row: {tbl_end_row}")

# COMMAND ----------

# new york is 36

ny_files = [r[1] for r in (
  spark.read.text(f"{os.environ['HAZARD_RISK_DBFS']}/roads.txt")
  .withColumn("row_num", F.monotonically_increasing_id())
    .filter(f"row_num > {tbl_start_row}")
    .filter(f"row_num < {tbl_end_row}")
  .withColumn("href_start", F.substring_index("value", 'href="', -1))
  .withColumn("href", F.substring_index("href_start", '">', 1))
    .filter(col("href").startswith("tl_rd22_36"))
  .select("row_num","href")
).collect()]

print(f"len ny_files? {len(ny_files):,}")
ny_files[:5]

# COMMAND ----------

# MAGIC %md __Download Shapefile ZIPs (62 | ~4MB each)__
# MAGIC
# MAGIC > Could do this in parallel, but keeping on just driver for now so as to not overload Census server with requests.

# COMMAND ----------

import pathlib
import requests

fuse_path = pathlib.Path(f"{os.environ['HAZARD_RISK_FUSE']}/road/shapefile")
fuse_path.mkdir(parents=True, exist_ok=True)

for idx,f in enumerate(ny_files):
  idx_str = str(idx).rjust(4)
  fuse_file = fuse_path / f 
  if not fuse_file.exists():
    print(f"{idx_str} --> '{f}'")
    req = requests.get(f'https://www2.census.gov/geo/tiger/TIGER_RD18/LAYER/ROADS/{f}')
    with open(fuse_file, 'wb') as f:
      f.write(req.content)
  else:
    print(f"{idx_str} --> '{f}' exists...skipping")

# COMMAND ----------

ls -l --block-size=M $HAZARD_RISK_FUSE/road/shapefile/tl_rd22_36001_roads.zip # look at 1 file size

# COMMAND ----------

display(dbutils.fs.ls(f"{os.environ['HAZARD_RISK_DBFS']}/road/shapefile"))

# COMMAND ----------

# MAGIC %md ## NY Bridges (Points | __3MB__)
# MAGIC
# MAGIC > From https://gis.ny.gov/gisdata/fileserver/?DSID=397&file=NYSDOTBridges.zip (FileGeoDB)

# COMMAND ----------

dbutils.fs.mkdirs(f"{os.environ['HAZARD_RISK_DBFS']}/bridge")

# COMMAND ----------

# MAGIC %sh 
# MAGIC rm -f ny_bridges.gdb.zip
# MAGIC wget -O ny_bridges.gdb.zip "https://gis.ny.gov/gisdata/fileserver/?DSID=397&file=NYSDOTBridges.zip"
# MAGIC cp ny_bridges.gdb.zip $HAZARD_RISK_FUSE/bridge
# MAGIC ls $HAZARD_RISK_FUSE/bridge

# COMMAND ----------

ls -l --block-size=M $HAZARD_RISK_FUSE/bridge/ny_bridges.gdb.zip

# COMMAND ----------

display(dbutils.fs.ls(f"{os.environ['HAZARD_RISK_DBFS']}/bridge"))

# COMMAND ----------

# MAGIC %md ## TOP LEVEL FOLDERS for `HAZARD_RISK`
# MAGIC
# MAGIC > List Post-Download

# COMMAND ----------

display(dbutils.fs.ls(os.environ['HAZARD_RISK_DBFS']))
