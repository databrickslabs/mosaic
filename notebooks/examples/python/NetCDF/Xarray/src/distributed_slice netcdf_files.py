# Databricks notebook source
# MAGIC %md # Slice NetCDFs [Distributed]
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC > This notebook demonstrates how to open and explore the netCDF file, and slice it using Spark + Xarray. Data is sliced by time, latitude, and longitude attributes.
# MAGIC
# MAGIC __Examples:__
# MAGIC
# MAGIC <p/>
# MAGIC
# MAGIC * Single File: slice example with flattening
# MAGIC * Distributed: slice examples with / without flattening
# MAGIC
# MAGIC ## Source Data
# MAGIC
# MAGIC The source data is NOAA Global Precipitation [Data](https://downloads.psl.noaa.gov/Datasets/cpc_global_precip/); contains all years since 1979, each ~60MB.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC Python 3 or later. Python modules: we will add 'h5netcdf', 'xarray', and 'cftime'; also will update 'scipy' version (numpy, pandas, matplotlib already available)
# MAGIC
# MAGIC ---
# MAGIC __Last Updated:__ 21 NOV 2023 [Mosaic 0.3.12]

# COMMAND ----------

# MAGIC %md ## Setup

# COMMAND ----------

# MAGIC %md ### Imports

# COMMAND ----------

# MAGIC %pip install h5netcdf cftime xarray scipy --quiet
# MAGIC %pip install databricks-mosaic --quiet

# COMMAND ----------

# -- configure AQE for more compute heavy operations
#  - choose option-1 or option-2 below, essential for REPARTITION!
# spark.conf.set("spark.databricks.optimizer.adaptive.enabled", False) # <- option-1: turn off completely for full control
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False) # <- option-2: just tweak partition management

# -- import databricks + spark functions

from pyspark.databricks.sql import functions as dbf
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import *

# -- setup mosaic
import mosaic as mos

mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

# -- other imports
import io
import json
import os
import pandas as pd
import xarray as xr

# COMMAND ----------

# MAGIC %md ### Data
# MAGIC
# MAGIC > Adjust `nc_dir` to your preferred fuse path. _For simplicity, we are going to use DBFS, but this is all shifting with Unity Catalog [more [here](https://docs.databricks.com/en/dbfs/unity-catalog.html)]._ __Note: [DBFS](https://docs.databricks.com/en/dbfs/dbfs-root.html), [Workspace Files](https://docs.databricks.com/en/files/workspace.html), and [most recent] [Volumes](https://docs.databricks.com/en/data-governance/unity-catalog/index.html#volumes), are FUSE mounted to the cluster nodes, looking like a local path.__

# COMMAND ----------

nc_dir = '/home/mjohns@databricks.com/geospatial/netcdf-precip'
nc_dir_fuse = f'/dbfs{nc_dir}'
os.makedirs(nc_dir_fuse, exist_ok=True)

nc_sample_path = f'{nc_dir}/precip.2023.nc'
nc_sample_path_fuse = f'/dbfs{nc_sample_path}'

os.environ['NC_DIR_FUSE'] = nc_dir_fuse
os.environ['NC_SAMPLE_PATH_FUSE'] = nc_sample_path_fuse
os.environ['NC_SAMPLE_FILE'] = 'precip.2023.nc'

# COMMAND ----------

os.path.isfile('test.txt')

# COMMAND ----------

def download_url(url:str, out_path:str, debug_level:int = 0):
  """
  Download URL to out path
  """
  import os
  import requests

  if os.path.exists(out_path):
    debug_level > 0 and print(f"...skipping existing '{out_path}'")
  else:
    r = requests.get(url) # create HTTP response object
    with open(out_path,'wb') as f:
      f.write(r.content)

# COMMAND ----------

# single year sample
year = "2023"
download_url(
  f"https://downloads.psl.noaa.gov/Datasets/cpc_global_precip/precip.{year}.nc", 
  f"{nc_dir_fuse}/precip.{year}.nc", 
  debug_level=1
)

# COMMAND ----------

# - you can adjust the range to avoid so many files
# - reminder: range is not inclusive, so this is through 2022 as-is
for year in range(1979,2023):
  download_url(f"https://downloads.psl.noaa.gov/Datasets/cpc_global_precip/precip.{year}.nc", f"{nc_dir_fuse}/precip.{year}.nc")

# COMMAND ----------

# MAGIC %sh
# MAGIC # avoid list all files
# MAGIC ls -lh $NC_DIR_FUSE | head -5
# MAGIC echo "..."
# MAGIC ls -lh $NC_DIR_FUSE | tail -5

# COMMAND ----------

# MAGIC %md ### Helper Functions
# MAGIC
# MAGIC > These are used a couple of places in the examples, have UDF version of each.

# COMMAND ----------

def from_360(lon):
  """
  Standardize from 0:360 to -180:180 degrees.
  - NetCDF does 0:360 for longitude
  - See https://itecnote.com/tecnote/python-change-longitude-from-180-to-180-to-0-to-360/
  """
  return ((lon - 180) % 360) - 180

@udf(returnType=DoubleType())
def from_360_udf(lon):
  return from_360(lon)

# COMMAND ----------

def from_180(lon):
  """
  Standardize from -180:180 to 0:360 degrees.
  - NetCDF does 0:360 for longitude
  - See https://itecnote.com/tecnote/python-change-longitude-from-180-to-180-to-0-to-360/
  """
  return lon % 360

@udf(returnType=DoubleType())
def from_180_udf(lon):
  return from_180(lon)

# COMMAND ----------

# MAGIC %md ## Load Metadata [Spark]
# MAGIC
# MAGIC > We start with the Mosaic reader to load our NetCDFs, all 45 in this case. Loading results in various metadata about the NetCDFs.

# COMMAND ----------

df_mos = (
  spark
    .read.format("gdal")
      .option("driverName", "NetCDF")
    .load(nc_dir)
)
print(f"count? {df_mos.count():,}")
df_mos.orderBy(F.desc("path")).limit(5).display() # <- limiting display for ipynb output only

# COMMAND ----------

# MAGIC %md ## Slice Example-1: Single File [with Flattening]
# MAGIC
# MAGIC > Before we move to distributed with Xarray, let's consider what slicing a single file might look like.

# COMMAND ----------

# MAGIC %md _Read XArray Dataset_

# COMMAND ----------

xds = xr.open_dataset(nc_sample_path_fuse)
xds

# COMMAND ----------

# MAGIC %md _Slice Dataset_

# COMMAND ----------

xds.sel(time=slice('2023-01-01','2023-01-31'))

# COMMAND ----------

xds.sel(lat=slice(89.0,88.0), lon=slice(0.25,0.75))

# COMMAND ----------

# MAGIC %md _Convert to Pandas & Flatten_

# COMMAND ----------

if 'time' in xds.dims.keys() and not isinstance(xds.indexes['time'], pd.DatetimeIndex):
    xds['time'] = xds.indexes['time'].to_datetimeindex()
pdf = xds.to_dataframe() # <- this is the right move (get a multi-index)
print(f'rows? {pdf.shape[0]:,}, cols? {pdf.shape[1]}')
pdf.head()

# COMMAND ----------

pdf.dropna(inplace=True)
print(f'rows? {pdf.shape[0]:,}, cols? {pdf.shape[1]}')
pdf.head()

# COMMAND ----------

pdf_flat = pdf.reset_index()
print(f'rows? {pdf_flat.shape[0]:,}, cols? {pdf_flat.shape[1]}')
pdf_flat.head()

# COMMAND ----------

# MAGIC %md _Get the [spark] schema from the flattened [pandas] sample_
# MAGIC
# MAGIC > This will be used in our distributed execution (below).

# COMMAND ----------

df = spark.createDataFrame(pdf_flat)
print(f"count? {df.count():,}")
df.limit(5).display()

# COMMAND ----------

df.schema

# COMMAND ----------

# MAGIC %md ## Slice Example-2: Vectorized UDF [with Flattening]
# MAGIC
# MAGIC > Use `applyInPandas` UDF to work more directly with the netCDF [outside of Moasaic + GDAL]. __Note: Will enforce grouping by path.__

# COMMAND ----------

# idenfified earlier in sample
flat_schema = (
  StructType(
    [
      StructField('lon', DoubleType(), True), 
      StructField('lat', DoubleType(), True), 
      StructField('time', TimestampType(), True), 
      StructField('precip', FloatType(), True), 
    ]
  )
)
flat_schema

# COMMAND ----------

def slice_flatten_path(key, input_pdf: pd.DataFrame) -> pd.DataFrame:
  """
  slice the `path` column [optimal w/single path]:
   - based on provided time, lat, lon slices
   - Read with XArray using h5netcdf engine
   - Handles conversion to pandas
   - flattens out multi-dimensions
   - drops na values (much smaller)
  Returns pandas dataframe
  """
  import io
  import pandas as pd
  import xarray as xr 

  # -- iterate over pdf --
  #  - this may just be 1 path,
  #    depends on groupBy
  #  - to further optimize, consider enforcing 1 path
  #    and not doing the `pd.concat` call, just returning 
  pdf_arr = []
  for index, row in input_pdf.iterrows():
    path_fuse = row['path'].replace("dbfs:","/dbfs")
    xds = xr.open_dataset(path_fuse)

    xds_slice = xds
    if 'time_slice' in input_pdf:
      xds_slice = xds_slice.sel(time=slice(*row['time_slice']))
    if 'lat_slice' in input_pdf:
      xds_slice = xds_slice.sel(lat=slice(*row['lat_slice']))
    if 'lon_slice' in input_pdf:
      xds_slice = xds_slice.sel(lon=slice(*row['lon_slice']))
    
    if 'time' in xds_slice.dims.keys() and not isinstance(xds_slice.indexes['time'], pd.DatetimeIndex):
      xds_slice['time'] = xds_slice.indexes['time'].to_datetimeindex()
    pdf = xds_slice.to_dataframe() # <- handle drops in xdf for large files
    pdf.dropna(inplace=True)
    pdf_arr.append(pdf.reset_index())
  
  return pd.concat(pdf_arr)

# COMMAND ----------

from_180(-83.0) # <- becomes min

# COMMAND ----------

from_180(-80.9) # <- becomes max

# COMMAND ----------

spark.catalog.clearCache()                                                       # <- cache for dev, help avoid recomputes

df_path = (
  df_mos
    .repartition(df_mos.count(), "path")                                         # <- repartition is important!
  .withColumn(
    "time_slice", 
    F.array([F.lit(x) for x in ['2023-01-01', '2023-01-31']])
  )
  .withColumn(
    "lat_slice", 
    F.array([F.lit(x) for x in [28.6, 26.9]])                                     # <- max, min
  )
  .withColumn(
    "lon_slice", 
    F.array([F.lit(x) for x in [from_180(-83.0), from_180(-80.9)]])               # <- min, max ... convert to 360 
  )
  .groupBy("path")
    .applyInPandas(slice_flatten_path, schema=flat_schema)                        # <- applyInPandas UDF 
  .withColumn("year", F.year("time"))
  .withColumn("month", F.month("time"))
  .withColumn("day", F.dayofmonth("time"))
  .withColumn("geom_wkt", mos.st_astext(mos.st_point(from_360_udf("lon"), "lat"))) # <- convert to -180:180
  .cache()
)

print(f"count? {df_path.count():,}")
display(df_path.limit(5)) # <- limiting for ipynb only

# COMMAND ----------

# MAGIC %md _Render average precipitation through the years_
# MAGIC
# MAGIC > This is per collected location __[our slice in Florida]__. Note: `precip` units are in millimeters.

# COMMAND ----------

df_kepler = (
  df_path
    .groupBy("geom_wkt")
  .agg(F.avg("precip").alias("avg_precip"))
)

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_kepler "geom_wkt" "geometry" 1_000

# COMMAND ----------

# MAGIC %md ## Slice Example-3: Vecorized UDF [without Flatten]
# MAGIC
# MAGIC > Use `applyInPandas` UDF to work more directly with the netCDF [outside of Mosaic + GDAL]. This shows two variations on maintaining a nested structure within a Delta Table: [a] Store Slices as NetCDF binary and [b] Store slices as JSON. __Note: Will enforce grouping by path.__

# COMMAND ----------

# MAGIC %md ### Option-[a]: Return Slice as NetCDF

# COMMAND ----------

# MAGIC %md _This will be binary type in our UDF._

# COMMAND ----------

nc_slice = xds.sel(time=slice('2023-01-01','2023-01-31'), lat=slice(89.0,88.0), lon=slice(0.25,0.75)).to_netcdf()
# nc_slice # <- this is binary

# COMMAND ----------

nc_slice_schema = StructType([StructField('content', BinaryType(), True)])

# COMMAND ----------

def slice_path_nc(key, input_pdf: pd.DataFrame) -> pd.DataFrame:
  """
  slice the `path` column [optimal w/single path]:
   - based on provided time, lat, lon slices
   - Read with XArray using h5netcdf engine
   - maintains the sliced netcdf as binary
  Returns pandas dataframe
  """
  import io
  import pandas as pd
  import xarray as xr 

  # -- iterate over pdf --
  #  - this may just be 1 path,
  #    depends on groupBy
  #  - to further optimize, consider enforcing 1 path
  #    and not doing the `pd.concat` call, just returning 
  pdf_arr = []
  for index, row in input_pdf.iterrows():
    path_fuse = row['path'].replace("dbfs:","/dbfs")
    xds = xr.open_dataset(path_fuse)

    xds_slice = xds
    if 'time_slice' in input_pdf:
      xds_slice = xds_slice.sel(time=slice(*row['time_slice']))
    if 'lat_slice' in input_pdf:
      xds_slice = xds_slice.sel(lat=slice(*row['lat_slice']))
    if 'lon_slice' in input_pdf:
      xds_slice = xds_slice.sel(lon=slice(*row['lon_slice']))
    
    pdf_arr.append(
      pd.DataFrame([xds_slice.to_netcdf()], columns=['content'])
    )
  
  return pd.concat(pdf_arr)

# COMMAND ----------

spark.catalog.clearCache()                                        # <- cache for dev, help avoid recomputes

df_nc_slice = (
  df_mos
    .repartition(df_mos.count(), "path")                          # <- repartition is important!
  .withColumn(
    "time_slice", 
    F.array([F.lit(x) for x in ['2023-01-01', '2023-01-31']])
  )
  .withColumn(
    "lat_slice", 
    F.array([F.lit(x) for x in [28.6, 26.9]])                      # <- max, min
  )
  .withColumn(
    "lon_slice", 
    F.array([F.lit(x) for x in [from_180(-83.0), from_180(-80.9)]]) # <- min, max ... convert to 360 
  )
  .groupBy("path")
    .applyInPandas(slice_path_nc, schema=nc_slice_schema)          # <- applyInPandas UDF 
  .cache()
)

print(f"count? {df_nc_slice.count():,}")
df_nc_slice.limit(1).show() # <- limiting for ipynb only

# COMMAND ----------

# MAGIC %md _Example flattening from the slices_
# MAGIC
# MAGIC > Though not explicitely shown here, you could have written out your slices to Delta Lake and then later decided to work with them again as field data. __Note: the use of BytesIO to load straight from the Delta Lake field data: `xds = xr.open_dataset(io.BytesIO(row['content']))`!__  

# COMMAND ----------

def explode_content(key, input_pdf: pd.DataFrame) -> pd.DataFrame:
  """
  Explode the expected `contents` column:
   - Read with XArray using h5netcdf engine
   - Handles conversion to pandas
   - flattens out multi-dimensions
   - drops na values (much smaller)
  Returns pandas dataframe
  """
  import io
  import pandas as pd
  import xarray as xr

  # -- iterate over pdf --
  #  - this may just be 1 path,
  #    depends on groupBy
  #  - to further optimize, consider enforcing 1 path
  #    and not doing the `pd.concat` call, just returning 
  pdf_arr = []

  for index, row in input_pdf.iterrows():
    xds = xr.open_dataset(io.BytesIO(row['content']))
    if 'time' in xds.dims.keys() and not isinstance(xds.indexes['time'], pd.DatetimeIndex):
      xds['time'] = xds.indexes['time'].to_datetimeindex()
    pdf = xds.to_dataframe()
    pdf.dropna(inplace=True) # <- handle drops in xdf for large files
    pdf_arr.append(pdf.reset_index())
  
  return pd.concat(pdf_arr)

# COMMAND ----------

df_flat_slice = (
  df_nc_slice
    .groupBy("content")
      .applyInPandas(explode_content, schema=flat_schema)
    .withColumn("year", F.year("time"))
    .withColumn("month", F.month("time"))
    .withColumn("day", F.dayofmonth("time"))
    .withColumn("geom_wkt", mos.st_astext(mos.st_point(from_360_udf("lon"), "lat"))) # <- to -180:180
  .cache()
)

print(f"count? {df_flat_slice.count():,}")
display(df_flat_slice.limit(5)) # <- limiting for ipynb only

# COMMAND ----------

# MAGIC %md _Render average precipitation through the years_
# MAGIC
# MAGIC > This is per collected location __[AKA our "same" slice in Florida]__. Note: `precip` units are in millimeters.

# COMMAND ----------

df_flat_slice_kepler = (
  df_flat_slice
    .groupBy("geom_wkt")
  .agg(F.avg("precip").alias("avg_precip"))
)

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_flat_slice_kepler "geom_wkt" "geometry" 1_000

# COMMAND ----------

# MAGIC %md ### Option-[b]: Slice as JSON
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md #### Single File Example 
# MAGIC
# MAGIC > Just to understand before moving to distributed.

# COMMAND ----------

pdf_slice = (
  xds.sel(
    time=slice('2023-01-01','2023-01-31'),
    lat=slice(28.6, 26.9), 
    lon=slice(from_180(-83.0), from_180(-80.9))
  ).to_dataframe()
)
print(f'rows? {pdf_slice.shape[0]:,}, cols? {pdf_slice.shape[1]}')
pdf_slice.head()

# COMMAND ----------

data_out = pdf_slice.groupby(['lat','lon','time']).apply(lambda x : x.to_json(orient='split')).values.tolist()
json_data = [json.loads(x) for x in data_out]
json_data[:3]

# COMMAND ----------

# MAGIC %md _Get the [spark] schema from the flattened [pandas] sample_
# MAGIC
# MAGIC > This will be used in our distributed execution (below).

# COMMAND ----------

df_slice_json = spark.createDataFrame(pd.DataFrame([[json_data]], columns=['nc_json']))
df_slice_json.show()

# COMMAND ----------

df_slice_json.schema

# COMMAND ----------

# MAGIC %md #### Distributed Example

# COMMAND ----------

json_schema = (
  StructType([
    StructField(
      'nc_json', 
      ArrayType(
        StructType([
          StructField('columns', ArrayType(StringType(), True), True), 
          StructField('data', ArrayType(ArrayType(DoubleType(), True), True), True),
          StructField('index', ArrayType(ArrayType(DoubleType(), True), True), True)
        ]),
        True
      ),
      True
    )
  ])
)

# COMMAND ----------

def slice_path_json(key, input_pdf: pd.DataFrame) -> pd.DataFrame:
  """
  slice the `path` column [optimal w/single path]:
   - based on provided time, lat, lon slices
   - Read with XArray using h5netcdf engine
   - drops na values
   - returns slice as json
  Returns pandas dataframe
  """
  import io
  import json
  import pandas as pd
  import xarray as xr 

  # -- iterate over pdf --
  #  - this may just be 1 path,
  #    depends on groupBy
  #  - to further optimize, consider enforcing 1 path
  #    and not doing the `pd.concat` call, just returning 
  pdf_arr = []
  for index, row in input_pdf.iterrows():
    path_fuse = row['path'].replace("dbfs:","/dbfs")
    xds = xr.open_dataset(path_fuse)

    xds_slice = xds
    if 'time_slice' in input_pdf:
      xds_slice = xds_slice.sel(time=slice(*row['time_slice']))
    if 'lat_slice' in input_pdf:
      xds_slice = xds_slice.sel(lat=slice(*row['lat_slice']))
    if 'lon_slice' in input_pdf:
      xds_slice = xds_slice.sel(lon=slice(*row['lon_slice']))
    
    if 'time' in xds_slice.dims.keys() and not isinstance(xds_slice.indexes['time'], pd.DatetimeIndex):
      xds_slice['time'] = xds_slice.indexes['time'].to_datetimeindex()
    pdf = xds_slice.to_dataframe() # <- handle drops in xdf for large files
    pdf.dropna(inplace=True)

    pdf_arr.append(pdf.groupby(['lat','lon','time']).apply(lambda x : x.to_json(orient='split')))
  
  pdf_list = pd.concat(pdf_arr).values.tolist()
  json_data = [json.loads(x) for x in pdf_list]
  
  return pd.DataFrame([[json_data]], columns=['nc_json'])

# COMMAND ----------

spark.catalog.clearCache()                                           # <- cache for dev, help avoid recomputes

df_json_slice = (
  df_mos
  .repartition(df_mos.count(), "path")                               # <- repartition is important!
    .withColumn(
    "time_slice", 
    F.array([F.lit(x) for x in ['2023-01-01', '2023-01-31']])
    )
    .withColumn(
      "lat_slice", 
      F.array([F.lit(x) for x in [28.6, 26.9]])                       # <- max, min
    )
    .withColumn(
      "lon_slice", 
      F.array([F.lit(x) for x in [from_180(-83.0), from_180(-80.9)]]) # <- min, max ... convert to 360 
    )
  .groupBy("path")
    .applyInPandas(slice_path_json, schema=json_schema)               # <- applyInPandas UDF
  .filter(F.size("nc_json") > 0)
  .cache()
)

print(f"count? {df_json_slice.count():,}") # <- this is all consolidated into a single json
df_json_slice.show()                       # <- not display, too big (see just the one row with results)

# COMMAND ----------

# MAGIC %md _We can explode the (consolidated) json with Spark to have a more manageable structure._ 

# COMMAND ----------

df_explode_slice = (
  df_json_slice
    .select(F.explode("nc_json"))
    .select("col.*")
)
print(f"count? {df_explode_slice.count():,}")
df_explode_slice.show()

# COMMAND ----------

# MAGIC %md _We can further extract a measure from the exploded structure._
# MAGIC
# MAGIC > The following shows precipitation, but you could pick another column as needed. __Note: There is still some nesting (handled further below).__

# COMMAND ----------

precip_idx = 1 # <- could be multiple data columns

df_precip_slice = (
  df_explode_slice
    .select(
      F.element_at("data", precip_idx)[0].alias("measure"),
      F.element_at("index", precip_idx).alias("index"),
    )
)

print(f"count? {df_precip_slice.count():,}")
display(df_precip_slice.limit(5)) # <- limiting output for ipynb only

# COMMAND ----------

# MAGIC %md _Here is an example of fully flattening from the precipitation slice._
# MAGIC
# MAGIC
# MAGIC __Notes:__
# MAGIC
# MAGIC <p/>
# MAGIC
# MAGIC * This standardizes from 0:360 degrees to -180:180, see [here](https://pratiman-91.github.io/2020/08/01/NetCDF-to-GeoTIFF-using-Python.html) for pattern `(<lon> + 180) % 360 - 180`
# MAGIC * Also, adjust double value for timestamp by `/1000`

# COMMAND ----------

df_precip_slice_flat = (
  df_precip_slice
    .select(
      (F.element_at("index", 3) / 1000.0).cast("timestamp").cast("date").alias("time"),
      F.element_at("index", 1).alias("lat"),
      from_360_udf(F.element_at("index",2)).alias("lon"),
      "measure",
      F.element_at("index", 2).alias("lon_360"),
    )
    .select("*", mos.st_astext(mos.st_point("lon", "lat")).alias("geom_wkt"))
)
print(f"count? {df_precip_slice_flat.count():,}")
display(df_precip_slice_flat.limit(5)) # <- limiting for ipynb only

# COMMAND ----------

# MAGIC %md _Render average precipitation through the years_
# MAGIC
# MAGIC > This is per collected location __[AKA our "same" slice in Florida]__. Note: `precip` units are in millimeters.

# COMMAND ----------

df_precip_slice_kepler = (
  df_precip_slice_flat
    .groupBy("geom_wkt")
  .agg(F.avg("measure").alias("avg_precip"))
)

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_precip_slice_kepler "geom_wkt" "geometry" 1_000

# COMMAND ----------

# MAGIC %md #### Databricks Lakehouse can read / write most any data format
# MAGIC
# MAGIC > Here are [built-in](https://docs.databricks.com/en/external-data/index.html) formats as well as Mosaic [readers](https://databrickslabs.github.io/mosaic/api/api.html). __Note: best performance with Delta Lake format__, ref [Databricks](https://docs.databricks.com/en/delta/index.html) and [OSS](https://docs.delta.io/latest/index.html) docs for Delta Lake. Beyond built-in formats, Databricks is a platform on which you can install a wide variety of libraries, e.g. [1](https://docs.databricks.com/en/libraries/index.html#python-environment-management) | [2](https://docs.databricks.com/en/compute/compatibility.html) | [3](https://docs.databricks.com/en/init-scripts/index.html).
# MAGIC
# MAGIC Example of [reading](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.html?highlight=read#pyspark.sql.DataFrameReader) and [writing](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.html?highlight=pyspark%20sql%20dataframe%20writer#pyspark.sql.DataFrameWriter) a Spark DataFrame with Delta Lake format.
# MAGIC
# MAGIC ```
# MAGIC # - `write.format("delta")` is default in Databricks
# MAGIC # - can save to a specified path in the Lakehouse
# MAGIC # - can save as a table in the Databricks Metastore
# MAGIC df.write.save("<some_path>")
# MAGIC df.write.saveAsTable("<some_delta_table>")
# MAGIC ```
# MAGIC
# MAGIC Example of loading a Delta Lake Table as a Spark DataFrame.
# MAGIC
# MAGIC ```
# MAGIC # - `read.format("delta")` is default in Databricks
# MAGIC # - can load a specified path in the Lakehouse
# MAGIC # - can load a table in the Databricks Metastore
# MAGIC df.read.load("<some_path>")
# MAGIC df.table("<some_delta_table>")
# MAGIC ```
# MAGIC
# MAGIC More on [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html) in Databricks Lakehouse for Governing [Tables](https://docs.databricks.com/en/data-governance/unity-catalog/index.html#tables) and [Volumes](https://docs.databricks.com/en/data-governance/unity-catalog/index.html#volumes).
