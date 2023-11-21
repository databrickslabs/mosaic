# Databricks notebook source
# MAGIC %md # Single Node: Opening + visualizing a netCDF file (Python)
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC > This notebook demonstrates how to open and explore the netCDF file, visualize the data, and convert it to Pandas as well as Spark DataFrames. This is for users just getting familiar with [netCDF climate and forecast (CF) metadata conventions](http://cfconventions.org/cf-conventions/v1.6.0/cf-conventions.html). __The example is mostly single node / non-distributed__; until the spark call at the end, it runs only on the cluster driver, which is something like running on a "laptop in the sky". 
# MAGIC
# MAGIC ## Source Data
# MAGIC
# MAGIC The source data is a netCDF file, you can swap out for any data you are working with and follow the same basic pattern.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC Python 3 or later. Python modules: we will add 'netCDF4', 'xarray', 'nc-time-axis', and 'cartopy' (numpy, pandas, matplotlib already available)
# MAGIC
# MAGIC ---
# MAGIC __Last Update:__ 07 NOV 2023 [Mosaic 0.3.12]

# COMMAND ----------

# MAGIC %md ### 1. Import python modules
# MAGIC First import the required modules:

# COMMAND ----------

# MAGIC %pip install netCDF4 xarray nc-time-axis cartopy

# COMMAND ----------

import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import netCDF4 as nc
import numpy as np
import pandas as pd
import xarray as xr

# COMMAND ----------

# MAGIC %md ### 2. Read and explore the netCDF file
# MAGIC
# MAGIC > Example of reading in the netCDF file -- this is [Community Climate
# MAGIC  System Model project](https://www.cesm.ucar.edu/models/ccsm). Printing `in_nc` displays important information about the data sets, such as *global attributes*, *data dimensions*, and *variable names*. Global attributes in a netCDF file contains information about the data such as data authors, publisher, data contacts, etc.
# MAGIC
# MAGIC __DataArray__
# MAGIC
# MAGIC xarray.DataArray is an implementation of a labelled, multi-dimensional array for a single variable, such as precipitation, temperature etc. It has the following key properties:
# MAGIC
# MAGIC <p/>
# MAGIC
# MAGIC * `values`: a numpy.ndarray holding the array’s values
# MAGIC * `dims`: dimension names for each axis (e.g., ('lat', 'lon', 'z', 'time'))
# MAGIC * `coords`: a dict-like container of arrays (coordinates) that label each point (e.g., 1-dim arrays of numbers, * DateTime objects, or strings)
# MAGIC * `attrs`: an OrderedDict to hold arbitrary metadata (attributes)

# COMMAND ----------

# MAGIC %sh 
# MAGIC # - again, this is single node
# MAGIC # - can just download to the driver and start working with it
# MAGIC wget https://www.unidata.ucar.edu/software/netcdf/examples/sresa1b_ncar_ccsm3-example.nc
# MAGIC ls -lh

# COMMAND ----------

ds = xr.open_dataset("sresa1b_ncar_ccsm3-example.nc")
print(ds)

# COMMAND ----------

ds.tas

# COMMAND ----------

# MAGIC %md ### 3. Plot a Variable
# MAGIC
# MAGIC > In this case air temperature ('tas') in Kelvins.

# COMMAND ----------

# - Example-1: a simple plot
ax = plt.axes(projection=ccrs.PlateCarree()) # equidistance
ax.coastlines() 
ds.tas.plot()

# COMMAND ----------

 # Example-2
 # - pick the center
 # - handle data and plot projection
 p = ds.tas.plot(
    transform=ccrs.PlateCarree(),                  # data's projection (equidistance)
    col="time",                                    # time
    col_wrap=1,                                    # multiplot settings
    aspect=ds.dims["lon"] / ds.dims["lat"],        # for a sensible figsize
    subplot_kws={                                  # plot projection (conic)
      "projection": 
      ccrs.LambertConformal(
        central_longitude=-95, central_latitude=45
      )},
)

# set options
for ax in p.axs.flat:
    ax.coastlines()
    ax.set_extent([-160, -30, 5, 75])

# COMMAND ----------

# Example-3: Kelvin to Celsius
airc = ds.tas - 273.15  

# Figure
# - can easily adjust this for multipe subplots
f, ax1 = plt.subplots(1, 1, figsize=(12, 9), sharey=True)

# Selected latitude indices
# - there are 128 lats, so 64 is around equator
isel_lats = [32, 64, 96]

# Temperature vs longitude plot 
# - lons are 0..360
# - we are filtering to just 1 day (in this case that's all there is)
airc.isel(time=0, lat=isel_lats).plot.line(ax=ax1, hue="lat")
ax1.set_ylabel("°C")

plt.tight_layout()

# COMMAND ----------

# convert air temp from kelvins to fahrenheit (for fun)
airf = (ds.tas * 1.8) - 459.67
airf[0][45][0] # <- here is a reading at time[0], lat[45], lon[0]

# COMMAND ----------

# MAGIC %md ### 4. Output to Pandas and Spark DataFrames
# MAGIC
# MAGIC > We see that each variable and coordinate in the Dataset is now a column in the DataFrame, with the exception of indexes which are in the index. To convert the DataFrame to any other convenient representation, use DataFrame methods like `reset_index()`, `stack()` and `unstack()`.
# MAGIC
# MAGIC _Note: We can save the Xarray Dataset to various formats, just focusing on Pandas for brevity._

# COMMAND ----------

# MAGIC %md __First let's convert time to DateTime Index (from CFTimeIndex)__

# COMMAND ----------

from datetime import datetime

ds_times = ds.indexes['time'].to_datetimeindex()
print(ds_times)

ds['time'] = ds_times

# COMMAND ----------

pdf = ds.to_dataframe().reset_index() # <- pandas
pdf

# COMMAND ----------

pdf.dtypes

# COMMAND ----------

# MAGIC %md __Can convert to a Spark DataFrame.__
# MAGIC
# MAGIC > Note: we are dropping "*_bnds" and "msk_rgn" columns for simplicity.

# COMMAND ----------

df = (
  spark
    .createDataFrame(
      pdf.drop(columns=["bnds", "time_bnds", "lat_bnds", "lon_bnds", "msk_rgn"])
    )
    #.distinct() # <- not needed
)
print(f"count? {df.count():,}")
df.display()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col

# COMMAND ----------

# You can visualize this plot in Databricks
# - hint: press the '+' button below
display(
  df
    .groupBy(F.expr("ceil(lat) as lat_degree"))
    .agg(
      F.mean(col("plev")).alias("plev_avg"),
      F.mean(col("pr")).alias("pr_avg"),
      F.mean(col("tas")).alias("tas_avg"),
      F.mean(col("ua")).alias("ua_avg")
    )
)

# COMMAND ----------

# MAGIC %md ### 5. Databricks Lakehouse can read / write most any data format
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
