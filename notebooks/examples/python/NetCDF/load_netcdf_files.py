# Databricks notebook source
# MAGIC %md
# MAGIC # Opening and visualizing a netCDF file (Python)

# COMMAND ----------

## Overview

This notebook demonstrates how to open and explore the netCDF file, visualize the data, and export to a comma-separated file (CSV). This tutorial is intended for the users with novice-level programming skills. However, it is expected that the users familiarize themselves with key aspects of [netCDF climate and forecast (CF) metadata convention](http://cfconventions.org/cf-conventions/v1.6.0/cf-conventions.html) before starting the tutorials.

![Volumetric soil moisture at various soil depths](https://raw.githubusercontent.com/ornldaac/netcdf_open_visualize_csv/master/resources/py-nc-visualize.png)

## Source Data

The source data is a netCDF file ([soil_moist_20min_Kendall_AZ_n1400.nc](https://daac.ornl.gov/daacdata/eos_land_val/SoilSCAPE/data//soil_moist_20min_Kendall_AZ_n1400.nc)) consisting of  volumetric root zone soil moisture data from a location in Kendall, Arizona, USA. This data was collected as a part of SoilSCAPE (Soil moisture Sensing Controller and oPtimal Estimator) project (https://daac.ornl.gov/cgi-bin/dsviewer.pl?ds_id=1339)

## Prerequisites

Python 3 or later. Python modules: netCDF4, numpy, pandas, matplotlib

## Tutorial
In this tutorial, we will open and explore the netCDF file, visualize the data, and export to a comma-separated file (CSV). 

### 1. Import python modules
First import the required modules:

# COMMAND ----------

# MAGIC %matplotlib inline
# MAGIC # above generates plots in line within this page
# MAGIC 
# MAGIC import pandas as pd # pandas module
# MAGIC import numpy as np # numpy module
# MAGIC import netCDF4 as nc # netcdf module
# MAGIC import matplotlib.pyplot as plt # plot from matplotlib module

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Read and explore the netCDF file
# MAGIC Read in the netCDF file at folder /data/indata/ into 'in_nc'. Printing 'in_nc' displays important information about the data sets, such as *global attributes*, *data dimensions*, and *variable names*. Global attributes in a netCDF file contains information about the data such as data authors, publisher, data contacts, etc.

# COMMAND ----------

in_nc = nc.Dataset("dbfs:/ml/blogs/geospatial/data/netcdf/indata/soil_moist_20min_Kendall_AZ_n1400.nc") # read file
print(in_nc) # print file information

# COMMAND ----------

# MAGIC %md
# MAGIC In the above print output, we can get the variables names and dimension names/sizes. For example, "lat", "lon" variables with geographic coordinates, and "soil moisture" variable with the volumetric soil moisture data. Let us print the location:

# COMMAND ----------

y = in_nc.variables['lat'][:] # read latitutde variable
x = in_nc.variables['lon'][:] # read longitude variable
print("Latitude: %.5f, Longitude: %.5f" % (y,x)) # print latitutde, longitude

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Read in variables and check attributes
# MAGIC In this step we will read in variables we are interested in and print their attributes (e.g. units of measurements, detailed names etc).

# COMMAND ----------

soil_moisture = in_nc.variables['soil_moisture'][:] # read soil moisture variable
print(in_nc.variables['soil_moisture']) # print the variable attributes

# COMMAND ----------

depth = in_nc.variables['depth'][:] # read depth variable
print(in_nc.variables['depth']) # print the variable attributes

# COMMAND ----------

time = in_nc.variables['time'][:] # read time variable
print(in_nc.variables['time']) # print the variable attributes

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Convert time values
# MAGIC As you may have noticed, the time units in most netCDFs are relative to a fixed date (e.g. minutes since 2011-01-01 in this case). To convert it to corresponding meaningful date/time values, we will use 'num2date' command:

# COMMAND ----------

time_unit = in_nc.variables["time"].getncattr('units') # first read the 'units' attributes from the variable time
time_cal = in_nc.variables["time"].getncattr('calendar') # read calendar type
local_time = nc.num2date(time, units=time_unit, calendar=time_cal) # convert time
print("Original time %s is now converted as %s" % (time[0], local_time[0])) # check conversion

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Create daily average soil moisture plot
# MAGIC To create soil moisture plots aggregated by day, we will first put the data into a *pandas dataframe*, which let you organize data in a meaningful tabular data structure and does time aggregation easily.

# COMMAND ----------

sm_df = pd.DataFrame(soil_moisture, columns=depth, index=local_time.tolist()) # read into pandas dataframe
print(sm_df[:5]) # print the first 5 rows of dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC Now we will convert the original (~ half-hourly) data to daily using *Pandas's TimeGrouper"*. '1D' means daily, '6M' means six-monthly etc. More aliases are listed [here](http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases). Notice that we are using "numpy's nanmean" instead of "mean" to exclude all NaN values. Ignore any run time warning messages. 

# COMMAND ----------

sm_df_daily = sm_df.groupby(pd.TimeGrouper('1D')).aggregate(np.nanmean) # convert to daily.
print(sm_df_daily[:5]) # print the first 5 rows

# COMMAND ----------

# MAGIC %md
# MAGIC We will now create plot of daily time series of soil moisture measured at soil depths (5, 15 and 30cm) using python's matplotlib module:

# COMMAND ----------

ylabel_name = in_nc.variables["soil_moisture"].getncattr('long_name') + ' (' + \
              in_nc.variables["soil_moisture"].getncattr('units') + ')' # Label for y-axis
series_name = in_nc.variables["depth"].getncattr('long_name') + ' (' + \
              in_nc.variables["depth"].getncattr('units') + ')' # Legend title
# plot
plt.figure()
sm_df_daily.plot()
plt.legend(title=series_name)
plt.ylabel(ylabel_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Output to CSV
# MAGIC We can also convert pandas dataframes (both daily and original) to separate comma-separated-values(CSV) files, to be used for further analysis, etc.

# COMMAND ----------

sm_df_daily.to_csv("dbfs:/ml/blogs/geospatial/data/netcdf/outdata/daily_soilscape.csv", index_label="DateTime") # Daily
sm_df.to_csv("dbfs:/ml/blogs/geospatial/data/netcdf/outdata/original_soilscape.csv", index_label="DateTime") # Original
