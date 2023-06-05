=================
Raster functions
=================

Intro
################
Raster functions are available in mosaic if you have installed the optional dependency `GDAL`.
Please see :doc:`Install and Enable GDAL with Mosaic </usage/install-gdal>` for installation instructions.
Mosaic provides several unique raster functions that are not available in other Spark packages.
Mainly raster to grid functions, which are useful for reprojecting the raster data into a standard grid index system.
This is useful for performing spatial joins between raster data and vector data.
Mosaic also provides a scalable retiling function that can be used to retile raster data in case of bottlenecking due to large files.
All raster functions respect the \"rst\_\" prefix naming convention.

rst_bandmetadata
****************

.. function:: rst_bandmetadata(raster, band)

    Extract the metadata describing the raster band.
    Metadata is return as a map of key value pairs.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :param band: The band number to extract metadata for.
    :type band: Column (IntegerType)
    :rtype: Column: MapType(StringType, StringType)

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_bandmetadata("path", F.lit(1))).limit(1).display()
    +------------------------------------------------------------------------------------+
    |rst_bandmetadata(path, 1)                                                           |
    +------------------------------------------------------------------------------------+
    |{"_FillValue": "251", "NETCDF_DIM_time": "1294315200", "long_name": "bleaching alert|
    |area 7-day maximum composite", "grid_mapping": "crs", "NETCDF_VARNAME":             |
    |"bleaching_alert_area", "coverage_content_type": "thematicClassification",          |
    |"standard_name": "N/A", "comment": "Bleaching Alert Area (BAA) values are coral     |
    |bleaching heat stress levels: 0 - No Stress; 1 - Bleaching Watch; 2 - Bleaching     |
    |Warning; 3 - Bleaching Alert Level 1; 4 - Bleaching Alert Level 2. Product          |
    |description is provided at https://coralreefwatch.noaa.gov/product/5km/index.php.", |
    |"valid_min": "0", "units": "stress_level", "valid_max": "4", "scale_factor": "1"}   |
    +------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_bandmetadata(col("path"), lit(1)).limit(1).show(false)
    +------------------------------------------------------------------------------------+
    |rst_bandmetadata(path, 1)                                                           |
    +------------------------------------------------------------------------------------+
    |{"_FillValue": "251", "NETCDF_DIM_time": "1294315200", "long_name": "bleaching alert|
    |area 7-day maximum composite", "grid_mapping": "crs", "NETCDF_VARNAME":             |
    |"bleaching_alert_area", "coverage_content_type": "thematicClassification",          |
    |"standard_name": "N/A", "comment": "Bleaching Alert Area (BAA) values are coral     |
    |bleaching heat stress levels: 0 - No Stress; 1 - Bleaching Watch; 2 - Bleaching     |
    |Warning; 3 - Bleaching Alert Level 1; 4 - Bleaching Alert Level 2. Product          |
    |description is provided at https://coralreefwatch.noaa.gov/product/5km/index.php.", |
    |"valid_min": "0", "units": "stress_level", "valid_max": "4", "scale_factor": "1"}   |
    +------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_bandmetadata(path, 1) FROM coral_netcdf LIMIT 1
    +------------------------------------------------------------------------------------+
    |rst_bandmetadata(path, 1)                                                           |
    +------------------------------------------------------------------------------------+
    |{"_FillValue": "251", "NETCDF_DIM_time": "1294315200", "long_name": "bleaching alert|
    |area 7-day maximum composite", "grid_mapping": "crs", "NETCDF_VARNAME":             |
    |"bleaching_alert_area", "coverage_content_type": "thematicClassification",          |
    |"standard_name": "N/A", "comment": "Bleaching Alert Area (BAA) values are coral     |
    |bleaching heat stress levels: 0 - No Stress; 1 - Bleaching Watch; 2 - Bleaching     |
    |Warning; 3 - Bleaching Alert Level 1; 4 - Bleaching Alert Level 2. Product          |
    |description is provided at https://coralreefwatch.noaa.gov/product/5km/index.php.", |
    |"valid_min": "0", "units": "stress_level", "valid_max": "4", "scale_factor": "1"}   |
    +------------------------------------------------------------------------------------+

rst_georeference
***************

.. function:: rst_georeference(raster)

    Returns GeoTransform of the raster as a GT array of doubles.
    GT(0) x-coordinate of the upper-left corner of the upper-left pixel.
    GT(1) w-e pixel resolution / pixel width.
    GT(2) row rotation (typically zero).
    GT(3) y-coordinate of the upper-left corner of the upper-left pixel.
    GT(4) column rotation (typically zero).
    GT(5) n-s pixel resolution / pixel height (negative value for a north-up image).

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :rtype: Column: MapType(StringType, DoubleType)

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_georeference("path")).limit(1).display()
    +-------------------------------------------------------------------------------------------+
    |rst_georeference(path)                                                                     |
    +-------------------------------------------------------------------------------------------+
    |{"scaleY": -0.049999999152053956, "skewX": 0, "skewY": 0, "upperLeftY": 89.99999847369712, |
    |"upperLeftX": -180.00000610436345, "scaleX": 0.050000001695656514}                         |
    +-------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_georeference(col("path"))).limit(1).show()
    +-------------------------------------------------------------------------------------------+
    |rst_georeference(path)                                                                     |
    +-------------------------------------------------------------------------------------------+
    |{"scaleY": -0.049999999152053956, "skewX": 0, "skewY": 0, "upperLeftY": 89.99999847369712, |
    |"upperLeftX": -180.00000610436345, "scaleX": 0.050000001695656514}                         |
    +-------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_georeference(path) FROM coral_netcdf LIMIT 1
    +-------------------------------------------------------------------------------------------+
    |rst_georeference(path)                                                                     |
    +-------------------------------------------------------------------------------------------+
    |{"scaleY": -0.049999999152053956, "skewX": 0, "skewY": 0, "upperLeftY": 89.99999847369712, |
    |"upperLeftX": -180.00000610436345, "scaleX": 0.050000001695656514}                         |
    +-------------------------------------------------------------------------------------------+

rst_height
**********

.. function:: rst_height(raster)

    Returns the height of the raster in pixels.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :rtype: Column: IntegerType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_height('path')).show()
    +--------------------+
    | rst_height(path)   |
    +--------------------+
    |3600                |
    |3600                |
    +--------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_height(col("path"))).show()
    +--------------------+
    | rst_height(path)   |
    +--------------------+
    |3600                |
    |3600                |
    +--------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_height(path) FROM coral_netcdf
    +--------------------+
    | rst_height(path)   |
    +--------------------+
    |3600                |
    |3600                |
    +--------------------+

rst_isempty
*************

.. function:: rst_isempty(raster)

    Returns true if the raster is empty.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :rtype: Column: BooleanType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_isempty('path')).show()
    +--------------------+
    | rst_height(path)   |
    +--------------------+
    |false               |
    |false               |
    +--------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_isempty(col("path"))).show()
    +--------------------+
    | rst_height(path)   |
    +--------------------+
    |false               |
    |false               |
    +--------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_isempty(path) FROM coral_netcdf
    +--------------------+
    | rst_height(path)   |
    +--------------------+
    |false               |
    |false               |
    +--------------------+

rst_memsize
*************

.. function:: rst_memsize(raster)

    Returns size of the raster in bytes.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :rtype: Column: LongType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_memsize('path')).show()
    +--------------------+
    | rst_height(path)   |
    +--------------------+
    |730260              |
    |730260              |
    +--------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_memsize(col("path"))).show()
    +--------------------+
    | rst_height(path)   |
    +--------------------+
    |730260              |
    |730260              |
    +--------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_memsize(path) FROM coral_netcdf
    +--------------------+
    | rst_height(path)   |
    +--------------------+
    |730260              |
    |730260              |
    +--------------------+

rst_metadata
*************

.. function:: rst_metadata(raster)

    Extract the metadata describing the raster.
    Metadata is return as a map of key value pairs.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :rtype: Column: MapType(StringType, StringType)

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_metadata('path')).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_metadata(path)                                                                                               |
    +------------------------------------------------------------------------------------------------------------------+
    |{"NC_GLOBAL#publisher_url": "https://coralreefwatch.noaa.gov", "NC_GLOBAL#geospatial_lat_units": "degrees_north", |
    |"NC_GLOBAL#platform_vocabulary": "NOAA NODC Ocean Archive System Platforms", "NC_GLOBAL#creator_type": "group",   |
    |"NC_GLOBAL#geospatial_lon_units": "degrees_east", "NC_GLOBAL#geospatial_bounds": "POLYGON((-90.0 180.0, 90.0      |
    |180.0, 90.0 -180.0, -90.0 -180.0, -90.0 180.0))", "NC_GLOBAL#keywords": "Oceans > Ocean Temperature > Sea Surface |
    |Temperature, Oceans > Ocean Temperature > Water Temperature, Spectral/Engineering > Infrared Wavelengths > Thermal|
    |Infrared, Oceans > Ocean Temperature > Bleaching Alert Area", "NC_GLOBAL#geospatial_lat_max": "89.974998",        |
    |.... (truncated).... "NC_GLOBAL#history": "This is a product data file of the NOAA Coral Reef Watch Daily Global  |
    |5km Satellite Coral Bleaching Heat Stress Monitoring Product Suite Version 3.1 (v3.1) in its NetCDF Version 1.0   |
    |(v1.0).", "NC_GLOBAL#publisher_institution": "NOAA/NESDIS/STAR Coral Reef Watch Program",                         |
    |"NC_GLOBAL#cdm_data_type": "Grid"}                                                                                |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_metadata(col("path"))).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_metadata(path)                                                                                               |
    +------------------------------------------------------------------------------------------------------------------+
    |{"NC_GLOBAL#publisher_url": "https://coralreefwatch.noaa.gov", "NC_GLOBAL#geospatial_lat_units": "degrees_north", |
    |"NC_GLOBAL#platform_vocabulary": "NOAA NODC Ocean Archive System Platforms", "NC_GLOBAL#creator_type": "group",   |
    |"NC_GLOBAL#geospatial_lon_units": "degrees_east", "NC_GLOBAL#geospatial_bounds": "POLYGON((-90.0 180.0, 90.0      |
    |180.0, 90.0 -180.0, -90.0 -180.0, -90.0 180.0))", "NC_GLOBAL#keywords": "Oceans > Ocean Temperature > Sea Surface |
    |Temperature, Oceans > Ocean Temperature > Water Temperature, Spectral/Engineering > Infrared Wavelengths > Thermal|
    |Infrared, Oceans > Ocean Temperature > Bleaching Alert Area", "NC_GLOBAL#geospatial_lat_max": "89.974998",        |
    |.... (truncated).... "NC_GLOBAL#history": "This is a product data file of the NOAA Coral Reef Watch Daily Global  |
    |5km Satellite Coral Bleaching Heat Stress Monitoring Product Suite Version 3.1 (v3.1) in its NetCDF Version 1.0   |
    |(v1.0).", "NC_GLOBAL#publisher_institution": "NOAA/NESDIS/STAR Coral Reef Watch Program",                         |
    |"NC_GLOBAL#cdm_data_type": "Grid"}                                                                                |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_metadata(path) FROM coral_netcdf LIMIT 1
    +------------------------------------------------------------------------------------------------------------------+
    | rst_metadata(path)                                                                                               |
    +------------------------------------------------------------------------------------------------------------------+
    |{"NC_GLOBAL#publisher_url": "https://coralreefwatch.noaa.gov", "NC_GLOBAL#geospatial_lat_units": "degrees_north", |
    |"NC_GLOBAL#platform_vocabulary": "NOAA NODC Ocean Archive System Platforms", "NC_GLOBAL#creator_type": "group",   |
    |"NC_GLOBAL#geospatial_lon_units": "degrees_east", "NC_GLOBAL#geospatial_bounds": "POLYGON((-90.0 180.0, 90.0      |
    |180.0, 90.0 -180.0, -90.0 -180.0, -90.0 180.0))", "NC_GLOBAL#keywords": "Oceans > Ocean Temperature > Sea Surface |
    |Temperature, Oceans > Ocean Temperature > Water Temperature, Spectral/Engineering > Infrared Wavelengths > Thermal|
    |Infrared, Oceans > Ocean Temperature > Bleaching Alert Area", "NC_GLOBAL#geospatial_lat_max": "89.974998",        |
    |.... (truncated).... "NC_GLOBAL#history": "This is a product data file of the NOAA Coral Reef Watch Daily Global  |
    |5km Satellite Coral Bleaching Heat Stress Monitoring Product Suite Version 3.1 (v3.1) in its NetCDF Version 1.0   |
    |(v1.0).", "NC_GLOBAL#publisher_institution": "NOAA/NESDIS/STAR Coral Reef Watch Program",                         |
    |"NC_GLOBAL#cdm_data_type": "Grid"}                                                                                |
    +------------------------------------------------------------------------------------------------------------------+

rst_numbands
*************

.. function:: rst_numbands(raster)

    Returns number of bands in the raster.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :rtype: Column: IntegerType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_numbands('path')).show()
    +---------------------+
    | rst_numbands(path)  |
    +---------------------+
    | 1                   |
    | 1                   |
    +---------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_metadata(col("path"))).show()
    +---------------------+
    | rst_numbands(path)  |
    +---------------------+
    | 1                   |
    | 1                   |
    +---------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_metadata(path)
    +---------------------+
    | rst_numbands(path)  |
    +---------------------+
    | 1                   |
    | 1                   |
    +---------------------+

rst_pixelheight
***************

.. function:: rst_pixelheight(raster)

    Returns the height of the pixel in the raster derived via GeoTransform.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_pixelheight('path')).show()
    +---------------------+
    |rst_pixelheight(path)|
    +---------------------+
    | 1                   |
    | 1                   |
    +---------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_pixelheight(col("path"))).show()
    +---------------------+
    |rst_pixelheight(path)|
    +---------------------+
    | 1                   |
    | 1                   |
    +---------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_pixelheight(path)
    +---------------------+
    |rst_pixelheight(path)|
    +---------------------+
    | 1                   |
    | 1                   |
    +---------------------+

rst_pixelwidth
**************

.. function:: rst_pixelwidth(raster)

    Returns the width of the pixel in the raster derived via GeoTransform.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_pixelwidth('path')).show()
    +---------------------+
    | rst_pixelwidth(path)|
    +---------------------+
    | 1                   |
    | 1                   |
    +---------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_pixelwidth(col("path"))).show()
    +---------------------+
    | rst_pixelwidth(path)|
    +---------------------+
    | 1                   |
    | 1                   |
    +---------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_pixelwidth(path)
    +---------------------+
    | rst_pixelwidth(path)|
    +---------------------+
    | 1                   |
    | 1                   |
    +---------------------+

rst_rastertogridavg
*******************

.. function:: rst_rastertogridavg(raster, resolution)

    The result is a 2D array of cells, where each cell is a struct of (cellID, value).
    For getting the output of cellID->value pairs, please use explode() function twice.
    CellID can be LongType or StringType depending on the configuration of MosaicContext.
    The value/measure for each cell is the average of the pixel values in the cell.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :param raster: A resolution of the grid index system.
    :type col: Column (IntegerType)
    :rtype: Column: ArrayType(ArrayType(StructType(LongType|StringType, DoubleType)))

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_rastertogridavg('path', F.lit(3)).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridavg(path, 3)                                                                                     |
    +------------------------------------------------------------------------------------------------------------------+
    |[[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1.2037735849056603},|
    |{"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                   |
    |{"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1.1283185840707965},  |
    |{"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                   |
    |{"cellID": "593472602366803967", "measure": 0.3963963963963964},                                                  |
    |{"cellID": "593785619583336447", "measure": 0.6590909090909091}, {"cellID": "591988330388783103", "measure": 1},  |
    |{"cellID": "592336738135834623", "measure": 1}, ....]]                                                            |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_rastertogridavg(col("path"), lit(3)).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridavg(path, 3)                                                                                     |
    +------------------------------------------------------------------------------------------------------------------+
    |[[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1.2037735849056603},|
    |{"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                   |
    |{"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1.1283185840707965},  |
    |{"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                   |
    |{"cellID": "593472602366803967", "measure": 0.3963963963963964},                                                  |
    |{"cellID": "593785619583336447", "measure": 0.6590909090909091}, {"cellID": "591988330388783103", "measure": 1},  |
    |{"cellID": "592336738135834623", "measure": 1}, ....]]                                                            |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_rastertogridavg(path, 3)
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridavg(path, 3)                                                                                     |
    +------------------------------------------------------------------------------------------------------------------+
    |[[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1.2037735849056603},|
    |{"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                   |
    |{"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1.1283185840707965},  |
    |{"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                   |
    |{"cellID": "593472602366803967", "measure": 0.3963963963963964},                                                  |
    |{"cellID": "593785619583336447", "measure": 0.6590909090909091}, {"cellID": "591988330388783103", "measure": 1},  |
    |{"cellID": "592336738135834623", "measure": 1}, ....]]                                                            |
    +------------------------------------------------------------------------------------------------------------------+

.. figure:: ../images/rst_rastertogridavg/h3.png
   :figclass: doc-figure

   Fig 1. RST_RasterToGridAvg(raster, 3)

rst_rastertogridcount
*********************

.. function:: rst_rastertogridcount(raster, resolution)

    The result is a 2D array of cells, where each cell is a struct of (cellID, value).
    For getting the output of cellID->value pairs, please use explode() function twice.
    CellID can be LongType or StringType depending on the configuration of MosaicContext.
    The value/measure for each cell is the average of the pixel values in the cell.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :param raster: A resolution of the grid index system.
    :type col: Column (IntegerType)
    :rtype: Column: ArrayType(ArrayType(StructType(LongType|StringType, DoubleType)))

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_rastertogridcount('path', F.lit(3)).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridcount(path, 3)                                                                                   |
    +------------------------------------------------------------------------------------------------------------------+
    |[[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1},                 |
    |{"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                   |
    |{"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1},                   |
    |{"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                   |
    |{"cellID": "593472602366803967", "measure": 3},                                                                   |
    |{"cellID": "593785619583336447", "measure": 3}, {"cellID": "591988330388783103", "measure": 1},                   |
    |{"cellID": "592336738135834623", "measure": 1}, ....]]                                                            |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_rastertogridcount(col("path"), lit(3)).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridcount(path, 3)                                                                                   |
    +------------------------------------------------------------------------------------------------------------------+
    |[[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1},                 |
    |{"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                   |
    |{"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1},                   |
    |{"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                   |
    |{"cellID": "593472602366803967", "measure": 3},                                                                   |
    |{"cellID": "593785619583336447", "measure": 3}, {"cellID": "591988330388783103", "measure": 1},                   |
    |{"cellID": "592336738135834623", "measure": 1}, ....]]                                                            |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_rastertogridcount(path, 3)
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridcount(path, 3)                                                                                   |
    +------------------------------------------------------------------------------------------------------------------+
    |[[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1},                 |
    |{"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                   |
    |{"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1},                   |
    |{"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                   |
    |{"cellID": "593472602366803967", "measure": 3},                                                                   |
    |{"cellID": "593785619583336447", "measure": 3}, {"cellID": "591988330388783103", "measure": 1},                   |
    |{"cellID": "592336738135834623", "measure": 1}, ....]]                                                            |
    +------------------------------------------------------------------------------------------------------------------+

.. figure:: ../images/rst_rastertogridavg/h3.png
   :figclass: doc-figure

   Fig 2. RST_RasterToGridCount(raster, 3)

rst_rastertogridmax
*******************

.. function:: rst_rastertogridmax(raster, resolution)

    The result is a 2D array of cells, where each cell is a struct of (cellID, value).
    For getting the output of cellID->value pairs, please use explode() function twice.
    CellID can be LongType or StringType depending on the configuration of MosaicContext.
    The value/measure for each cell is the maximum pixel value.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :param raster: A resolution of the grid index system.
    :type col: Column (IntegerType)
    :rtype: Column: ArrayType(ArrayType(StructType(LongType|StringType, DoubleType)))

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_rastertogridmax('path', F.lit(3)).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridmax(path, 3)                                                                                     |
    +------------------------------------------------------------------------------------------------------------------+
    |[[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1.2037735849056603},|
    |{"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                   |
    |{"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1.1283185840707965},  |
    |{"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                   |
    |{"cellID": "593472602366803967", "measure": 0.3963963963963964},                                                  |
    |{"cellID": "593785619583336447", "measure": 0.6590909090909091}, {"cellID": "591988330388783103", "measure": 1},  |
    |{"cellID": "592336738135834623", "measure": 1}, ....]]                                                            |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_rastertogridmax(col("path"), lit(3)).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridmax(path, 3)                                                                                     |
    +------------------------------------------------------------------------------------------------------------------+
    |[[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1.2037735849056603},|
    |{"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                   |
    |{"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1.1283185840707965},  |
    |{"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                   |
    |{"cellID": "593472602366803967", "measure": 0.3963963963963964},                                                  |
    |{"cellID": "593785619583336447", "measure": 0.6590909090909091}, {"cellID": "591988330388783103", "measure": 1},  |
    |{"cellID": "592336738135834623", "measure": 1}, ....]]                                                            |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_rastertogridmax(path, 3)
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridmax(path, 3)                                                                                     |
    +------------------------------------------------------------------------------------------------------------------+
    |[[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1.2037735849056603},|
    |{"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                   |
    |{"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1.1283185840707965},  |
    |{"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                   |
    |{"cellID": "593472602366803967", "measure": 0.3963963963963964},                                                  |
    |{"cellID": "593785619583336447", "measure": 0.6590909090909091}, {"cellID": "591988330388783103", "measure": 1},  |
    |{"cellID": "592336738135834623", "measure": 1}, ....]]                                                            |
    +------------------------------------------------------------------------------------------------------------------+

.. figure:: ../images/rst_rastertogridavg/h3.png
   :figclass: doc-figure

   Fig 3. RST_RasterToGridMax(raster, 3)

rst_rastertogridmedian
**********************

.. function:: rst_rastertogridmedian(raster, resolution)

    The result is a 2D array of cells, where each cell is a struct of (cellID, value).
    For getting the output of cellID->value pairs, please use explode() function twice.
    CellID can be LongType or StringType depending on the configuration of MosaicContext.
    The value/measure for each cell is the median pixel value.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :param raster: A resolution of the grid index system.
    :type col: Column (IntegerType)
    :rtype: Column: ArrayType(ArrayType(StructType(LongType|StringType, DoubleType)))

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_rastertogridmedian('path', F.lit(3)).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridmedian(path, 3)                                                                                  |
    +------------------------------------------------------------------------------------------------------------------+
    |[[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1.2037735849056603},|
    |{"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                   |
    |{"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1.1283185840707965},  |
    |{"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                   |
    |{"cellID": "593472602366803967", "measure": 0.3963963963963964},                                                  |
    |{"cellID": "593785619583336447", "measure": 0.6590909090909091}, {"cellID": "591988330388783103", "measure": 1},  |
    |{"cellID": "592336738135834623", "measure": 1}, ....]]                                                            |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_rastertogridmedian(col("path"), lit(3)).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridmedian(path, 3)                                                                                  |
    +------------------------------------------------------------------------------------------------------------------+
    |[[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1.2037735849056603},|
    |{"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                   |
    |{"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1.1283185840707965},  |
    |{"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                   |
    |{"cellID": "593472602366803967", "measure": 0.3963963963963964},                                                  |
    |{"cellID": "593785619583336447", "measure": 0.6590909090909091}, {"cellID": "591988330388783103", "measure": 1},  |
    |{"cellID": "592336738135834623", "measure": 1}, ....]]                                                            |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_rastertogridmax(path, 3)
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridmedian(path, 3)                                                                                  |
    +------------------------------------------------------------------------------------------------------------------+
    |[[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1.2037735849056603},|
    |{"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                   |
    |{"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1.1283185840707965},  |
    |{"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                   |
    |{"cellID": "593472602366803967", "measure": 0.3963963963963964},                                                  |
    |{"cellID": "593785619583336447", "measure": 0.6590909090909091}, {"cellID": "591988330388783103", "measure": 1},  |
    |{"cellID": "592336738135834623", "measure": 1}, ....]]                                                            |
    +------------------------------------------------------------------------------------------------------------------+

.. figure:: ../images/rst_rastertogridavg/h3.png
   :figclass: doc-figure

   Fig 4. RST_RasterToGridMedian(raster, 3)

rst_rastertogridmin
*******************

.. function:: rst_rastertogridmin(raster, resolution)

    The result is a 2D array of cells, where each cell is a struct of (cellID, value).
    For getting the output of cellID->value pairs, please use explode() function twice.
    CellID can be LongType or StringType depending on the configuration of MosaicContext.
    The value/measure for each cell is the median pixel value.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :param raster: A resolution of the grid index system.
    :type col: Column (IntegerType)
    :rtype: Column: ArrayType(ArrayType(StructType(LongType|StringType, DoubleType)))

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_rastertogridmin('path', F.lit(3)).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridmin(path, 3)                                                                                     |
    +------------------------------------------------------------------------------------------------------------------+
    |[[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1.2037735849056603},|
    |{"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                   |
    |{"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1.1283185840707965},  |
    |{"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                   |
    |{"cellID": "593472602366803967", "measure": 0.3963963963963964},                                                  |
    |{"cellID": "593785619583336447", "measure": 0.6590909090909091}, {"cellID": "591988330388783103", "measure": 1},  |
    |{"cellID": "592336738135834623", "measure": 1}, ....]]                                                            |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_rastertogridmin(col("path"), lit(3)).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridmin(path, 3)                                                                                     |
    +------------------------------------------------------------------------------------------------------------------+
    |[[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1.2037735849056603},|
    |{"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                   |
    |{"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1.1283185840707965},  |
    |{"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                   |
    |{"cellID": "593472602366803967", "measure": 0.3963963963963964},                                                  |
    |{"cellID": "593785619583336447", "measure": 0.6590909090909091}, {"cellID": "591988330388783103", "measure": 1},  |
    |{"cellID": "592336738135834623", "measure": 1}, ....]]                                                            |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_rastertogridmin(path, 3)
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridmin(path, 3)                                                                                     |
    +------------------------------------------------------------------------------------------------------------------+
    |[[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1.2037735849056603},|
    |{"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                   |
    |{"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1.1283185840707965},  |
    |{"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                   |
    |{"cellID": "593472602366803967", "measure": 0.3963963963963964},                                                  |
    |{"cellID": "593785619583336447", "measure": 0.6590909090909091}, {"cellID": "591988330388783103", "measure": 1},  |
    |{"cellID": "592336738135834623", "measure": 1}, ....]]                                                            |
    +------------------------------------------------------------------------------------------------------------------+

.. figure:: ../images/rst_rastertogridavg/h3.png
   :figclass: doc-figure

   Fig 4. RST_RasterToGridMin(raster, 3)

rst_rastertoworldcoord
**********************

.. function:: rst_rastertoworldcoord(raster, x, y)

    Computes the world coordinates of the raster pixel at the given x and y coordinates.
    The result is a WKT point geometry.
    The coordinates are computed using the GeoTransform of the raster to respect the projection.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :param x: x coordinate of the pixel.
    :type col: Column (IntegerType)
    :param y: y coordinate of the pixel.
    :type col: Column (IntegerType)
    :rtype: Column: StringType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_rastertoworldcoord('path', F.lit(3), F.lit(3)).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertoworldcoord(path, 3, 3)                                                                               |
    +------------------------------------------------------------------------------------------------------------------+
    |POINT (-179.85000609927647 89.84999847624096)                                                                     |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_rastertoworldcoord(col("path"), lit(3), lit(3)).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertoworldcoord(path, 3, 3)                                                                               |
    +------------------------------------------------------------------------------------------------------------------+
    |POINT (-179.85000609927647 89.84999847624096)                                                                     |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_rastertoworldcoord(path, 3, 3)
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertoworldcoord(path, 3, 3)                                                                               |
    +------------------------------------------------------------------------------------------------------------------+
    |POINT (-179.85000609927647 89.84999847624096)                                                                     |
    +------------------------------------------------------------------------------------------------------------------+

rst_rastertoworldcoordx
**********************

.. function:: rst_rastertoworldcoord(raster, x, y)

    Computes the world coordinates of the raster pixel at the given x and y coordinates.
    The result is the X coordinate of the point after applying the GeoTransform of the raster.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :param x: x coordinate of the pixel.
    :type col: Column (IntegerType)
    :param y: y coordinate of the pixel.
    :type col: Column (IntegerType)
    :rtype: Column: StringType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_rastertoworldcoordx('path', F.lit(3), F.lit(3)).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertoworldcoordx(path, 3, 3)                                                                              |
    +------------------------------------------------------------------------------------------------------------------+
    | -179.85000609927647                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_rastertoworldcoordx(col("path"), lit(3), lit(3)).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertoworldcoordx(path, 3, 3)                                                                              |
    +------------------------------------------------------------------------------------------------------------------+
    | -179.85000609927647                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_rastertoworldcoordx(path, 3, 3)
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertoworldcoordx(path, 3, 3)                                                                              |
    +------------------------------------------------------------------------------------------------------------------+
    | -179.85000609927647                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

rst_rastertoworldcoordy
**********************

.. function:: rst_rastertoworldcoordy(raster, x, y)

    Computes the world coordinates of the raster pixel at the given x and y coordinates.
    The result is the X coordinate of the point after applying the GeoTransform of the raster.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :param x: x coordinate of the pixel.
    :type col: Column (IntegerType)
    :param y: y coordinate of the pixel.
    :type col: Column (IntegerType)
    :rtype: Column: StringType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_rastertoworldcoordy('path', F.lit(3), F.lit(3)).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertoworldcoordy(path, 3, 3)                                                                              |
    +------------------------------------------------------------------------------------------------------------------+
    | 89.84999847624096                                                                                                |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_rastertoworldcoordy(col("path"), lit(3), lit(3)).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertoworldcoordy(path, 3, 3)                                                                              |
    +------------------------------------------------------------------------------------------------------------------+
    | 89.84999847624096                                                                                                |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_rastertoworldcoordy(path, 3, 3)
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertoworldcoordy(path, 3, 3)                                                                              |
    +------------------------------------------------------------------------------------------------------------------+
    | 89.84999847624096                                                                                                |
    +------------------------------------------------------------------------------------------------------------------+

rst_retile
**********************

.. function:: rst_retile(raster, width, height)

    Retiles the raster to the given tile size. The result is a collection of new raster files.
    The new rasters are stored in the checkpoint directory.
    The results are the paths to the new rasters.
    The result set is automatically exploded.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :param width: The width of the tiles.
    :type col: Column (IntegerType)
    :param height: The height of the tiles.
    :type col: Column (IntegerType)
    :rtype: Column: StringType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_retile('path', F.lit(300), F.lit(300)).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_retile(path, 300, 300)                                                                                       |
    +------------------------------------------------------------------------------------------------------------------+
    | /dbfs/tmp/mosaic/raster/checkpoint/raster_1095576780709022500.tif                                                |
    | /dbfs/tmp/mosaic/raster/checkpoint/raster_-1042125519107460588.tif                                               |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_retile(col("path"), lit(300), lit(300)).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_retile(path, 300, 300)                                                                                       |
    +------------------------------------------------------------------------------------------------------------------+
    | /dbfs/tmp/mosaic/raster/checkpoint/raster_1095576780709022500.tif                                                |
    | /dbfs/tmp/mosaic/raster/checkpoint/raster_-1042125519107460588.tif                                               |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_retile(path, 300, 300)
    +------------------------------------------------------------------------------------------------------------------+
    | rst_retile(path, 300, 300)                                                                                       |
    +------------------------------------------------------------------------------------------------------------------+
    | /dbfs/tmp/mosaic/raster/checkpoint/raster_1095576780709022500.tif                                                |
    | /dbfs/tmp/mosaic/raster/checkpoint/raster_-1042125519107460588.tif                                               |
    +------------------------------------------------------------------------------------------------------------------+

rst_rotation
**********************

.. function:: rst_rotation(raster)

    Computes the rotation of the raster in degrees.
    The rotation is the angle between the X axis and the North axis.
    The rotation is computed using the GeoTransform of the raster.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_rotation('path').show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rotation(path)                                                                                               |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    | 21.2                                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_rotation(col("path")).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rotation(path)                                                                                               |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    | 21.2                                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_rotation(path)
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rotation(path)                                                                                               |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    | 21.2                                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+

rst_scalex
**********************

.. function:: rst_scalex(raster)

    Computes the scale of the raster in the X direction.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_scalex('path').show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_scalex(path)                                                                                                 |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_scalex(col("path")).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_scalex(path)                                                                                                 |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_scalex(path)
    +------------------------------------------------------------------------------------------------------------------+
    | rst_scalex(path)                                                                                                 |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

rst_scaley
**********************

.. function:: rst_scaley(raster)

    Computes the scale of the raster in the Y direction.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_scaley('path').show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_scaley(path)                                                                                                 |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_scaley(col("path")).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_scaley(path)                                                                                                 |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_scaley(path)
    +------------------------------------------------------------------------------------------------------------------+
    | rst_scaley(path)                                                                                                 |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

rst_skewx
**********************

.. function:: rst_skewx(raster)

    Computes the skew of the raster in the X direction.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_skewx('path').show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_skewx(path)                                                                                                  |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_skewx(col("path")).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_skewx(path)                                                                                                  |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_skewx(path)
    +------------------------------------------------------------------------------------------------------------------+
    | rst_skewx(path)                                                                                                  |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

rst_skewy
**********************

.. function:: rst_skewx(raster)

    Computes the skew of the raster in the Y direction.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_skewy('path').show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_skewy(path)                                                                                                  |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_skewy(col("path")).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_skewy(path)                                                                                                  |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_skewy(path)
    +------------------------------------------------------------------------------------------------------------------+
    | rst_skewy(path)                                                                                                  |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

rst_srid
**********************

.. function:: rst_srid(raster)

    Computes the SRID of the raster.
    The SRID is the EPSG code of the raster.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_srid('path').show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_srid(path)                                                                                                   |
    +------------------------------------------------------------------------------------------------------------------+
    | 9122                                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_srid(col("path")).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_srid(path)                                                                                                   |
    +------------------------------------------------------------------------------------------------------------------+
    | 9122                                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_srid(path)
    +------------------------------------------------------------------------------------------------------------------+
    | rst_srid(path)                                                                                                   |
    +------------------------------------------------------------------------------------------------------------------+
    | 9122                                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+

rst_subdatasets
**********************

.. function:: rst_subdatasets(raster)

    Computes the subdatasets of the raster.
    The subdatasets are the paths to the subdatasets of the raster.
    The result is a map of the subdataset path to the subdatasets and the description of the subdatasets.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :rtype: Column: MapType(StringType, StringType)

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_subdatasets('path').show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_subdatasets(path)                                                                                            |
    +------------------------------------------------------------------------------------------------------------------+
    | {"NETCDF:\"/dbfs/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa_max_7d_v3_1_2022010|
    |6-1.nc\":bleaching_alert_area": "[1x3600x7200] N/A (8-bit unsigned integer)", "NETCDF:\"/dbfs/FileStore/geospatial|
    |/mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa_max_7d_v3_1_20220106-1.nc\":mask": "[1x3600x7200] mask (8|
    |-bit unsigned integer)"}                                                                                          |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_subdatasets(col("path")).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_subdatasets(path)                                                                                            |
    +------------------------------------------------------------------------------------------------------------------+
    | {"NETCDF:\"/dbfs/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa_max_7d_v3_1_2022010|
    |6-1.nc\":bleaching_alert_area": "[1x3600x7200] N/A (8-bit unsigned integer)", "NETCDF:\"/dbfs/FileStore/geospatial|
    |/mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa_max_7d_v3_1_20220106-1.nc\":mask": "[1x3600x7200] mask (8|
    |-bit unsigned integer)"}                                                                                          |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_subdatasets(path)
    +------------------------------------------------------------------------------------------------------------------+
    | rst_subdatasets(path)                                                                                            |
    +------------------------------------------------------------------------------------------------------------------+
    | {"NETCDF:\"/dbfs/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa_max_7d_v3_1_2022010|
    |6-1.nc\":bleaching_alert_area": "[1x3600x7200] N/A (8-bit unsigned integer)", "NETCDF:\"/dbfs/FileStore/geospatial|
    |/mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa_max_7d_v3_1_20220106-1.nc\":mask": "[1x3600x7200] mask (8|
    |-bit unsigned integer)"}                                                                                          |
    +------------------------------------------------------------------------------------------------------------------+

rst_summary
**********************

.. function:: rst_summary(raster)

    Computes the summary of the raster.
    The summary is a map of the statistics of the raster.
    The logic is produced by gdalinfo procedure.
    The result is stored as JSON.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :rtype: Column: MapType(StringType, StringType)

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_summary('path').show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_summary(path)                                                                                                |
    +------------------------------------------------------------------------------------------------------------------+
    | {   "description":"/dbfs/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa_max_7d_v3_1|
    |_20220106-1.nc",   "driverShortName":"netCDF",   "driverLongName":"Network Common Data Format",   "files":[       |
    |"/dbfs/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa_max_7d_v3_1_20220106-1.nc"    |
    |],   "size":[     512,     512   ],   "metadata":{     "":{       "NC_GLOBAL#acknowledgement":"NOAA Coral Reef    |
    |Watch Program",       "NC_GLOBAL#cdm_data_type":"Gr...                                                            |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_summary(col("path")).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_summary(path)                                                                                                |
    +------------------------------------------------------------------------------------------------------------------+
    | {   "description":"/dbfs/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa_max_7d_v3_1|
    |_20220106-1.nc",   "driverShortName":"netCDF",   "driverLongName":"Network Common Data Format",   "files":[       |
    |"/dbfs/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa_max_7d_v3_1_20220106-1.nc"    |
    |],   "size":[     512,     512   ],   "metadata":{     "":{       "NC_GLOBAL#acknowledgement":"NOAA Coral Reef    |
    |Watch Program",       "NC_GLOBAL#cdm_data_type":"Gr...                                                            |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_summary(path)
    +------------------------------------------------------------------------------------------------------------------+
    | rst_summary(path)                                                                                                |
    +------------------------------------------------------------------------------------------------------------------+
    | {   "description":"/dbfs/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa_max_7d_v3_1|
    |_20220106-1.nc",   "driverShortName":"netCDF",   "driverLongName":"Network Common Data Format",   "files":[       |
    |"/dbfs/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa_max_7d_v3_1_20220106-1.nc"    |
    |],   "size":[     512,     512   ],   "metadata":{     "":{       "NC_GLOBAL#acknowledgement":"NOAA Coral Reef    |
    |Watch Program",       "NC_GLOBAL#cdm_data_type":"Gr...                                                            |
    +------------------------------------------------------------------------------------------------------------------+

rst_upperleftx
**********************

.. function:: rst_upperleftx(raster)

    Computes the upper left X coordinate of the raster.
    The value is computed based on GeoTransform.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_upperleftx('path').show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_upperleftx(path)                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+
    | -180.00000610436345                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_upperleftx(col("path")).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_upperleftx(path)                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+
    | -180.00000610436345                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_upperleftx(path)
    +------------------------------------------------------------------------------------------------------------------+
    | rst_upperleftx(path)                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+
    | -180.00000610436345                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

rst_upperlefty
**********************

.. function:: rst_upperlefty(raster)

    Computes the upper left Y coordinate of the raster.
    The value is computed based on GeoTransform.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_upperlefty('path').show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_upperlefty(path)                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+
    | 89.99999847369712                                                                                                |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_upperlefty(col("path")).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_upperlefty(path)                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+
    | 89.99999847369712                                                                                                |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_upperlefty(path)
    +------------------------------------------------------------------------------------------------------------------+
    | rst_upperlefty(path)                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+
    | 89.99999847369712                                                                                                |
    +------------------------------------------------------------------------------------------------------------------+

rst_width
**********************

.. function:: rst_width(raster)

    Computes the width of the raster in pixels.


    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :rtype: Column: IntegerType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_width('path').show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_width(path)                                                                                                  |
    +------------------------------------------------------------------------------------------------------------------+
    | 600                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_width(col("path")).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_width(path)                                                                                                  |
    +------------------------------------------------------------------------------------------------------------------+
    | 600                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_width(path)
    +------------------------------------------------------------------------------------------------------------------+
    | rst_width(path)                                                                                                  |
    +------------------------------------------------------------------------------------------------------------------+
    | 600                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

rst_worldtorastercoord
**********************

.. function:: rst_worldtorastercoord(raster, xworld, yworld)

    Computes the raster coordinates of the world coordinates.
    The raster coordinates are the pixel coordinates of the raster.
    The world coordinates are the coordinates in the CRS of the raster.
    The coordinates are resolved using GeoTransform.

    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :param x: X world coordinate.
    :type col: Column (StringType)
    :param y: Y world coordinate.
    :type col: Column (StringType)
    :rtype: Column: StructType(IntegerType, IntegerType)

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_worldtorastercoord('path', F.lit(-160.1), F.lit(40.0)).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_worldtorastercoord(path)                                                                                     |
    +------------------------------------------------------------------------------------------------------------------+
    | {"x": 398, "y": 997}                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_worldtorastercoord(col("path"), lit(-160.1), lit(40.0)).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_worldtorastercoord(path)                                                                                     |
    +------------------------------------------------------------------------------------------------------------------+
    | {"x": 398, "y": 997}                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_worldtorastercoord(path, -160.1, 40.0)
    +------------------------------------------------------------------------------------------------------------------+
    | rst_worldtorastercoord(path)                                                                                     |
    +------------------------------------------------------------------------------------------------------------------+
    | {"x": 398, "y": 997}                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+

rst_worldtorastercoordx
***********************

.. function:: rst_worldtorastercoordx(raster, xworld, yworld)

    Computes the raster coordinates of the world coordinates.
    The raster coordinates are the pixel coordinates of the raster.
    The world coordinates are the coordinates in the CRS of the raster.
    The coordinates are resolved using GeoTransform.
    This method returns the X coordinate.


    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :param x: X world coordinate.
    :type col: Column (StringType)
    :param y: Y world coordinate.
    :type col: Column (StringType)
    :rtype: Column: IntegerType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_worldtorastercoord('path', F.lit(-160.1), F.lit(40.0)).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_worldtorastercoordx(path, -160.1, 40.0)                                                                      |
    +------------------------------------------------------------------------------------------------------------------+
    | 398                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_worldtorastercoordx(col("path"), lit(-160.1), lit(40.0)).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_worldtorastercoordx(path, -160.1, 40.0)                                                                      |
    +------------------------------------------------------------------------------------------------------------------+
    | 398                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_worldtorastercoordx(path, -160.1, 40.0)
    +------------------------------------------------------------------------------------------------------------------+
    | rst_worldtorastercoordx(path, -160.1, 40.0)                                                                      |
    +------------------------------------------------------------------------------------------------------------------+
    | 398                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

rst_worldtorastercoordy
***********************

.. function:: rst_worldtorastercoordy(raster, xworld, yworld)

    Computes the raster coordinates of the world coordinates.
    The raster coordinates are the pixel coordinates of the raster.
    The world coordinates are the coordinates in the CRS of the raster.
    The coordinates are resolved using GeoTransform.
    This method returns the Y coordinate.


    :param raster: A column containing the path to a raster file.
    :type col: Column (StringType)
    :param x: X world coordinate.
    :type col: Column (StringType)
    :param y: Y world coordinate.
    :type col: Column (StringType)
    :rtype: Column: IntegerType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.nc")\
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(mos.rst_worldtorastercoordy('path', F.lit(-160.1), F.lit(40.0)).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_worldtorastercoordy(path, -160.1, 40.0)                                                                      |
    +------------------------------------------------------------------------------------------------------------------+
    | 997                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = spark.read
        .format("binaryFile").option("pathGlobFilter", "*.nc")
        .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    df.select(rst_worldtorastercoordy(col("path"), lit(-160.1), lit(40.0)).show()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_worldtorastercoordy(path, -160.1, 40.0)                                                                      |
    +------------------------------------------------------------------------------------------------------------------+
    | 997                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
        USING binaryFile
        OPTIONS (pathGlobFilter "*.nc", path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
    SELECT rst_worldtorastercoordy(path, -160.1, 40.0)
    +------------------------------------------------------------------------------------------------------------------+
    | rst_worldtorastercoordy(path, -160.1, 40.0)                                                                      |
    +------------------------------------------------------------------------------------------------------------------+
    | 997                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+
