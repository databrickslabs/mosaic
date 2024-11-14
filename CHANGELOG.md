## v0.4.3 [DBR 13.3 LTS]

This is the final mainline release of Mosaic. Future development will be focused on the planned spatial-utils library, which will be a successor to Mosaic and will include new features and improvements. The first release of spatial-utils is expected in the coming months.

We will continue to maintain Mosaic for the foreseeable future, including bug fixes and security updates. However, we recommend that users start transitioning to spatial-utils as soon as possible to take advantage of the new features and improvements that will be available in that library.

This release includes a number of enhancements and fixes, detailed below.

### Raster checkpointing functions
Fuse-based checkpointing for raster operations is disabled by default but can be enabled and managed through:
- spark configs `spark.databricks.labs.mosaic.raster.use.checkpoint` and `spark.databricks.labs.mosaic.raster.checkpoint`.
- python: `mos.enable_gdal(spark, with_checkpoint_path=path)`.
- scala: `MosaicGDAL.enableGDALWithCheckpoint(spark, path)`.
  
This feature is designed to improve performance and reduce memory usage for raster operations by writing intermediate data to a fuse directory. This is particularly useful for large rasters or when working with many rasters in a single operation. 

We plan further enhancements to this feature (including automatic cleanup of checkpoint locations) as part of the first release of spatial-utils.

### Enhancements and fixes to the raster processing APIs
  - Added `RST_Write`, a function that permits writing each raster 'tile' in a DataFrame to a specified location (e.g. fuse directory) using the appropriate GDAL driver and tile data / path. This is useful for formalizing the path when writing a Lakehouse table and allows removal of interim checkpointed data.
  - Python bindings added for `RST_Avg`, `RST_Max`, `RST_Median`, `RST_Min`, and `RST_PixelCount`.
  - `RST_PixelCount` now supports optional 'countNoData' and 'countMask' parameters (defaults are false, can now be true) to optionally get full pixel counts where mask is 0.0 and noData is what is configured in the tile.
  - `RST_Clip` now exposes the GDAL Warp option `CUTLINE_ALL_TOUCHED` which determines whether or not any given pixel is included whether the clipping geometry crosses the centre point of the pixel (false) or any part of the pixel (true). The default is true but this is now configurable.
  - Within clipping operations such as `RST_Clip` we now correctly set the CRS in the generated Shapefile Feature Layer used for clipping. This means that the CRS of the input geometry will be respected when clipping rasters.
  - Added two new functions for getting and upcasting the datatype of a raster band: `RST_Type` and `RST_UpdateType`. Use these for ensuring that the datatype of a raster is appropriate for the operations being performed, e.g. upcasting the types of integer-typed input rasters before performing raster algebra like NDVI calculations where the result needs to be a float.
  - Added `RST_AsFormat`, a function that translates rasters between formats e.g. from NetCDF to GeoTIFF.
  - The logic underpinning `RST_MemSize` (and related operations) has been updated to fall back to estimating based on the raster dimensions and data types of each band if the raster is held in-memory.
  - `RST_To_Overlapping_Tiles` is renamed `RST_ToOverlappingTiles`. The original expression remains but is marked as deprecated.
  - `RST_WorldToRasterCoordY` now returns the correct `y` value (was returning `x`)
  - Docs added for expression `RST_SetSRID`.
  - Docs updated for `RST_FromContent` to capture the optional 'driver' parameter.

### Dependency management
Updates to and pinning of Python language and dependency versions:
- Pyspark requirement removed from python setup.cfg as it is supplied by DBR
- Python version limited to "<3.11,>=3.10" for DBR
- iPython dependency limited to "<8.11,>=7.4.2" for both DBR and keplergl-jupyter
- numpy now limited to "<2.0,>=1.21.5" to match DBR minimum

### Surface mesh APIs
A set of experimental APIs for for creating and working with surface meshes (i.e. triangulated irregular networks) have been added to Mosaic. Users can now generate a conforming Delaunay triangulation over point data (optionally including 'break' lines as hard constraints), interpolate elevation over a regular grid and rasterize the results to produce terrain models.
  - `ST_Triangulate` performs a conforming Delaunay triangulation using a set of mass points and break lines. 
  - `ST_InterpolateElevation` computes the interpolated elevations of a grid of points.
  - `RST_DTMFromGeoms` burns the interpolated elevations into a raster.

### British National Grid
Two fixes have been made to the British National Grid indexing system:
- Corrected a typo in the grid letter array used to perform lookups.
- Updated the logic used for identifying quadrants when these are specified in a grid reference

### Documentation
A few updates to our documentation and examples library:
- An example walkthrough has been added for arbitrary GDAL Warp and Transform operations using a pyspark UDF (see the section "API Documentation / Rasterio + GDAL UDFs")
- The Python "Quickstart Notebook" has been updated to use the `MosaicAnalyzer` class (added after `MosaicFrame` was deprecated)


## v0.4.2 [DBR 13.3 LTS]
- Geopandas now fixed to "<0.14.4,>=0.14" due to conflict with minimum numpy version in geopandas 0.14.4.
- H3 python changed from "==3.7.0" to "<4.0,>=3.7" to pick up patches.
- Fixed an issue with fallback logic when deserializing subdatasets from a zip.
- Adjusted data used to speed up a long-running test.
- Streamlines setup_gdal and setup_fuse_install:
    - init script and resource copy logic fixed to repo "main" (.so) / "latest" (.jar).
    - added apt-get lock handling for native installs.
    - removed support for UbuntuGIS PPA as GDAL version no longer compatible with jammy default (3.4.x).

## v0.4.1 [DBR 13.3 LTS]
- Fixed python bindings for MosaicAnalyzer functions.
- Added tiller functions, ST_AsGeoJSONTile and ST_AsMVTTile, for creating GeoJSON and MVT tiles as aggregations of geometries.
- Added filter and convolve functions for raster data.
- Raster tile schema changed to be <tile:struct<index_id:bigint, tile:binary, metadata:map<string, string>>.
- Raster tile metadata will contain driver, parentPath and path.
- Raster tile metadata will contain warnings and errors in case of failures.
- All raster functions ensure rasters are TILED and not STRIPED when appropriate.
- GDAL cache memory has been decreased to 512MB to reduce memory usage and competition with Spark.
- Add RST_MakeTiles that allows for different raster creations.
- Rasters can now be passed as file pointers using checkpoint location.
- Added logic to handle zarr format for raster data.
- Added RST_SeparateBands to separate bands from a raster for NetCDF and Zarr formats.

## v0.4.0 [DBR 13.3 LTS]
- First release for DBR 13.3 LTS which is Ubuntu Jammy and Spark 3.4.1. Not backwards compatible, meaning it will not run on prior DBRs; requires either a Photon DBR or a ML Runtime (__Standard, non-Photon DBR no longer allowed__).
- New `setup_fuse_install` function to meet various requirements arising with Unity Catalog + Shared Access clusters; removed the scala equivalent function, making artifact setup and install python-first for scala and Spark SQL. 
- Removed OSS ESRI Geometry API for 0.4 series, JTS now the only vector provider.
- MosaicAnalyzer functions now accept Spark DataFrames instead of MosaicFrame, which has been removed.
- Docs for 0.3.x have been archived and linked from current docs; notebooks for 0.3.x have been separated from current notebooks.
- This release targets Assigned (vs Shared Access) clusters and offers python and scala language bindings; SQL expressions will not register in this release within Unity Catalog.

## v0.3.14 [DBR < 13]
- Fixes for Warning and Error messages on mosaic_enable call.
- Performance improvements for raster functions.
- Fix support for GDAL configuration via spark config (use 'spark.databricks.labs.mosaic.gdal.' prefix).

## v0.3.13
- R bindings generation fixed and improved.
- Remove usage of /vsimem/ drivers for GDAL due to memory leaks.
- Add support for MapAlgebra expressions via RST_MapAlgebra.
- Add support for custom combine python functions via RST_DerivedBand.
- Improve test coverage.
- Allow for GDAL configuration via spark config (use 'spark.databricks.labs.mosaic.gdal.' prefix).

## v0.3.12
- Make JTS default Geometry Provider
- Add raster tile functions.
- Expand the support for raster manipulation.
- Add abstractions for running distributed gdal_translate, gdalwarp, gdalcalc, etc.
- Add RST_BoundingBox, RST_Clip, RST_CombineAvg, RST_CombineAvgAgg, RST_FromBands, RST_FromFile, RST_GetNoData,
  RST_InitNoData, RST_Merge, RST_MergeAgg, RST_NDVI, RST_ReTile, RST_SetNoData, RST_Subdivide
- Add RST_Tessellate that generates H3 tiles from rasters.
- Add RST_ToOverlappingTiles that generates tiles with overlapping areas from rasters.
- Add GDAL generic format reader.

## v0.3.11
- Update the CONTRIBUTING.md to follow the standard process.
- Fix for issue 383: grid_pointascellid fails with a Java type error when run on an already instantiated point.
- Bump maven-assembly-plugin from 3.5.0 to 3.6.0.
- Fix the cluster side init script generation.
- Fixed photon check for DBR warnings.
- Bump maven-surefire-plugin from 3.0.0 to 3.1.0.
- Fix the bug described in issue 360: incomplete coverage from grid_geometrykring and grid_tessellate.
- Add default value for script location path to init script.

## v0.3.10
- Fixed k-ring logic for BNG grid close to the edge of the grid
- Fixed deprecated st_centroid2D expression
- Documentation improvements
- Fix handling null geometry fields for OGRFileFormat reader
- Added warning for future DBR environment support
- Added support for GeometryCollection
- Fix intersection operations with ESRI geometry APIs
- Fixed custom grid issues for grids not multiple of the root size resolution
- Fixed python binding for rst_georeference
- Fixed ESRI create polygon with correct path order with ESRI APIs
- Fixed automatic SQL registration with GDAL

## v0.3.9
- Fixed k-ring interpolation on raster data read

## v0.3.8
- Added readers for default GDAL raster drivers (https://gdal.org/drivers/raster/index.html)
  - TIFF
  - COG
  - NetCDF
  - ... And more 
- Added readers for default GDAL vector drivers  (https://gdal.org/drivers/vector/index.html)
  - Shapefiles
  - Geodatabase (File GDB)
  - ... And more
- Added custom grid index system for arbitrary CRS
- Added Spatial KNN example
- Refactored and simplified Mosaic expressions definition
- Documentation updates and improvements

## v0.3.7
- Fixed pip release publish script

## v0.3.6
- Added GDAL and 32 rst_* raster functions:
  - RST_BandMetaData
  - RST_GeoReference
  - RST_IsEmpty
  - RST_MemSize
  - RST_MetaData
  - RST_NumBands
  - RST_PixelWidth
  - RST_PixelHeight
  - RST_RasterToGridAvg
  - RST_RasterToGridMax
  - RST_RasterToGridMin
  - RST_RasterToGridMedian
  - RST_RasterToGridCount
  - RST_RasterToWorldCoord
  - RST_RasterToWorldCoordX
  - RST_RasterToWorldCoordY
  - RST_ReTile
  - RST_Rotation
  - RST_ScaleX
  - RST_ScaleY
  - RST_SkewX
  - RST_SkewY
  - RST_SRID
  - RST_Subdatasets
  - RST_Summary
  - RST_UpperLeftX
  - RST_UpperLeftY
  - RST_Width
  - RST_Height
  - RST_WorldToRasterCoord
  - RST_WorldToRasterCoordX
  - RST_WorldToRasterCoordY
- Fixed geometry creation from empty Seq
- Fixed landmarks_miid and candidates_miid column names parameter in KNN
- Improved docs

## v0.3.5
- Implemented KNN (K Nearest Neighbours) transformer
- Implemented grid cell functions `grid_cellkring`, `grid_cellkloop`, `grid_cellkringexplode`, and `grid_cellkloopexplode`
- Implemented geometry grid functions `grid_geometrykring`, `grid_geometrykloop`, `grid_geometrykringexplode`, and `grid_geometrykloopexplode`
- Implemented `st_envelope`, `st_difference`, and `st_bufferloop` geometry functions
- Fixed the function names for `convert_to` functions
- Fixed corner case of duplicate H3 indexes in grid tessellation
- Fixed corner case of possible missing H3 indexes crossing the icosahedron edges
- Improved documentation

## v0.3.4
- Implemented `st_simplify`
- Improved docs for R language bindings
- Improve the unit testing patterns
- Exported `to_json` on R bindings

## v0.3.3
- Implemented `st_union` and `st_union_agg`
- Fixed line tessellation traversal when the first point falls between two indexes 
- Added sparklyR bindings
- Added BNG grid notebook example

## v0.3.2
- Fixed bug in `mosaic_kepler` plot for H3

## v0.3.1
- Implemented `st_unaryunion` function
- Added BNG grid plotting to mosaic_kepler
- Added arbitrary CRS transformations to mosaic_kepler plotting
- Added documentation for BNG grid
- Bug fixes and improvements on the BNG grid implementation
- Typo fixes

## v0.3.0
- Integration with H3 functions from Databricks runtime 11.2
- Refactored grid functions to reflect the naming convention of H3 functions from Databricks runtime
  - `index_geometry` -> `grid_boundaryaswkb`
  - `point_index_lonlat` -> `grid_longlatascellid`
  - `polyfill` -> `grid_polyfill`
  - `point_index_geom` -> `grid_pointascellid`
  - `mosaicfill` -> `grid_tessellate`
  - `mosaic_explode` -> `grid_tessellateexplode`
- Added links to the solution accelerators
- Refactored build actions
- Updated BNG grid output cell ID as string
- Typos and style fixes
- Improved Kepler visualisation integration
- Updated docs 

## v0.2.1
- Added CodeQL scanner
- Added Ship-to-Ship transfer detection example
- Added Open Street Maps ingestion and processing example
- Fixed geoJSON conversion DataType
- Fixed SparkR mirror URL
- Fixed R dependencies
- Fixed comments in pom.xml
- Updated and polished Readme and example files
- Switched to `published` release type

## v0.2.0
- Support for British National Grid index system
- Improved documentation (installation instructions and coverage of functions)
- Added `st_hasvalidcoordinates` for checking coordinate validity before indexing
- Fixed bug in `st_dump`
- Added examples of using Mosaic with Sedona
- Added SparkR bindings to release artifacts and SparkR docs
- Automated SQL registration included in docs
- Automation for publishing to PyPI
- Fixed bug with KeplerGL (caching between cell refreshes)
- Corrected quickstart notebook to reference New York 'zones'
- Included documentation code example notebooks in `/docs`
- Added code coverage monitoring to project
- Updated JTS version to 1.19.0

## v0.1.1
- Enable notebook-scoped library installation via `%pip` magic.

## v0.1.0
- Add indexing support for h3
- Add Mosaic logic
- Move to spark 3.2
- Add documentation
- Add python bindings
- Add scalastyle template
- Add support for many st_ expressions
- Add support for Esri Geometries
- Add support of GeoJSON
- Add type checks in Catalyst
- Add Geometry validity expressions
- Create WKT, WKB and Hex conversion expressions
- Setup the project
- Define GitHub templates
