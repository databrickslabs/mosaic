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
