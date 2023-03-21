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