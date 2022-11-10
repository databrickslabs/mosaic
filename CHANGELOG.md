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