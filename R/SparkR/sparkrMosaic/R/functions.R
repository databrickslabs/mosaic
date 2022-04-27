#' @include generics.R

NULL
#' as_hex

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname as_hex
#' @exportMethod as_hex
#' @param  inGeom Column
setMethod(
              f = "as_hex"
              ,signature(
                   inGeom = 'Column'
                )
              ,function(inGeom) {
                  jc <- sparkR.callJMethod(functions, "as_hex", inGeom@jc )
                  column(jc)
                  }
              )




#' as_json

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname as_json
#' @exportMethod as_json
#' @param  inGeom Column
setMethod(
              f = "as_json"
              ,signature(
                   inGeom = 'Column'
                )
              ,function(inGeom) {
                  jc <- sparkR.callJMethod(functions, "as_json", inGeom@jc )
                  column(jc)
                  }
              )




#' st_point

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_point
#' @exportMethod st_point
#' @param  xVal Column
#' @param  yVal Column
setMethod(
              f = "st_point"
              ,signature(
                   xVal = 'Column'
                   ,yVal = 'Column'
                )
              ,function(xVal,yVal) {
                  jc <- sparkR.callJMethod(functions, "st_point", xVal@jc, yVal@jc )
                  column(jc)
                  }
              )




#' st_makeline

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_makeline
#' @exportMethod st_makeline
#' @param  points Column
setMethod(
              f = "st_makeline"
              ,signature(
                   points = 'Column'
                )
              ,function(points) {
                  jc <- sparkR.callJMethod(functions, "st_makeline", points@jc )
                  column(jc)
                  }
              )




#' st_makepolygon

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_makepolygon
#' @exportMethod st_makepolygon
#' @param  boundaryRing Column
setMethod(
              f = "st_makepolygon"
              ,signature(
                   boundaryRing = 'Column'
                )
              ,function(boundaryRing) {
                  jc <- sparkR.callJMethod(functions, "st_makepolygon", boundaryRing@jc )
                  column(jc)
                  }
              )




#' st_makepolygon

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_makepolygon
#' @exportMethod st_makepolygon
#' @param  boundaryRing Column
#' @param  holeRingArray Column
setMethod(
              f = "st_makepolygon"
              ,signature(
                   boundaryRing = 'Column'
                   ,holeRingArray = 'Column'
                )
              ,function(boundaryRing,holeRingArray) {
                  jc <- sparkR.callJMethod(functions, "st_makepolygon", boundaryRing@jc, holeRingArray@jc )
                  column(jc)
                  }
              )




#' flatten_polygons

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname flatten_polygons
#' @exportMethod flatten_polygons
#' @param  geom Column
setMethod(
              f = "flatten_polygons"
              ,signature(
                   geom = 'Column'
                )
              ,function(geom) {
                  jc <- sparkR.callJMethod(functions, "flatten_polygons", geom@jc )
                  column(jc)
                  }
              )




#' st_xmax

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_xmax
#' @exportMethod st_xmax
#' @param  geom Column
setMethod(
              f = "st_xmax"
              ,signature(
                   geom = 'Column'
                )
              ,function(geom) {
                  jc <- sparkR.callJMethod(functions, "st_xmax", geom@jc )
                  column(jc)
                  }
              )




#' st_xmin

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_xmin
#' @exportMethod st_xmin
#' @param  geom Column
setMethod(
              f = "st_xmin"
              ,signature(
                   geom = 'Column'
                )
              ,function(geom) {
                  jc <- sparkR.callJMethod(functions, "st_xmin", geom@jc )
                  column(jc)
                  }
              )




#' st_ymax

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_ymax
#' @exportMethod st_ymax
#' @param  geom Column
setMethod(
              f = "st_ymax"
              ,signature(
                   geom = 'Column'
                )
              ,function(geom) {
                  jc <- sparkR.callJMethod(functions, "st_ymax", geom@jc )
                  column(jc)
                  }
              )




#' st_ymin

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_ymin
#' @exportMethod st_ymin
#' @param  geom Column
setMethod(
              f = "st_ymin"
              ,signature(
                   geom = 'Column'
                )
              ,function(geom) {
                  jc <- sparkR.callJMethod(functions, "st_ymin", geom@jc )
                  column(jc)
                  }
              )




#' st_zmax

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_zmax
#' @exportMethod st_zmax
#' @param  geom Column
setMethod(
              f = "st_zmax"
              ,signature(
                   geom = 'Column'
                )
              ,function(geom) {
                  jc <- sparkR.callJMethod(functions, "st_zmax", geom@jc )
                  column(jc)
                  }
              )




#' st_zmin

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_zmin
#' @exportMethod st_zmin
#' @param  geom Column
setMethod(
              f = "st_zmin"
              ,signature(
                   geom = 'Column'
                )
              ,function(geom) {
                  jc <- sparkR.callJMethod(functions, "st_zmin", geom@jc )
                  column(jc)
                  }
              )




#' st_isvalid

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_isvalid
#' @exportMethod st_isvalid
#' @param  geom Column
setMethod(
              f = "st_isvalid"
              ,signature(
                   geom = 'Column'
                )
              ,function(geom) {
                  jc <- sparkR.callJMethod(functions, "st_isvalid", geom@jc )
                  column(jc)
                  }
              )




#' st_geometrytype

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_geometrytype
#' @exportMethod st_geometrytype
#' @param  geom Column
setMethod(
              f = "st_geometrytype"
              ,signature(
                   geom = 'Column'
                )
              ,function(geom) {
                  jc <- sparkR.callJMethod(functions, "st_geometrytype", geom@jc )
                  column(jc)
                  }
              )




#' st_area

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_area
#' @exportMethod st_area
#' @param  geom Column
setMethod(
              f = "st_area"
              ,signature(
                   geom = 'Column'
                )
              ,function(geom) {
                  jc <- sparkR.callJMethod(functions, "st_area", geom@jc )
                  column(jc)
                  }
              )




#' st_centroid2D

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_centroid2D
#' @exportMethod st_centroid2D
#' @param  geom Column
setMethod(
              f = "st_centroid2D"
              ,signature(
                   geom = 'Column'
                )
              ,function(geom) {
                  jc <- sparkR.callJMethod(functions, "st_centroid2D", geom@jc )
                  column(jc)
                  }
              )




#' st_centroid3D

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_centroid3D
#' @exportMethod st_centroid3D
#' @param  geom Column
setMethod(
              f = "st_centroid3D"
              ,signature(
                   geom = 'Column'
                )
              ,function(geom) {
                  jc <- sparkR.callJMethod(functions, "st_centroid3D", geom@jc )
                  column(jc)
                  }
              )




#' convert_to

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname convert_to
#' @exportMethod convert_to
#' @param  inGeom Column
#' @param  outDataType character
setMethod(
              f = "convert_to"
              ,signature(
                   inGeom = 'Column'
                   ,outDataType = 'character'
                )
              ,function(inGeom,outDataType) {
                  jc <- sparkR.callJMethod(functions, "convert_to", inGeom@jc, outDataType@jc )
                  column(jc)
                  }
              )




#' st_geomfromwkt

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_geomfromwkt
#' @exportMethod st_geomfromwkt
#' @param  inGeom Column
setMethod(
              f = "st_geomfromwkt"
              ,signature(
                   inGeom = 'Column'
                )
              ,function(inGeom) {
                  jc <- sparkR.callJMethod(functions, "st_geomfromwkt", inGeom@jc )
                  column(jc)
                  }
              )




#' st_geomfromwkb

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_geomfromwkb
#' @exportMethod st_geomfromwkb
#' @param  inGeom Column
setMethod(
              f = "st_geomfromwkb"
              ,signature(
                   inGeom = 'Column'
                )
              ,function(inGeom) {
                  jc <- sparkR.callJMethod(functions, "st_geomfromwkb", inGeom@jc )
                  column(jc)
                  }
              )




#' st_geomfromgeojson

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_geomfromgeojson
#' @exportMethod st_geomfromgeojson
#' @param  inGeom Column
setMethod(
              f = "st_geomfromgeojson"
              ,signature(
                   inGeom = 'Column'
                )
              ,function(inGeom) {
                  jc <- sparkR.callJMethod(functions, "st_geomfromgeojson", inGeom@jc )
                  column(jc)
                  }
              )




#' st_aswkt

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_aswkt
#' @exportMethod st_aswkt
#' @param  geom Column
setMethod(
              f = "st_aswkt"
              ,signature(
                   geom = 'Column'
                )
              ,function(geom) {
                  jc <- sparkR.callJMethod(functions, "st_aswkt", geom@jc )
                  column(jc)
                  }
              )




#' st_astext

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_astext
#' @exportMethod st_astext
#' @param  geom Column
setMethod(
              f = "st_astext"
              ,signature(
                   geom = 'Column'
                )
              ,function(geom) {
                  jc <- sparkR.callJMethod(functions, "st_astext", geom@jc )
                  column(jc)
                  }
              )




#' st_aswkb

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_aswkb
#' @exportMethod st_aswkb
#' @param  geom Column
setMethod(
              f = "st_aswkb"
              ,signature(
                   geom = 'Column'
                )
              ,function(geom) {
                  jc <- sparkR.callJMethod(functions, "st_aswkb", geom@jc )
                  column(jc)
                  }
              )




#' st_asbinary

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_asbinary
#' @exportMethod st_asbinary
#' @param  geom Column
setMethod(
              f = "st_asbinary"
              ,signature(
                   geom = 'Column'
                )
              ,function(geom) {
                  jc <- sparkR.callJMethod(functions, "st_asbinary", geom@jc )
                  column(jc)
                  }
              )




#' st_asgeojson

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_asgeojson
#' @exportMethod st_asgeojson
#' @param  geom Column
setMethod(
              f = "st_asgeojson"
              ,signature(
                   geom = 'Column'
                )
              ,function(geom) {
                  jc <- sparkR.callJMethod(functions, "st_asgeojson", geom@jc )
                  column(jc)
                  }
              )




#' st_dump

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_dump
#' @exportMethod st_dump
#' @param  geom Column
setMethod(
              f = "st_dump"
              ,signature(
                   geom = 'Column'
                )
              ,function(geom) {
                  jc <- sparkR.callJMethod(functions, "st_dump", geom@jc )
                  column(jc)
                  }
              )




#' st_length

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_length
#' @exportMethod st_length
#' @param  geom Column
setMethod(
              f = "st_length"
              ,signature(
                   geom = 'Column'
                )
              ,function(geom) {
                  jc <- sparkR.callJMethod(functions, "st_length", geom@jc )
                  column(jc)
                  }
              )




#' st_perimeter

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_perimeter
#' @exportMethod st_perimeter
#' @param  geom Column
setMethod(
              f = "st_perimeter"
              ,signature(
                   geom = 'Column'
                )
              ,function(geom) {
                  jc <- sparkR.callJMethod(functions, "st_perimeter", geom@jc )
                  column(jc)
                  }
              )




#' st_distance

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_distance
#' @exportMethod st_distance
#' @param  geom1 Column
#' @param  geom2 Column
setMethod(
              f = "st_distance"
              ,signature(
                   geom1 = 'Column'
                   ,geom2 = 'Column'
                )
              ,function(geom1,geom2) {
                  jc <- sparkR.callJMethod(functions, "st_distance", geom1@jc, geom2@jc )
                  column(jc)
                  }
              )




#' st_contains

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_contains
#' @exportMethod st_contains
#' @param  geom1 Column
#' @param  geom2 Column
setMethod(
              f = "st_contains"
              ,signature(
                   geom1 = 'Column'
                   ,geom2 = 'Column'
                )
              ,function(geom1,geom2) {
                  jc <- sparkR.callJMethod(functions, "st_contains", geom1@jc, geom2@jc )
                  column(jc)
                  }
              )




#' st_translate

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_translate
#' @exportMethod st_translate
#' @param  geom1 Column
#' @param  xd Column
#' @param  yd Column
setMethod(
              f = "st_translate"
              ,signature(
                   geom1 = 'Column'
                   ,xd = 'Column'
                   ,yd = 'Column'
                )
              ,function(geom1,xd,yd) {
                  jc <- sparkR.callJMethod(functions, "st_translate", geom1@jc, xd@jc, yd@jc )
                  column(jc)
                  }
              )




#' st_scale

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_scale
#' @exportMethod st_scale
#' @param  geom1 Column
#' @param  xd Column
#' @param  yd Column
setMethod(
              f = "st_scale"
              ,signature(
                   geom1 = 'Column'
                   ,xd = 'Column'
                   ,yd = 'Column'
                )
              ,function(geom1,xd,yd) {
                  jc <- sparkR.callJMethod(functions, "st_scale", geom1@jc, xd@jc, yd@jc )
                  column(jc)
                  }
              )




#' st_rotate

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_rotate
#' @exportMethod st_rotate
#' @param  geom1 Column
#' @param  td Column
setMethod(
              f = "st_rotate"
              ,signature(
                   geom1 = 'Column'
                   ,td = 'Column'
                )
              ,function(geom1,td) {
                  jc <- sparkR.callJMethod(functions, "st_rotate", geom1@jc, td@jc )
                  column(jc)
                  }
              )




#' st_convexhull

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_convexhull
#' @exportMethod st_convexhull
#' @param  geom Column
setMethod(
              f = "st_convexhull"
              ,signature(
                   geom = 'Column'
                )
              ,function(geom) {
                  jc <- sparkR.callJMethod(functions, "st_convexhull", geom@jc )
                  column(jc)
                  }
              )




#' st_numpoints

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_numpoints
#' @exportMethod st_numpoints
#' @param  geom Column
setMethod(
              f = "st_numpoints"
              ,signature(
                   geom = 'Column'
                )
              ,function(geom) {
                  jc <- sparkR.callJMethod(functions, "st_numpoints", geom@jc )
                  column(jc)
                  }
              )




#' st_intersects

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_intersects
#' @exportMethod st_intersects
#' @param  left Column
#' @param  right Column
setMethod(
              f = "st_intersects"
              ,signature(
                   left = 'Column'
                   ,right = 'Column'
                )
              ,function(left,right) {
                  jc <- sparkR.callJMethod(functions, "st_intersects", left@jc, right@jc )
                  column(jc)
                  }
              )




#' st_intersection

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_intersection
#' @exportMethod st_intersection
#' @param  left Column
#' @param  right Column
setMethod(
              f = "st_intersection"
              ,signature(
                   left = 'Column'
                   ,right = 'Column'
                )
              ,function(left,right) {
                  jc <- sparkR.callJMethod(functions, "st_intersection", left@jc, right@jc )
                  column(jc)
                  }
              )




#' st_srid

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_srid
#' @exportMethod st_srid
#' @param  geom Column
setMethod(
              f = "st_srid"
              ,signature(
                   geom = 'Column'
                )
              ,function(geom) {
                  jc <- sparkR.callJMethod(functions, "st_srid", geom@jc )
                  column(jc)
                  }
              )




#' st_setsrid

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_setsrid
#' @exportMethod st_setsrid
#' @param  geom Column
#' @param  srid Column
setMethod(
              f = "st_setsrid"
              ,signature(
                   geom = 'Column'
                   ,srid = 'Column'
                )
              ,function(geom,srid) {
                  jc <- sparkR.callJMethod(functions, "st_setsrid", geom@jc, srid@jc )
                  column(jc)
                  }
              )




#' st_transform

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_transform
#' @exportMethod st_transform
#' @param  geom Column
#' @param  srid Column
setMethod(
              f = "st_transform"
              ,signature(
                   geom = 'Column'
                   ,srid = 'Column'
                )
              ,function(geom,srid) {
                  jc <- sparkR.callJMethod(functions, "st_transform", geom@jc, srid@jc )
                  column(jc)
                  }
              )




#' st_intersects_aggregate

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_intersects_aggregate
#' @exportMethod st_intersects_aggregate
#' @param  leftIndex Column
#' @param  rightIndex Column
setMethod(
              f = "st_intersects_aggregate"
              ,signature(
                   leftIndex = 'Column'
                   ,rightIndex = 'Column'
                )
              ,function(leftIndex,rightIndex) {
                  jc <- sparkR.callJMethod(functions, "st_intersects_aggregate", leftIndex@jc, rightIndex@jc )
                  column(jc)
                  }
              )




#' st_intersection_aggregate

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname st_intersection_aggregate
#' @exportMethod st_intersection_aggregate
#' @param  leftIndex Column
#' @param  rightIndex Column
setMethod(
              f = "st_intersection_aggregate"
              ,signature(
                   leftIndex = 'Column'
                   ,rightIndex = 'Column'
                )
              ,function(leftIndex,rightIndex) {
                  jc <- sparkR.callJMethod(functions, "st_intersection_aggregate", leftIndex@jc, rightIndex@jc )
                  column(jc)
                  }
              )




#' mosaic_explode

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname mosaic_explode
#' @exportMethod mosaic_explode
#' @param  geom Column
#' @param  resolution Column
setMethod(
              f = "mosaic_explode"
              ,signature(
                   geom = 'Column'
                   ,resolution = 'Column'
                )
              ,function(geom,resolution) {
                  jc <- sparkR.callJMethod(functions, "mosaic_explode", geom@jc, resolution@jc )
                  column(jc)
                  }
              )




#' mosaic_explode

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname mosaic_explode
#' @exportMethod mosaic_explode
#' @param  geom Column
#' @param  resolution numeric
setMethod(
              f = "mosaic_explode"
              ,signature(
                   geom = 'Column'
                   ,resolution = 'numeric'
                )
              ,function(geom,resolution) {
                  jc <- sparkR.callJMethod(functions, "mosaic_explode", geom@jc, resolution@jc )
                  column(jc)
                  }
              )




#' mosaicfill

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname mosaicfill
#' @exportMethod mosaicfill
#' @param  geom Column
#' @param  resolution Column
setMethod(
              f = "mosaicfill"
              ,signature(
                   geom = 'Column'
                   ,resolution = 'Column'
                )
              ,function(geom,resolution) {
                  jc <- sparkR.callJMethod(functions, "mosaicfill", geom@jc, resolution@jc )
                  column(jc)
                  }
              )




#' mosaicfill

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname mosaicfill
#' @exportMethod mosaicfill
#' @param  geom Column
#' @param  resolution numeric
setMethod(
              f = "mosaicfill"
              ,signature(
                   geom = 'Column'
                   ,resolution = 'numeric'
                )
              ,function(geom,resolution) {
                  jc <- sparkR.callJMethod(functions, "mosaicfill", geom@jc, resolution@jc )
                  column(jc)
                  }
              )




#' point_index_geom

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname point_index_geom
#' @exportMethod point_index_geom
#' @param  point Column
#' @param  resolution Column
setMethod(
              f = "point_index_geom"
              ,signature(
                   point = 'Column'
                   ,resolution = 'Column'
                )
              ,function(point,resolution) {
                  jc <- sparkR.callJMethod(functions, "point_index_geom", point@jc, resolution@jc )
                  column(jc)
                  }
              )




#' point_index_geom

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname point_index_geom
#' @exportMethod point_index_geom
#' @param  point Column
#' @param  resolution numeric
setMethod(
              f = "point_index_geom"
              ,signature(
                   point = 'Column'
                   ,resolution = 'numeric'
                )
              ,function(point,resolution) {
                  jc <- sparkR.callJMethod(functions, "point_index_geom", point@jc, resolution@jc )
                  column(jc)
                  }
              )




#' point_index_lonlat

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname point_index_lonlat
#' @exportMethod point_index_lonlat
#' @param  lon Column
#' @param  lat Column
#' @param  resolution Column
setMethod(
              f = "point_index_lonlat"
              ,signature(
                   lon = 'Column'
                   ,lat = 'Column'
                   ,resolution = 'Column'
                )
              ,function(lon,lat,resolution) {
                  jc <- sparkR.callJMethod(functions, "point_index_lonlat", lon@jc, lat@jc, resolution@jc )
                  column(jc)
                  }
              )




#' point_index_lonlat

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname point_index_lonlat
#' @exportMethod point_index_lonlat
#' @param  lon Column
#' @param  lat Column
#' @param  resolution numeric
setMethod(
              f = "point_index_lonlat"
              ,signature(
                   lon = 'Column'
                   ,lat = 'Column'
                   ,resolution = 'numeric'
                )
              ,function(lon,lat,resolution) {
                  jc <- sparkR.callJMethod(functions, "point_index_lonlat", lon@jc, lat@jc, resolution@jc )
                  column(jc)
                  }
              )




#' polyfill

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname polyfill
#' @exportMethod polyfill
#' @param  geom Column
#' @param  resolution Column
setMethod(
              f = "polyfill"
              ,signature(
                   geom = 'Column'
                   ,resolution = 'Column'
                )
              ,function(geom,resolution) {
                  jc <- sparkR.callJMethod(functions, "polyfill", geom@jc, resolution@jc )
                  column(jc)
                  }
              )




#' polyfill

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname polyfill
#' @exportMethod polyfill
#' @param  geom Column
#' @param  resolution numeric
setMethod(
              f = "polyfill"
              ,signature(
                   geom = 'Column'
                   ,resolution = 'numeric'
                )
              ,function(geom,resolution) {
                  jc <- sparkR.callJMethod(functions, "polyfill", geom@jc, resolution@jc )
                  column(jc)
                  }
              )




#' index_geometry

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname index_geometry
#' @exportMethod index_geometry
#' @param  indexID Column
setMethod(
              f = "index_geometry"
              ,signature(
                   indexID = 'Column'
                )
              ,function(indexID) {
                  jc <- sparkR.callJMethod(functions, "index_geometry", indexID@jc )
                  column(jc)
                  }
              )




#' try_sql

#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @rdname try_sql
#' @exportMethod try_sql
#' @param  inCol Column
setMethod(
              f = "try_sql"
              ,signature(
                   inCol = 'Column'
                )
              ,function(inCol) {
                  jc <- sparkR.callJMethod(functions, "try_sql", inCol@jc )
                  column(jc)
                  }
              )




