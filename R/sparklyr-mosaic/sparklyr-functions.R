#' @rdname as_hex
as_hex <- function(sc){
  sparklyr::invoke(functions, "as_hex", spark_session(sc))
}
#' @rdname as_json
as_json <- function(sc){
  sparklyr::invoke(functions, "as_json", spark_session(sc))
}
#' @rdname st_point
st_point <- function(sc){
  sparklyr::invoke(functions, "st_point", spark_session(sc))
}
#' @rdname st_makeline
st_makeline <- function(sc){
  sparklyr::invoke(functions, "st_makeline", spark_session(sc))
}
#' @rdname st_makepolygon
st_makepolygon <- function(sc){
  sparklyr::invoke(functions, "st_makepolygon", spark_session(sc))
}
#' @rdname st_makepolygon
st_makepolygon <- function(sc){
  sparklyr::invoke(functions, "st_makepolygon", spark_session(sc))
}
#' @rdname flatten_polygons
flatten_polygons <- function(sc){
  sparklyr::invoke(functions, "flatten_polygons", spark_session(sc))
}
#' @rdname st_xmax
st_xmax <- function(sc){
  sparklyr::invoke(functions, "st_xmax", spark_session(sc))
}
#' @rdname st_xmin
st_xmin <- function(sc){
  sparklyr::invoke(functions, "st_xmin", spark_session(sc))
}
#' @rdname st_ymax
st_ymax <- function(sc){
  sparklyr::invoke(functions, "st_ymax", spark_session(sc))
}
#' @rdname st_ymin
st_ymin <- function(sc){
  sparklyr::invoke(functions, "st_ymin", spark_session(sc))
}
#' @rdname st_zmax
st_zmax <- function(sc){
  sparklyr::invoke(functions, "st_zmax", spark_session(sc))
}
#' @rdname st_zmin
st_zmin <- function(sc){
  sparklyr::invoke(functions, "st_zmin", spark_session(sc))
}
#' @rdname st_isvalid
st_isvalid <- function(sc){
  sparklyr::invoke(functions, "st_isvalid", spark_session(sc))
}
#' @rdname st_geometrytype
st_geometrytype <- function(sc){
  sparklyr::invoke(functions, "st_geometrytype", spark_session(sc))
}
#' @rdname st_area
st_area <- function(sc){
  sparklyr::invoke(functions, "st_area", spark_session(sc))
}
#' @rdname st_centroid2D
st_centroid2D <- function(sc){
  sparklyr::invoke(functions, "st_centroid2D", spark_session(sc))
}
#' @rdname st_centroid3D
st_centroid3D <- function(sc){
  sparklyr::invoke(functions, "st_centroid3D", spark_session(sc))
}
#' @rdname convert_to
convert_to <- function(sc){
  sparklyr::invoke(functions, "convert_to", spark_session(sc))
}
#' @rdname st_geomfromwkt
st_geomfromwkt <- function(sc){
  sparklyr::invoke(functions, "st_geomfromwkt", spark_session(sc))
}
#' @rdname st_geomfromwkb
st_geomfromwkb <- function(sc){
  sparklyr::invoke(functions, "st_geomfromwkb", spark_session(sc))
}
#' @rdname st_geomfromgeojson
st_geomfromgeojson <- function(sc){
  sparklyr::invoke(functions, "st_geomfromgeojson", spark_session(sc))
}
#' @rdname st_aswkt
st_aswkt <- function(sc){
  sparklyr::invoke(functions, "st_aswkt", spark_session(sc))
}
#' @rdname st_astext
st_astext <- function(sc){
  sparklyr::invoke(functions, "st_astext", spark_session(sc))
}
#' @rdname st_aswkb
st_aswkb <- function(sc){
  sparklyr::invoke(functions, "st_aswkb", spark_session(sc))
}
#' @rdname st_asbinary
st_asbinary <- function(sc){
  sparklyr::invoke(functions, "st_asbinary", spark_session(sc))
}
#' @rdname st_asgeojson
st_asgeojson <- function(sc){
  sparklyr::invoke(functions, "st_asgeojson", spark_session(sc))
}
#' @rdname st_dump
st_dump <- function(sc){
  sparklyr::invoke(functions, "st_dump", spark_session(sc))
}
#' @rdname st_length
st_length <- function(sc){
  sparklyr::invoke(functions, "st_length", spark_session(sc))
}
#' @rdname st_perimeter
st_perimeter <- function(sc){
  sparklyr::invoke(functions, "st_perimeter", spark_session(sc))
}
#' @rdname st_distance
st_distance <- function(sc){
  sparklyr::invoke(functions, "st_distance", spark_session(sc))
}
#' @rdname st_contains
st_contains <- function(sc){
  sparklyr::invoke(functions, "st_contains", spark_session(sc))
}
#' @rdname st_translate
st_translate <- function(sc){
  sparklyr::invoke(functions, "st_translate", spark_session(sc))
}
#' @rdname st_scale
st_scale <- function(sc){
  sparklyr::invoke(functions, "st_scale", spark_session(sc))
}
#' @rdname st_rotate
st_rotate <- function(sc){
  sparklyr::invoke(functions, "st_rotate", spark_session(sc))
}
#' @rdname st_convexhull
st_convexhull <- function(sc){
  sparklyr::invoke(functions, "st_convexhull", spark_session(sc))
}
#' @rdname st_buffer
st_buffer <- function(sc){
  sparklyr::invoke(functions, "st_buffer", spark_session(sc))
}
#' @rdname st_buffer
st_buffer <- function(sc){
  sparklyr::invoke(functions, "st_buffer", spark_session(sc))
}
#' @rdname st_numpoints
st_numpoints <- function(sc){
  sparklyr::invoke(functions, "st_numpoints", spark_session(sc))
}
#' @rdname st_intersects
st_intersects <- function(sc){
  sparklyr::invoke(functions, "st_intersects", spark_session(sc))
}
#' @rdname st_intersection
st_intersection <- function(sc){
  sparklyr::invoke(functions, "st_intersection", spark_session(sc))
}
#' @rdname st_srid
st_srid <- function(sc){
  sparklyr::invoke(functions, "st_srid", spark_session(sc))
}
#' @rdname st_setsrid
st_setsrid <- function(sc){
  sparklyr::invoke(functions, "st_setsrid", spark_session(sc))
}
#' @rdname st_hasvalidcoordinates
st_hasvalidcoordinates <- function(sc){
  sparklyr::invoke(functions, "st_hasvalidcoordinates", spark_session(sc))
}
#' @rdname st_transform
st_transform <- function(sc){
  sparklyr::invoke(functions, "st_transform", spark_session(sc))
}
#' @rdname st_intersects_aggregate
st_intersects_aggregate <- function(sc){
  sparklyr::invoke(functions, "st_intersects_aggregate", spark_session(sc))
}
#' @rdname st_intersection_aggregate
st_intersection_aggregate <- function(sc){
  sparklyr::invoke(functions, "st_intersection_aggregate", spark_session(sc))
}
#' @rdname mosaic_explode
mosaic_explode <- function(sc){
  sparklyr::invoke(functions, "mosaic_explode", spark_session(sc))
}
#' @rdname mosaic_explode
mosaic_explode <- function(sc){
  sparklyr::invoke(functions, "mosaic_explode", spark_session(sc))
}
#' @rdname mosaic_explode
mosaic_explode <- function(sc){
  sparklyr::invoke(functions, "mosaic_explode", spark_session(sc))
}
#' @rdname mosaic_explode
mosaic_explode <- function(sc){
  sparklyr::invoke(functions, "mosaic_explode", spark_session(sc))
}
#' @rdname mosaic_explode
mosaic_explode <- function(sc){
  sparklyr::invoke(functions, "mosaic_explode", spark_session(sc))
}
#' @rdname mosaic_explode
mosaic_explode <- function(sc){
  sparklyr::invoke(functions, "mosaic_explode", spark_session(sc))
}
#' @rdname mosaicfill
mosaicfill <- function(sc){
  sparklyr::invoke(functions, "mosaicfill", spark_session(sc))
}
#' @rdname mosaicfill
mosaicfill <- function(sc){
  sparklyr::invoke(functions, "mosaicfill", spark_session(sc))
}
#' @rdname mosaicfill
mosaicfill <- function(sc){
  sparklyr::invoke(functions, "mosaicfill", spark_session(sc))
}
#' @rdname mosaicfill
mosaicfill <- function(sc){
  sparklyr::invoke(functions, "mosaicfill", spark_session(sc))
}
#' @rdname mosaicfill
mosaicfill <- function(sc){
  sparklyr::invoke(functions, "mosaicfill", spark_session(sc))
}
#' @rdname mosaicfill
mosaicfill <- function(sc){
  sparklyr::invoke(functions, "mosaicfill", spark_session(sc))
}
#' @rdname point_index_geom
point_index_geom <- function(sc){
  sparklyr::invoke(functions, "point_index_geom", spark_session(sc))
}
#' @rdname point_index_geom
point_index_geom <- function(sc){
  sparklyr::invoke(functions, "point_index_geom", spark_session(sc))
}
#' @rdname point_index_lonlat
point_index_lonlat <- function(sc){
  sparklyr::invoke(functions, "point_index_lonlat", spark_session(sc))
}
#' @rdname point_index_lonlat
point_index_lonlat <- function(sc){
  sparklyr::invoke(functions, "point_index_lonlat", spark_session(sc))
}
#' @rdname polyfill
polyfill <- function(sc){
  sparklyr::invoke(functions, "polyfill", spark_session(sc))
}
#' @rdname polyfill
polyfill <- function(sc){
  sparklyr::invoke(functions, "polyfill", spark_session(sc))
}
#' @rdname index_geometry
index_geometry <- function(sc){
  sparklyr::invoke(functions, "index_geometry", spark_session(sc))
}
#' @rdname try_sql
try_sql <- function(sc){
  sparklyr::invoke(functions, "try_sql", spark_session(sc))
}
