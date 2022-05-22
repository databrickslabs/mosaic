#' @rdname as_hex 
    setGeneric(
        name="as_hex"
            ,def=function(inGeom)  {standardGeneric("as_hex")}
              )
                  
#' @rdname as_json 
    setGeneric(
        name="as_json"
            ,def=function(inGeom)  {standardGeneric("as_json")}
              )
                  
#' @rdname st_point 
    setGeneric(
        name="st_point"
            ,def=function(xVal,yVal)  {standardGeneric("st_point")}
              )
                  
#' @rdname st_makeline 
    setGeneric(
        name="st_makeline"
            ,def=function(points)  {standardGeneric("st_makeline")}
              )
                  
#' @rdname st_makepolygon 
    setGeneric(
        name="st_makepolygon"
            ,def=function(boundaryRing)  {standardGeneric("st_makepolygon")}
              )
                  
#' @rdname st_makepolygon 
    setGeneric(
        name="st_makepolygon"
            ,def=function(boundaryRing,holeRingArray)  {standardGeneric("st_makepolygon")}
              )
                  
#' @rdname flatten_polygons 
    setGeneric(
        name="flatten_polygons"
            ,def=function(geom)  {standardGeneric("flatten_polygons")}
              )
                  
#' @rdname st_xmax 
    setGeneric(
        name="st_xmax"
            ,def=function(geom)  {standardGeneric("st_xmax")}
              )
                  
#' @rdname st_xmin 
    setGeneric(
        name="st_xmin"
            ,def=function(geom)  {standardGeneric("st_xmin")}
              )
                  
#' @rdname st_ymax 
    setGeneric(
        name="st_ymax"
            ,def=function(geom)  {standardGeneric("st_ymax")}
              )
                  
#' @rdname st_ymin 
    setGeneric(
        name="st_ymin"
            ,def=function(geom)  {standardGeneric("st_ymin")}
              )
                  
#' @rdname st_zmax 
    setGeneric(
        name="st_zmax"
            ,def=function(geom)  {standardGeneric("st_zmax")}
              )
                  
#' @rdname st_zmin 
    setGeneric(
        name="st_zmin"
            ,def=function(geom)  {standardGeneric("st_zmin")}
              )
                  
#' @rdname st_isvalid 
    setGeneric(
        name="st_isvalid"
            ,def=function(geom)  {standardGeneric("st_isvalid")}
              )
                  
#' @rdname st_geometrytype 
    setGeneric(
        name="st_geometrytype"
            ,def=function(geom)  {standardGeneric("st_geometrytype")}
              )
                  
#' @rdname st_area 
    setGeneric(
        name="st_area"
            ,def=function(geom)  {standardGeneric("st_area")}
              )
                  
#' @rdname st_centroid2D 
    setGeneric(
        name="st_centroid2D"
            ,def=function(geom)  {standardGeneric("st_centroid2D")}
              )
                  
#' @rdname st_centroid3D 
    setGeneric(
        name="st_centroid3D"
            ,def=function(geom)  {standardGeneric("st_centroid3D")}
              )
                  
#' @rdname convert_to 
    setGeneric(
        name="convert_to"
            ,def=function(inGeom,outDataType)  {standardGeneric("convert_to")}
              )
                  
#' @rdname st_geomfromwkt 
    setGeneric(
        name="st_geomfromwkt"
            ,def=function(inGeom)  {standardGeneric("st_geomfromwkt")}
              )
                  
#' @rdname st_geomfromwkb 
    setGeneric(
        name="st_geomfromwkb"
            ,def=function(inGeom)  {standardGeneric("st_geomfromwkb")}
              )
                  
#' @rdname st_geomfromgeojson 
    setGeneric(
        name="st_geomfromgeojson"
            ,def=function(inGeom)  {standardGeneric("st_geomfromgeojson")}
              )
                  
#' @rdname st_aswkt 
    setGeneric(
        name="st_aswkt"
            ,def=function(geom)  {standardGeneric("st_aswkt")}
              )
                  
#' @rdname st_astext 
    setGeneric(
        name="st_astext"
            ,def=function(geom)  {standardGeneric("st_astext")}
              )
                  
#' @rdname st_aswkb 
    setGeneric(
        name="st_aswkb"
            ,def=function(geom)  {standardGeneric("st_aswkb")}
              )
                  
#' @rdname st_asbinary 
    setGeneric(
        name="st_asbinary"
            ,def=function(geom)  {standardGeneric("st_asbinary")}
              )
                  
#' @rdname st_asgeojson 
    setGeneric(
        name="st_asgeojson"
            ,def=function(geom)  {standardGeneric("st_asgeojson")}
              )
                  
#' @rdname st_dump 
    setGeneric(
        name="st_dump"
            ,def=function(geom)  {standardGeneric("st_dump")}
              )
                  
#' @rdname st_length 
    setGeneric(
        name="st_length"
            ,def=function(geom)  {standardGeneric("st_length")}
              )
                  
#' @rdname st_perimeter 
    setGeneric(
        name="st_perimeter"
            ,def=function(geom)  {standardGeneric("st_perimeter")}
              )
                  
#' @rdname st_distance 
    setGeneric(
        name="st_distance"
            ,def=function(geom1,geom2)  {standardGeneric("st_distance")}
              )
                  
#' @rdname st_contains 
    setGeneric(
        name="st_contains"
            ,def=function(geom1,geom2)  {standardGeneric("st_contains")}
              )
                  
#' @rdname st_translate 
    setGeneric(
        name="st_translate"
            ,def=function(geom1,xd,yd)  {standardGeneric("st_translate")}
              )
                  
#' @rdname st_scale 
    setGeneric(
        name="st_scale"
            ,def=function(geom1,xd,yd)  {standardGeneric("st_scale")}
              )
                  
#' @rdname st_rotate 
    setGeneric(
        name="st_rotate"
            ,def=function(geom1,td)  {standardGeneric("st_rotate")}
              )
                  
#' @rdname st_convexhull 
    setGeneric(
        name="st_convexhull"
            ,def=function(geom)  {standardGeneric("st_convexhull")}
              )
                  
#' @rdname st_numpoints 
    setGeneric(
        name="st_numpoints"
            ,def=function(geom)  {standardGeneric("st_numpoints")}
              )
                  
#' @rdname st_intersects 
    setGeneric(
        name="st_intersects"
            ,def=function(left,right)  {standardGeneric("st_intersects")}
              )
                  
#' @rdname st_intersection 
    setGeneric(
        name="st_intersection"
            ,def=function(left,right)  {standardGeneric("st_intersection")}
              )
                  
#' @rdname st_srid 
    setGeneric(
        name="st_srid"
            ,def=function(geom)  {standardGeneric("st_srid")}
              )
                  
#' @rdname st_setsrid 
    setGeneric(
        name="st_setsrid"
            ,def=function(geom,srid)  {standardGeneric("st_setsrid")}
              )
                  
#' @rdname st_transform 
    setGeneric(
        name="st_transform"
            ,def=function(geom,srid)  {standardGeneric("st_transform")}
              )
                  
#' @rdname st_intersects_aggregate 
    setGeneric(
        name="st_intersects_aggregate"
            ,def=function(leftIndex,rightIndex)  {standardGeneric("st_intersects_aggregate")}
              )
                  
#' @rdname st_intersection_aggregate 
    setGeneric(
        name="st_intersection_aggregate"
            ,def=function(leftIndex,rightIndex)  {standardGeneric("st_intersection_aggregate")}
              )
                  
#' @rdname mosaic_explode 
    setGeneric(
        name="mosaic_explode"
            ,def=function(geom,resolution)  {standardGeneric("mosaic_explode")}
              )
                  
#' @rdname mosaic_explode 
    setGeneric(
        name="mosaic_explode"
            ,def=function(geom,resolution)  {standardGeneric("mosaic_explode")}
              )
                  
#' @rdname mosaicfill 
    setGeneric(
        name="mosaicfill"
            ,def=function(geom,resolution)  {standardGeneric("mosaicfill")}
              )
                  
#' @rdname mosaicfill 
    setGeneric(
        name="mosaicfill"
            ,def=function(geom,resolution)  {standardGeneric("mosaicfill")}
              )
                  
#' @rdname point_index_geom 
    setGeneric(
        name="point_index_geom"
            ,def=function(point,resolution)  {standardGeneric("point_index_geom")}
              )
                  
#' @rdname point_index_geom 
    setGeneric(
        name="point_index_geom"
            ,def=function(point,resolution)  {standardGeneric("point_index_geom")}
              )
                  
#' @rdname point_index_lonlat 
    setGeneric(
        name="point_index_lonlat"
            ,def=function(lon,lat,resolution)  {standardGeneric("point_index_lonlat")}
              )
                  
#' @rdname point_index_lonlat 
    setGeneric(
        name="point_index_lonlat"
            ,def=function(lon,lat,resolution)  {standardGeneric("point_index_lonlat")}
              )
                  
#' @rdname polyfill 
    setGeneric(
        name="polyfill"
            ,def=function(geom,resolution)  {standardGeneric("polyfill")}
              )
                  
#' @rdname polyfill 
    setGeneric(
        name="polyfill"
            ,def=function(geom,resolution)  {standardGeneric("polyfill")}
              )
                  
#' @rdname index_geometry 
    setGeneric(
        name="index_geometry"
            ,def=function(indexID)  {standardGeneric("index_geometry")}
              )
                  
#' @rdname try_sql 
    setGeneric(
        name="try_sql"
            ,def=function(inCol)  {standardGeneric("try_sql")}
              )
                  
