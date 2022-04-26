setGeneric(
        name="as_hex"
            ,def=function(inGeom)  {standardGeneric("as_hex")}
              )
                  
setGeneric(
        name="as_json"
            ,def=function(inGeom)  {standardGeneric("as_json")}
              )
                  
setGeneric(
        name="st_point"
            ,def=function(xVal,yVal)  {standardGeneric("st_point")}
              )
                  
setGeneric(
        name="st_makeline"
            ,def=function(points)  {standardGeneric("st_makeline")}
              )
                  
setGeneric(
        name="st_makepolygon"
            ,def=function(boundaryRing)  {standardGeneric("st_makepolygon")}
              )
                  
setGeneric(
        name="st_makepolygon"
            ,def=function(boundaryRing,holeRingArray)  {standardGeneric("st_makepolygon")}
              )
                  
setGeneric(
        name="flatten_polygons"
            ,def=function(geom)  {standardGeneric("flatten_polygons")}
              )
                  
setGeneric(
        name="st_xmax"
            ,def=function(geom)  {standardGeneric("st_xmax")}
              )
                  
setGeneric(
        name="st_xmin"
            ,def=function(geom)  {standardGeneric("st_xmin")}
              )
                  
setGeneric(
        name="st_ymax"
            ,def=function(geom)  {standardGeneric("st_ymax")}
              )
                  
setGeneric(
        name="st_ymin"
            ,def=function(geom)  {standardGeneric("st_ymin")}
              )
                  
setGeneric(
        name="st_zmax"
            ,def=function(geom)  {standardGeneric("st_zmax")}
              )
                  
setGeneric(
        name="st_zmin"
            ,def=function(geom)  {standardGeneric("st_zmin")}
              )
                  
setGeneric(
        name="st_isvalid"
            ,def=function(geom)  {standardGeneric("st_isvalid")}
              )
                  
setGeneric(
        name="st_geometrytype"
            ,def=function(geom)  {standardGeneric("st_geometrytype")}
              )
                  
setGeneric(
        name="st_area"
            ,def=function(geom)  {standardGeneric("st_area")}
              )
                  
setGeneric(
        name="st_centroid2D"
            ,def=function(geom)  {standardGeneric("st_centroid2D")}
              )
                  
setGeneric(
        name="st_centroid3D"
            ,def=function(geom)  {standardGeneric("st_centroid3D")}
              )
                  
setGeneric(
        name="convert_to"
            ,def=function(inGeom,outDataType)  {standardGeneric("convert_to")}
              )
                  
setGeneric(
        name="st_geomfromwkt"
            ,def=function(inGeom)  {standardGeneric("st_geomfromwkt")}
              )
                  
setGeneric(
        name="st_geomfromwkb"
            ,def=function(inGeom)  {standardGeneric("st_geomfromwkb")}
              )
                  
setGeneric(
        name="st_geomfromgeojson"
            ,def=function(inGeom)  {standardGeneric("st_geomfromgeojson")}
              )
                  
setGeneric(
        name="st_aswkt"
            ,def=function(geom)  {standardGeneric("st_aswkt")}
              )
                  
setGeneric(
        name="st_astext"
            ,def=function(geom)  {standardGeneric("st_astext")}
              )
                  
setGeneric(
        name="st_aswkb"
            ,def=function(geom)  {standardGeneric("st_aswkb")}
              )
                  
setGeneric(
        name="st_asbinary"
            ,def=function(geom)  {standardGeneric("st_asbinary")}
              )
                  
setGeneric(
        name="st_asgeojson"
            ,def=function(geom)  {standardGeneric("st_asgeojson")}
              )
                  
setGeneric(
        name="st_dump"
            ,def=function(geom)  {standardGeneric("st_dump")}
              )
                  
setGeneric(
        name="st_length"
            ,def=function(geom)  {standardGeneric("st_length")}
              )
                  
setGeneric(
        name="st_perimeter"
            ,def=function(geom)  {standardGeneric("st_perimeter")}
              )
                  
setGeneric(
        name="st_distance"
            ,def=function(geom1,geom2)  {standardGeneric("st_distance")}
              )
                  
setGeneric(
        name="st_contains"
            ,def=function(geom1,geom2)  {standardGeneric("st_contains")}
              )
                  
setGeneric(
        name="st_translate"
            ,def=function(geom1,xd,yd)  {standardGeneric("st_translate")}
              )
                  
setGeneric(
        name="st_scale"
            ,def=function(geom1,xd,yd)  {standardGeneric("st_scale")}
              )
                  
setGeneric(
        name="st_rotate"
            ,def=function(geom1,td)  {standardGeneric("st_rotate")}
              )
                  
setGeneric(
        name="st_convexhull"
            ,def=function(geom)  {standardGeneric("st_convexhull")}
              )
                  
setGeneric(
        name="st_numpoints"
            ,def=function(geom)  {standardGeneric("st_numpoints")}
              )
                  
setGeneric(
        name="st_intersects"
            ,def=function(left,right)  {standardGeneric("st_intersects")}
              )
                  
setGeneric(
        name="st_intersection"
            ,def=function(left,right)  {standardGeneric("st_intersection")}
              )
                  
setGeneric(
        name="st_srid"
            ,def=function(geom)  {standardGeneric("st_srid")}
              )
                  
setGeneric(
        name="st_setsrid"
            ,def=function(geom,srid)  {standardGeneric("st_setsrid")}
              )
                  
setGeneric(
        name="st_transform"
            ,def=function(geom,srid)  {standardGeneric("st_transform")}
              )
                  
setGeneric(
        name="st_intersects_aggregate"
            ,def=function(leftIndex,rightIndex)  {standardGeneric("st_intersects_aggregate")}
              )
                  
setGeneric(
        name="st_intersection_aggregate"
            ,def=function(leftIndex,rightIndex)  {standardGeneric("st_intersection_aggregate")}
              )
                  
setGeneric(
        name="mosaic_explode"
            ,def=function(geom,resolution)  {standardGeneric("mosaic_explode")}
              )
                  
setGeneric(
        name="mosaic_explode"
            ,def=function(geom,resolution)  {standardGeneric("mosaic_explode")}
              )
                  
setGeneric(
        name="mosaicfill"
            ,def=function(geom,resolution)  {standardGeneric("mosaicfill")}
              )
                  
setGeneric(
        name="mosaicfill"
            ,def=function(geom,resolution)  {standardGeneric("mosaicfill")}
              )
                  
setGeneric(
        name="point_index_geom"
            ,def=function(point,resolution)  {standardGeneric("point_index_geom")}
              )
                  
setGeneric(
        name="point_index_geom"
            ,def=function(point,resolution)  {standardGeneric("point_index_geom")}
              )
                  
setGeneric(
        name="point_index_lonlat"
            ,def=function(lon,lat,resolution)  {standardGeneric("point_index_lonlat")}
              )
                  
setGeneric(
        name="point_index_lonlat"
            ,def=function(lon,lat,resolution)  {standardGeneric("point_index_lonlat")}
              )
                  
setGeneric(
        name="polyfill"
            ,def=function(geom,resolution)  {standardGeneric("polyfill")}
              )
                  
setGeneric(
        name="polyfill"
            ,def=function(geom,resolution)  {standardGeneric("polyfill")}
              )
                  
setGeneric(
        name="index_geometry"
            ,def=function(indexID)  {standardGeneric("index_geometry")}
              )
                  
setGeneric(
        name="try_sql"
            ,def=function(inCol)  {standardGeneric("try_sql")}
              )
                  
