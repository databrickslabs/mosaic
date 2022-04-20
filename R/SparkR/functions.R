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
setMethod(
              f = "convert_to"
              ,signature(
                   inGeom = 'Column'
                   ,outDataType = 'String'
                )
              ,function(inGeom,outDataType) {
                  jc <- sparkR.callJMethod(functions, "convert_to", inGeom@jc, outDataType@jc )
                  column(jc)
                  }
              )
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
setMethod(
              f = "mosaic_explode"
              ,signature(
                   geom = 'Column'
                   ,resolution = 'Int'
                )
              ,function(geom,resolution) {
                  jc <- sparkR.callJMethod(functions, "mosaic_explode", geom@jc, resolution@jc )
                  column(jc)
                  }
              )
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
setMethod(
              f = "mosaicfill"
              ,signature(
                   geom = 'Column'
                   ,resolution = 'Int'
                )
              ,function(geom,resolution) {
                  jc <- sparkR.callJMethod(functions, "mosaicfill", geom@jc, resolution@jc )
                  column(jc)
                  }
              )
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
setMethod(
              f = "point_index_geom"
              ,signature(
                   point = 'Column'
                   ,resolution = 'Int'
                )
              ,function(point,resolution) {
                  jc <- sparkR.callJMethod(functions, "point_index_geom", point@jc, resolution@jc )
                  column(jc)
                  }
              )
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
setMethod(
              f = "point_index_lonlat"
              ,signature(
                   lon = 'Column'
                   ,lat = 'Column'
                   ,resolution = 'Int'
                )
              ,function(lon,lat,resolution) {
                  jc <- sparkR.callJMethod(functions, "point_index_lonlat", lon@jc, lat@jc, resolution@jc )
                  column(jc)
                  }
              )
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
setMethod(
              f = "polyfill"
              ,signature(
                   geom = 'Column'
                   ,resolution = 'Int'
                )
              ,function(geom,resolution) {
                  jc <- sparkR.callJMethod(functions, "polyfill", geom@jc, resolution@jc )
                  column(jc)
                  }
              )
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
