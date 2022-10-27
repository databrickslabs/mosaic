sdf <- sparklyr::sdf_copy_to(sc,
                             data.frame(
                               wkt = "POLYGON ((0 0, 0 2, 1 2, 1 0, 0 0))",
                               point_wkt = "POINT (1 1)" 
                             )
)

sdf <- mutate(sdf, "st_area" = st_area(wkt))
sdf <- mutate(sdf, "st_length" = st_length(wkt))
sdf <- mutate(sdf, "st_perimeter" =  st_perimeter(wkt))
sdf <- mutate(sdf, "st_convexhull" = st_convexhull(wkt))
sdf <- mutate(sdf, "st_dump" = st_dump(wkt))
sdf <- mutate(sdf, "st_translate" =  st_translate(wkt, 1L, 1L))
sdf <- mutate(sdf, "st_scale" =  st_scale(wkt, 1L, 1L))
sdf <- mutate(sdf, "st_rotate" = st_rotate(wkt, 1L))
sdf <- mutate(sdf, "st_centroid2D" = st_centroid2D(wkt))
sdf <- mutate(sdf, "st_centroid3D" = st_centroid3D(wkt))
sdf <- mutate(sdf, "st_length" = st_length(wkt))
sdf <- mutate(sdf, "st_isvalid" =  st_isvalid(wkt))
sdf <- mutate(sdf, "st_intersects" = st_intersects(wkt, wkt))
sdf <- mutate(sdf, "st_intersection" = st_intersection(wkt, wkt))
sdf <- mutate(sdf, "st_geometrytype" = st_geometrytype(wkt))
sdf <- mutate(sdf, "st_isvalid" =  st_isvalid(wkt))
sdf <- mutate(sdf, "st_xmin" = st_xmin(wkt))
sdf <- mutate(sdf, "st_xmax" = st_xmax(wkt))
sdf <- mutate(sdf, "st_ymin" = st_ymin(wkt))
sdf <- mutate(sdf, "st_ymax" = st_ymax(wkt))
sdf <- mutate(sdf, "st_zmin" = st_zmin(wkt))
sdf <- mutate(sdf, "st_zmax" = st_zmax(wkt))
sdf <- mutate(sdf, "flatten_polygons" =  flatten_polygons(wkt))
sdf <- mutate(sdf, "point_index_lonlat" =  point_index_lonlat(as.double(1L), as.double(1L), 1L)) # issue with type. needs a double but native R type not converting properly so requires explicit setting
sdf <- mutate(sdf, "point_index_geom" =  point_index_geom(point_wkt, as.integer(1L)))
sdf <- mutate(sdf, "index_geometry" =  index_geometry(point_index_geom))
sdf <- mutate(sdf, "polyfill" =  polyfill(wkt, as.integer(1L))) # requires a long and passing a double or an integer causes a cast exception - can't convert to Long.  
sdf <- mutate(sdf, "mosaic_explode" =  mosaic_explode(wkt, as.integer(1L)))
sdf <- mutate(sdf, "mosaicfill" =  mosaicfill(wkt, 1L))
sdf <- mutate(sdf, "geom_with_srid" =  st_setsrid(st_geomfromwkt(wkt), 4326L))
sdf <- mutate(sdf, "srid_check" =  st_srid(geom_with_srid))
sdf <- mutate(sdf, "transformed_geom" =  st_transform(geom_with_srid, 3857L))




sdf %>% sparklyr::collect() %>% as.data.frame()