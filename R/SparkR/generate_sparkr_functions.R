function_data = c(
  "as_hex(inGeom: Column)"
  ,"as_json(inGeom: Column)"
  ,"st_point(xVal: Column, yVal: Column)"
  ,"st_makeline(points: Column)"
  ,"st_makepolygon(boundaryRing: Column)"
  ,"st_makepolygon(boundaryRing: Column, holeRingArray: Column)"
  ,"flatten_polygons(geom: Column)"
  ,"st_xmax(geom: Column)"
  ,"st_xmin(geom: Column)"
  ,"st_ymax(geom: Column)"
  ,"st_ymin(geom: Column)"
  ,"st_zmax(geom: Column)"
  ,"st_zmin(geom: Column)"
  ,"st_isvalid(geom: Column)"
  ,"st_geometrytype(geom: Column)"
  ,"st_area(geom: Column)"
  ,"st_centroid2D(geom: Column)"
  ,"st_centroid3D(geom: Column)"
  ,"convert_to(inGeom: Column, outDataType: String)"
  ,"st_geomfromwkt(inGeom: Column)"
  ,"st_geomfromwkb(inGeom: Column)"
  ,"st_geomfromgeojson(inGeom: Column)"
  ,"st_aswkt(geom: Column)"
  ,"st_astext(geom: Column)"
  ,"st_aswkb(geom: Column)"
  ,"st_asbinary(geom: Column)"
  ,"st_asgeojson(geom: Column)"
  ,"st_dump(geom: Column)"
  ,"st_length(geom: Column)"
  ,"st_perimeter(geom: Column)"
  ,"st_distance(geom1: Column, geom2: Column)"
  ,"st_contains(geom1: Column, geom2: Column)"
  ,"st_translate(geom1: Column, xd: Column, yd: Column)"
  ,"st_scale(geom1: Column, xd: Column, yd: Column)"
  ,"st_rotate(geom1: Column, td: Column)"
  ,"st_convexhull(geom: Column)"
  ,"st_numpoints(geom: Column)"
  ,"st_intersects(left: Column, right: Column)"
  ,"st_intersection(left: Column, right: Column)"
  ,"st_srid(geom: Column)"
  ,"st_setsrid(geom: Column, srid: Column)"
  ,"st_transform(geom: Column, srid: Column)"
  ,"st_intersects_aggregate(leftIndex: Column, rightIndex: Column)"
  ,"st_intersection_aggregate(leftIndex: Column, rightIndex: Column)"
  ,"mosaic_explode(geom: Column, resolution: Column)"
  ,"mosaic_explode(geom: Column, resolution: Int)"
  ,"mosaicfill(geom: Column, resolution: Column)"
  ,"mosaicfill(geom: Column, resolution: Int)"
  ,"point_index_geom(point: Column, resolution: Column)"
  ,"point_index_geom(point: Column, resolution: Int)"
  ,"point_index_lonlat(lon: Column, lat: Column, resolution: Column)"
  ,"point_index_lonlat(lon: Column, lat: Column, resolution: Int)"
  ,"polyfill(geom: Column, resolution: Column)"
  ,"polyfill(geom: Column, resolution: Int)"
  ,"index_geometry(indexID: Column)"
  ,"try_sql(inCol: Column)"
)

parser <- function(x){
  #split on left bracket to get name
  splitted = strsplit(x, "(", fixed=T)[[1]]
  # extract function name
  function_name = splitted[1]
  # remove the trailing bracket
  args = gsub( ")", '',splitted[2], fixed=T)
  args = strsplit(args, ", ", fixed=T)[[1]]
  args = lapply(args, function(x){strsplit(x, ": ", fixed=T)}[[1]])
  output = list(
    "function_name" = function_name
    ,"args"=args
  )
  output
}

############################################################
build_generic <- function(input){
  function_name = input$function_name
  args = lapply(input$args, function(x){x[1]})
  paste0(
    'setGeneric(
        name="',function_name,'"
            ,def=function(',paste0(args, collapse=','), ')  {standardGeneric("',function_name, '")}
              )
                  ')
}
############################################################
build_column_specifiers <- function(input){
  args = lapply(input$args, function(x){x[1]})
  build_column_specifier <- function(arg){ 
    return(paste0(arg, '@jc'))
  }
  
  if (length(args) > 1){
    specs <- paste0(unlist(lapply(args, build_column_specifier)), collapse = ", ")
    return(specs)
  }
  else return( build_column_specifier(args))
}
############################################################
build_method<-function(input){
  function_name = input$function_name
  arg_names = lapply(input$args, function(x){c(x[1])})
  args = lapply(input$args, function(x){c(x[1], paste0("'", x[2], "'"))})
  args = lapply(args, function(x){paste0(x,  collapse= ' = ')})
  column_specifiers <- build_column_specifiers(input)
  paste0(
    'setMethod(
              f = "',function_name, '"
              ,signature(
                   ',paste0(args, collapse = "\n                   ,"),
    '
                )
              ,function(', paste0(arg_names, collapse= ','), ') {
                  jc <- sparkR.callJMethod(functions, "', function_name,'", ', column_specifiers, ' )
                  column(jc)
                  }
              )')
  
}
############################################################

genericFileConn = file("generics.R")
methodsFileConn = file("functions.R")


generics <- lapply(function_data, function(x){build_generic(parser(x))})
methods <- lapply(function_data, function(x){build_method(parser(x))})
writeLines(paste0(generics, collapse="\n"), genericFileConn)
writeLines(paste0(methods, collapse="\n"), methodsFileConn)
closeAllConnections()
