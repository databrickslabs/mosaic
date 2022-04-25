setwd("/Users/robert.whiffin/Documents/mosaic/R/SparkR")
source("sparkrMosaic/R/generics.R")
source("sparkrMosaic/R/functions.R")

sdf = SparkR::createDataFrame(
  data.frame(
    index = 1:5, 
    X=as.double(0:4)
    ,Y=as.double(0:4)
  )
)
sdf = SparkR::withColumn(sdf, "points", st_point(column("X"), column("Y")))
SparkR::collect(sdf)

SparkR::collect(
  SparkR::withColumn(sdf, "wkt", st_aswkt(column("points")))
)

SparkR::collect(
  SparkR::withColumn(sdf, "length", st_length(st_aswkt(column("points"))))
)
