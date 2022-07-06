raw_taxi_zones_path <- "/Users/robert.whiffin/Documents/NYC Taxi Zones.geojson"
spark_location = "/usr/local/Cellar/apache-spark/3.2.1/libexec"
mosaic_jar = "/Users/robert.whiffin/Downloads/artefacts_2/mosaic-0.1.1-SNAPSHOT-jar-with-dependencies.jar"
Sys.setenv(SPARK_HOME = spark_location)
JAVA_HOME = "/usr/local/Cellar/openjdk@8/1.8.0+312/"
Sys.setenv(JAVA_HOME = JAVA_HOME)


#source("/Users/robert.whiffin/Documents/mosaic/R/sparklyr-mosaic/mosaic_extension.R")

# start sparklyr
library(sparklyr)
library(sparklyr.nested)
config=list()
config[["sparklyr.connect.jars"]] <- mosaic_jar
sc <- spark_connect(master = "local", config = config)
#sc <- spark_connect(master = "local")

spark_adaptive_query_execution(sc, enable = FALSE)

source("/Users/robert.whiffin/Documents/mosaic/R/sparklyr-mosaic/enableMosaic.R")
source("/Users/robert.whiffin/Documents/mosaic/R/sparklyr-mosaic/generate_sparklyr_mosaic_functions.R")
enableMosaic(sc)
source("/Users/robert.whiffin/Documents/mosaic/R/sparklyr-mosaic/spark_functions.R")
neighbourhoods_pre <- sparklyr::spark_read_json(
  sc
  ,name='neighbourhoods'
  ,path = raw_taxi_zones_path
  ,options=list("multiLine"="TRUE")
) 
neighbourhoods = neighbourhoods_pre %>% mutate(feature=explode(features))

neighbourhoods2 <- neighbourhoods %>%
  sdf_select(
    type
    ,properties2=features.properties
    ,geometry2 = features.geometry
  ) 


neighbourhoods3 <- neighbourhoods2 %>%
  mutate(json_geometry = to_json(geometry2))

neighbourhoods3 %>%
  mutate(wkt = st_aswkt(st_geomfromgeojson(json_geometry))
  ) %>%
  select(wkt)

spark_disconnect(sc)
