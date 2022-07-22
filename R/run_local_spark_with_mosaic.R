spark_location = "/usr/local/Cellar/apache-spark/3.2.1/libexec"
mosaic_jar = "/Users/robert.whiffin/Downloads/artefacts_2/mosaic-0.1.1-SNAPSHOT-jar-with-dependencies.jar"
Sys.setenv(SPARK_HOME = spark_location)
JAVA_HOME = "/usr/local/Cellar/openjdk@8/1.8.0+312/"
Sys.setenv(JAVA_HOME = JAVA_HOME)


# start sparkr
library(SparkR, lib.loc = c(file.path(spark_location, "R", "lib")))
sparkR.session(master = "local", sparkJars = mosaic_jar)
sparkR.session.stop()


# start sparklyr
library(sparklyr)
library(sparklyr.nested) # necessary for accessing nested features
config=list()
config[["sparklyr.jars.default"]] <- mosaic_jar
sc <- spark_connect(master = "local", config = config)
spark_disconnect(sc)

#install.packages("devtools")
#library(devtools)
#devtools::install_github("mitre/sparklyr.nested")
