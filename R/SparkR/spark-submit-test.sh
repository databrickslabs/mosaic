#!/bin/zsh

/usr/local/Cellar/apache-spark/3.2.1/libexec/bin/spark-submit \
  --master "local[*]" \
  --jars /Users/robert.whiffin/Downloads/artefacts/mosaic-0.1.0-SNAPSHOT-jar-with-dependencies.jar \
  /Users/robert.whiffin/Documents/mosaic/R/SparkR/sparkrMosaic/tests/test_spark.R  