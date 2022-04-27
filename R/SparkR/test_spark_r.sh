#!/bin/zsh 

./bin/spark-submit \
  --master local[*] \
  --jars /Users/robert.whiffin/Downloads/artefacts/mosaic-0.1.0-SNAPSHOT-jar-with-dependencies.jar \
  test_spark.R 
