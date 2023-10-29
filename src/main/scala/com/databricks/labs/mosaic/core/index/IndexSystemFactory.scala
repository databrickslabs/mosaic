package com.databricks.labs.mosaic.core.index

import org.apache.spark.sql.SparkSession

object IndexSystemFactory {

    /**
      * Returns the index system based on the spark session configuration. If
      * the spark session configuration is not set, it will default to H3.
      * @param spark
      *   SparkSession
      * @return
      *   IndexSystem
      */
    def getIndexSystem(spark: SparkSession): IndexSystem = {
        val indexSystem = spark.conf.get("spark.databricks.labs.mosaic.index.system", "H3")
        getIndexSystem(indexSystem)
    }

    /**
      * Returns the index system based on the name provided. If the name is not
      * supported, it will throw an error. For custom index systems, the format
      * is as follows: CUSTOM(xMin, xMax, yMin, yMax, splits, rootCellSizeX,
      * rootCellSizeY, crsID) or CUSTOM(xMin, xMax, yMin, yMax, splits,
      * rootCellSizeX, rootCellSizeY)
      * @param name
      *   String
      * @return
      *   IndexSystem
      */
    def getIndexSystem(name: String): IndexSystem = {
        val customIndexRE = "CUSTOM\\((-?\\d+), ?(-?\\d+), ?(-?\\d+), ?(-?\\d+), ?(\\d+), ?(\\d+), ?(\\d+) ?\\)".r
        val customIndexWithCRSRE = "CUSTOM\\((-?\\d+), ?(-?\\d+), ?(-?\\d+), ?(-?\\d+), ?(\\d+), ?(\\d+), ?(\\d+), ?(\\d+) ?\\)".r

        name match {
            case "H3"                                                                                      => H3IndexSystem
            case "BNG"                                                                                     => BNGIndexSystem
            case customIndexRE(xMin, xMax, yMin, yMax, splits, rootCellSizeX, rootCellSizeY)               => CustomIndexSystem(
                  GridConf(
                    xMin.toInt,
                    xMax.toInt,
                    yMin.toInt,
                    yMax.toInt,
                    splits.toInt,
                    rootCellSizeX.toInt,
                    rootCellSizeY.toInt
                  )
                )
            case customIndexWithCRSRE(xMin, xMax, yMin, yMax, splits, rootCellSizeX, rootCellSizeY, crsID) => CustomIndexSystem(
                  GridConf(
                    xMin.toInt,
                    xMax.toInt,
                    yMin.toInt,
                    yMax.toInt,
                    splits.toInt,
                    rootCellSizeX.toInt,
                    rootCellSizeY.toInt,
                    Some(crsID.toInt)
                  )
                )
            case _ => throw new Error("Index not supported yet!")
        }
    }

}
