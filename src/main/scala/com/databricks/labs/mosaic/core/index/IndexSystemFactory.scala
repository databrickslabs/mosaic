package com.databricks.labs.mosaic.core.index

object IndexSystemFactory {

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
