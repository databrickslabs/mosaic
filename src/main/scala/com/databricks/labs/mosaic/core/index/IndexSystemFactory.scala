package com.databricks.labs.mosaic.core.index

object IndexSystemFactory {

    def getIndexSystem(name: String): IndexSystem = {
        val customIndexRE = "CUSTOM\\((-?\\d+), ?(-?\\d+), ?(-?\\d+), ?(-?\\d+), ?(\\d+), ?(\\d+), ?(\\d+) ?\\)".r

        name match {
            case "H3"  => H3IndexSystem
            case "BNG" => BNGIndexSystem
            case customIndexRE(xMin, xMax, yMin, yMax, splits, rootCellSizeX, rootCellSizeY)
                => new CustomIndexSystem(
                    GridConf(
                        xMin.toInt,
                        xMax.toInt,
                        yMin.toInt,
                        yMax.toInt,
                        splits.toInt,
                        rootCellSizeX.toInt,
                        rootCellSizeY.toInt))
            case _ => throw new Error("Index not supported yet!")
        }
    }
}