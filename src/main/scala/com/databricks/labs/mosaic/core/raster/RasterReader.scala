package com.databricks.labs.mosaic.core.raster

import org.apache.spark.internal.Logging

trait RasterReader extends Logging {

    def fromBytes(bytes: Array[Byte]): MosaicRaster
    def fromBytes(bytes: Array[Byte], path: String): MosaicRaster

}
