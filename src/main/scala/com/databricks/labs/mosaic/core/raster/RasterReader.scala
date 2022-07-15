package com.databricks.labs.mosaic.core.raster

trait RasterReader {
  def fromBytes(bytes: Array[Byte]): MosaicRaster
}
