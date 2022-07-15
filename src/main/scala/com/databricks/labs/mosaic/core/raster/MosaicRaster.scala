package com.databricks.labs.mosaic.core.raster

trait MosaicRaster extends Serializable {
  def numBands: Int

  def SRID: Int

  def proj4String: String

  def xSize: Int

  def ySize: Int

  def getBand(bandId: Int): MosaicRasterBand

  def extent: Seq[Double]

  def geoTransform(pixel: Int, line: Int): Seq[Double]

  def getRaster: Any
}
