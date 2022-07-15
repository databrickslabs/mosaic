package com.databricks.labs.mosaic.core.raster

trait MosaicRasterBand extends Serializable {
  def index: Int
  def units: String
  def xSize: Int
  def ySize: Int
  def minPixelValue: Double
  def maxPixelValue: Double
  def pixelValueScale: Double
  def pixelValueOffset: Double
  def pixelValueToUnitValue(pixelValue: Double): Double
  def values: Array[Array[Double]] = values(0, 0, xSize, ySize)
  def values(xOffset: Int, yOffset: Int, xSize: Int, ySize: Int): Array[Array[Double]]
}
