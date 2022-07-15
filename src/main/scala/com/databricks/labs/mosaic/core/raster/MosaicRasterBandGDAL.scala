package com.databricks.labs.mosaic.core.raster

import org.gdal.gdal.Band
import org.gdal.gdalconst.gdalconstConstants.GDT_Float64

class MosaicRasterBandGDAL(band: Band, id: Int) extends MosaicRasterBand {
  override def index: Int = id

  override def units: String = band.GetUnitType

  override def xSize: Int = band.GetXSize

  override def ySize: Int = band.GetYSize

  override def minPixelValue: Double = computeMinMax(0)

  override def maxPixelValue: Double = computeMinMax(1)

  override def pixelValueScale: Double = {
    val scale = Array.fill[java.lang.Double](1)(0)
    band.GetScale(scale)
    scale.head
  }

  override def pixelValueOffset: Double = {
    val offset = Array.fill[java.lang.Double](1)(0)
    band.GetOffset(offset)
    offset.head
  }

  override def values(xOffset: Int, yOffset: Int, xSize: Int, ySize: Int): Array[Array[Double]] = {
    val flatArray = Array.fill[Double](xSize * ySize)(0)
    band.ReadRaster(xOffset, yOffset, xSize, ySize, xSize, ySize, GDT_Float64, flatArray, 0, 0)
    flatArray.grouped(xSize).toArray
  }

  override def pixelValueToUnitValue(pixelValue: Double): Double = (pixelValue * pixelValueScale) + pixelValueOffset

  def computeMinMax: Seq[Double] = {
    val minMaxVals = Array.fill[Double](2)(0)
    band.ComputeRasterMinMax(minMaxVals, 0)
    minMaxVals.toSeq
  }
}
