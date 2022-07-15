package com.databricks.labs.mosaic.core.raster

import org.apache.spark.sql.types._
import org.gdal.gdal.Band
import org.gdal.gdalconst.gdalconstConstants

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag, typeOf}


class MosaicRasterBandGDAL(band: Band, id: Int) extends MosaicRasterBand {
  override def index: Int = id

  override def description: String = band.GetDescription

  override def units: String = band.GetUnitType

  override def dataType: Int = band.getDataType

  override def xSize: Int = band.GetXSize

  override def ySize: Int = band.GetYSize

  override def minPixelValue: Double = computeMinMax(0)

  override def maxPixelValue: Double = computeMinMax(1)

  override def noDataValue: Double = {
    val noDataVal = Array.fill[java.lang.Double](1)(0)
    band.GetNoDataValue(noDataVal)
    noDataVal.head
  }

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

  override def values[T: TypeTag: ClassTag](xOffset: Int, yOffset: Int, xSize: Int, ySize: Int): Array[Array[T]] = {

    typeOf[T] match {
      case t if t =:= typeOf[Double] =>
        val flatArray = Array.ofDim[Double](xSize * ySize)
        readDoubleValues(xOffset, yOffset, xSize, ySize, flatArray)
        flatArray.map(v => v.asInstanceOf[T]).grouped(xSize).toArray
      case t if t =:= typeOf[Float] =>
        val flatArray = Array.ofDim[Float](xSize * ySize)
        readFloatValues(xOffset, yOffset, xSize, ySize, flatArray)
        flatArray.map(v => v.asInstanceOf[T]).grouped(xSize).toArray
      case t if t =:= typeOf[Int] =>
        val flatArray = Array.ofDim[Int](xSize * ySize)
        readIntValues(xOffset, yOffset, xSize, ySize, flatArray)
        flatArray.map(v => v.asInstanceOf[T]).grouped(xSize).toArray
    }
  }

    def readDoubleValues(xOffset: Int, yOffset: Int, xSize: Int, ySize: Int, array: Array[Double]): Unit = {
      band.ReadRaster(xOffset, yOffset, xSize, ySize, xSize, ySize, gdalconstConstants.GDT_Float64, array, 0, 0)
    }

    def readFloatValues(xOffset: Int, yOffset: Int, xSize: Int, ySize: Int, array: Array[Float]): Unit = {
      band.ReadRaster(xOffset, yOffset, xSize, ySize, xSize, ySize, gdalconstConstants.GDT_Float32, array, 0, 0)
    }

    def readIntValues(xOffset: Int, yOffset: Int, xSize: Int, ySize: Int, array: Array[Int]): Unit = {
      band.ReadRaster(xOffset, yOffset, xSize, ySize, xSize, ySize, gdalconstConstants.GDT_Int64, array, 0, 0)
    }

  override def pixelValueToUnitValue(pixelValue: Double): Double = (pixelValue * pixelValueScale) + pixelValueOffset

  def computeMinMax: Seq[Double] = {
    val minMaxVals = Array.fill[Double](2)(0)
    band.ComputeRasterMinMax(minMaxVals, 0)
    minMaxVals.toSeq
  }

}
