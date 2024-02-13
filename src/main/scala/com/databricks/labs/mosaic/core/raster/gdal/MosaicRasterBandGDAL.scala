package com.databricks.labs.mosaic.core.raster.gdal

import org.gdal.gdal.Band
import org.gdal.gdalconst.gdalconstConstants

import scala.collection.JavaConverters.dictionaryAsScalaMapConverter
import scala.util._

/** GDAL implementation of the MosaicRasterBand trait. */
case class MosaicRasterBandGDAL(band: Band, id: Int) {

    def getBand: Band = band

    /**
      * @return
      *   The band's index.
      */
    def index: Int = id

    /**
      * @return
      *   The band's description.
      */
    def description: String = coerceNull(Try(band.GetDescription))

    /**
      * @return
      *   Returns the pixels of the raster as a 1D array.
      */
    def values: Array[Double] = values(0, 0, xSize, ySize)

    /**
      * @return
      *   Returns the pixels of the raster as a 1D array.
      */
    def maskValues: Array[Double] = maskValues(0, 0, xSize, ySize)

    /**
      * Get the band's metadata as a Map.
      *
      * @return
      *   A Map of the band's metadata.
      */
    def metadata: Map[String, String] =
        Option(band.GetMetadata_Dict)
            .map(_.asScala.toMap.asInstanceOf[Map[String, String]])
            .getOrElse(Map.empty[String, String])

    /**
      * @return
      *   Returns band's unity type.
      */
    def units: String = coerceNull(Try(band.GetUnitType))

    /**
      * Utility method to coerce a null value to an empty string.
      * @param tryVal
      *   The Try value to coerce.
      * @return
      *   The value of the Try or an empty string.
      */
    def coerceNull(tryVal: Try[String]): String = tryVal.filter(_ != null).getOrElse("")

    /**
      * @return
      *   Returns the band's data type.
      */
    def dataType: Int = Try(band.getDataType).getOrElse(0)

    /**
      * @return
      *   Returns the band's x size.
      */
    def xSize: Int = Try(band.GetXSize).getOrElse(0)

    /**
      * @return
      *   Returns the band's y size.
      */
    def ySize: Int = Try(band.GetYSize).getOrElse(0)

    /**
      * @return
      *   Returns the band's min pixel value.
      */
    def minPixelValue: Double = computeMinMax.head

    /**
      * @return
      *   Returns the band's max pixel value.
      */
    def maxPixelValue: Double = computeMinMax.last

    /**
      * @return
      *   Returns the band's min and max pixel values.
      */
    def computeMinMax: Seq[Double] = {
        val minMaxVals = Array.fill[Double](2)(0)
        Try(band.ComputeRasterMinMax(minMaxVals, 0))
            .map(_ => minMaxVals.toSeq)
            .getOrElse(Seq(Double.NaN, Double.NaN))
    }

    /**
      * @return
      *   Returns the band's no data value.
      */
    def noDataValue: Double = {
        val noDataVal = Array.fill[java.lang.Double](1)(0)
        band.GetNoDataValue(noDataVal)
        noDataVal.head
    }

    /**
      * Get the band's pixels as a 1D array.
      *
      * @param xOffset
      *   The x offset to start reading from.
      * @param yOffset
      *   The y offset to start reading from.
      * @param xSize
      *   The number of pixels to read in the x direction.
      * @param ySize
      *   The number of pixels to read in the y direction.
      * @return
      *   A 2D array of pixels from the band.
      */
    def values(xOffset: Int, yOffset: Int, xSize: Int, ySize: Int): Array[Double] = {
        val flatArray = Array.ofDim[Double](xSize * ySize)
        (xSize, ySize) match {
            case (0, 0) => Array.empty[Double]
            case _      =>
                band.ReadRaster(xOffset, yOffset, xSize, ySize, xSize, ySize, gdalconstConstants.GDT_Float64, flatArray, 0, 0)
                flatArray
        }
    }

    /**
      * Get the band's pixels as a 1D array.
      *
      * @param xOffset
      *   The x offset to start reading from.
      * @param yOffset
      *   The y offset to start reading from.
      * @param xSize
      *   The number of pixels to read in the x direction.
      * @param ySize
      *   The number of pixels to read in the y direction.
      * @return
      *   A 2D array of pixels from the band.
      */
    def maskValues(xOffset: Int, yOffset: Int, xSize: Int, ySize: Int): Array[Double] = {
        val flatArray = Array.ofDim[Double](xSize * ySize)
        val maskBand = band.GetMaskBand
        (xSize, ySize) match {
            case (0, 0) => Array.empty[Double]
            case _      =>
                maskBand.ReadRaster(xOffset, yOffset, xSize, ySize, xSize, ySize, gdalconstConstants.GDT_Float64, flatArray, 0, 0)
                flatArray
        }
    }

    /**
      * @return
      *   Returns the band's pixel value with scale and offset applied.
      */
    def pixelValueToUnitValue(pixelValue: Double): Double = (pixelValue * pixelValueScale) + pixelValueOffset

    def pixelValueScale: Double = {
        val scale = Array.fill[java.lang.Double](1)(0)
        Try(band.GetScale(scale))
            .map(_ => scale.head.doubleValue())
            .getOrElse(0.0)
    }

    /**
      * @return
      *   Returns the band's pixel value scale.
      */
    def pixelValueOffset: Double = {
        val offset = Array.fill[java.lang.Double](1)(0)
        Try(band.GetOffset(offset))
            .map(_ => offset.head.doubleValue())
            .getOrElse(0.0)
    }

    /**
      * Reads band pixels and band mask pixels into a 2D array of doubles. If
      * the mask pixels is set to 0.0 skip the pixel and use default value.
      * @param f
      *   the function to apply to each pixel.
      * @param default
      *   the default value to use if the pixel is noData.
      * @tparam T
      *   the return type of the function.
      * @return
      *   an array of the results of applying f to each pixel.
      */
    def transformValues[T](f: (Int, Int, Double) => T, default: T = null): Seq[Seq[T]] = {
        val maskBand = band.GetMaskBand()
        val bandValues = Array.ofDim[Double](band.GetXSize() * band.GetYSize())
        val maskValues = Array.ofDim[Byte](band.GetXSize() * band.GetYSize())

        band.ReadRaster(0, 0, band.GetXSize(), band.GetYSize(), bandValues)
        maskBand.ReadRaster(0, 0, band.GetXSize(), band.GetYSize(), maskValues)
        band.FlushCache()
        maskBand.FlushCache()

        for (y <- 0 until band.GetYSize()) yield {
            for (x <- 0 until band.GetXSize()) yield {
                val index = y * band.GetXSize() + x
                if (maskValues(index) == 0.0) {
                    default
                } else {
                    f(x, y, bandValues(index))
                }
            }
        }
    }

    /**
      * @return
      *   Returns the band's mask flags.
      */
    def maskFlags: Seq[Any] = Seq(band.GetMaskFlags())

    /**
      * @return
      *   Returns true if the band is a no data mask.
      */
    def isNoDataMask: Boolean = band.GetMaskFlags() == gdalconstConstants.GMF_NODATA

    /**
      * @return
      *   Returns true if the band is empty.
      */
    def isEmpty: Boolean = {
        val stats = band.AsMDArray().GetStatistics()
        stats.getValid_count == 0
    }

}
