package com.databricks.labs.mosaic.core.raster.gdal

import com.databricks.labs.mosaic.gdal.MosaicGDAL
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
      *   Returns the band's data type as a string.
      */
    def dataTypeHuman: String = Try(band.getDataType).getOrElse(0) match {
        case gdalconstConstants.GDT_Byte  => "Byte"
        case gdalconstConstants.GDT_UInt16 => "UInt16"
        case gdalconstConstants.GDT_Int16 => "Int16"
        case gdalconstConstants.GDT_UInt32 => "UInt32"
        case gdalconstConstants.GDT_Int32 => "Int32"
        case gdalconstConstants.GDT_Float32 => "Float32"
        case gdalconstConstants.GDT_Float64 => "Float64"
        case gdalconstConstants.GDT_CInt16=> "ComplexInt16"
        case gdalconstConstants.GDT_CInt32=> "ComplexInt32"
        case gdalconstConstants.GDT_CFloat32=> "ComplexFloat32"
        case gdalconstConstants.GDT_CFloat64=> "ComplexFloat64"
        case _  => "Unknown"
    }

    /**
      * @return
      *   Returns the estimated number of bytes in each pixel.
      */
    def dataTypeBytes: Int = {
        val pixelMemSizeMap = Map(
            gdalconstConstants.GDT_Byte -> 1,
            gdalconstConstants.GDT_UInt16 -> 2,
            gdalconstConstants.GDT_Int16 -> 2,
            gdalconstConstants.GDT_UInt32 -> 4,
            gdalconstConstants.GDT_Int32 -> 4,
            gdalconstConstants.GDT_Float32 -> 4,
            gdalconstConstants.GDT_Float64 -> 8,
            gdalconstConstants.GDT_CInt16 -> 2,
            gdalconstConstants.GDT_CInt32 -> 4,
            gdalconstConstants.GDT_CFloat32 -> 4,
            gdalconstConstants.GDT_CFloat64 -> 8
        )
        if (pixelMemSizeMap.contains(dataType)) pixelMemSizeMap(dataType) else 0
    }

    /**
     * @return
     *   Returns the estimated size in memory of the band.
     */
    def estimatedSizeInMem: Int = {
        val pixelMemSize = dataTypeBytes
        val pixelCount = xSize * ySize
        pixelMemSize * pixelCount
    }

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
        // will GDAL refuse to compute these stats?
        if (band.GetXSize() == 1 || band.GetYSize() == 1) {
            val validPixels = values.filter(_ != noDataValue)
            if (validPixels.isEmpty) {
                return Seq(Double.NaN, Double.NaN)
            } else {
                Seq(validPixels.min, validPixels.max)
            }
        } else {
            Try(band.ComputeRasterMinMax(minMaxVals, 0))
                .map(_ => minMaxVals.toSeq)
                .getOrElse(Seq(Double.NaN, Double.NaN))
        }
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
      * Counts the number of pixels in the band. The mask is used to determine
      * if a pixel is valid. If pixel value is noData or mask value is 0.0, the
      * pixel is not counted by default.
      * @param countNoData
      *   If specified as true, include the noData (default is false).
      * @param countAll
      *   If specified as true, simply return bandX * bandY (default is false).
      * @return
      *   Returns the band's pixel count.
      */
    def pixelCount(countNoData: Boolean = false, countAll: Boolean = false): Int = {
        if (countAll) {
            // all pixels returned
            band.GetXSize() * band.GetYSize()
        } else {
            // nodata not included (default)
            val line = Array.ofDim[Double](band.GetXSize())
            var count = 0
            for (y <- 0 until band.GetYSize()) {
                band.ReadRaster(0, y, band.GetXSize(), 1, line)
                val maskLine = Array.ofDim[Double](band.GetXSize())
                val maskRead = band.GetMaskBand().ReadRaster(0, y, band.GetXSize(), 1, maskLine)
                if (maskRead != gdalconstConstants.CE_None) {
                    count = count + line.count(pixel => countNoData || pixel != noDataValue)
                } else {
                    count = count + line.zip(maskLine).count {
                        case (pixel, mask) => mask != 0.0 && (countNoData || pixel != noDataValue)
                    }
                }
            }
            count
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

    /**
      * Applies a kernel filter to the band. It assumes the kernel is square and
      * has an odd number of rows and columns.
      *
      * @param kernel
      *   The kernel to apply to the band.
      * @return
      *   The band with the kernel filter applied.
      */
    def convolve(kernel: Array[Array[Double]], outputBand: Band): Unit = {
        val kernelSize = kernel.length
        require(kernelSize % 2 == 1, "Kernel size must be odd")

        val blockSize = MosaicGDAL.blockSize
        val stride = kernelSize / 2

        for (yOffset <- 0 until ySize by blockSize) {
            for (xOffset <- 0 until xSize by blockSize) {

                val currentBlock = GDALBlock(
                    this,
                    stride,
                    xOffset,
                    yOffset,
                    blockSize
                )

                val result = Array.ofDim[Double](currentBlock.block.length)

                for (y <- 0 until currentBlock.height) {
                    for (x <- 0 until currentBlock.width) {
                        result(y * currentBlock.width + x) = currentBlock.convolveAt(x, y, kernel)
                    }
                }

                val trimmedResult = currentBlock.copy(block = result).trimBlock(stride)

                outputBand.WriteRaster(xOffset, yOffset, trimmedResult.width, trimmedResult.height, trimmedResult.block)
                outputBand.FlushCache()
                outputBand.GetMaskBand().WriteRaster(xOffset, yOffset, trimmedResult.width, trimmedResult.height, trimmedResult.maskBlock)
                outputBand.GetMaskBand().FlushCache()

            }
        }
    }

    def filter(kernelSize: Int, operation: String, outputBand: Band): Unit = {
        require(kernelSize % 2 == 1, "Kernel size must be odd")

        val blockSize = MosaicGDAL.blockSize
        val stride = kernelSize / 2

        for (yOffset <- 0 until ySize by blockSize) {
            for (xOffset <- 0 until xSize by blockSize) {

                val currentBlock = GDALBlock(
                  this,
                  stride,
                  xOffset,
                  yOffset,
                  blockSize
                )

                val result = Array.ofDim[Double](currentBlock.block.length)

                for (y <- 0 until currentBlock.height) {
                    for (x <- 0 until currentBlock.width) {
                        result(y * currentBlock.width + x) = operation match {
                            case "avg"    => currentBlock.avgFilterAt(x, y, kernelSize)
                            case "min"    => currentBlock.minFilterAt(x, y, kernelSize)
                            case "max"    => currentBlock.maxFilterAt(x, y, kernelSize)
                            case "median" => currentBlock.medianFilterAt(x, y, kernelSize)
                            case "mode"   => currentBlock.modeFilterAt(x, y, kernelSize)
                            case _        => throw new Exception("Invalid operation")
                        }
                    }
                }

                val trimmedResult = currentBlock.copy(block = result).trimBlock(stride)

                outputBand.WriteRaster(xOffset, yOffset, trimmedResult.width, trimmedResult.height, trimmedResult.block)
                outputBand.FlushCache()
                outputBand.GetMaskBand().WriteRaster(xOffset, yOffset, trimmedResult.width, trimmedResult.height, trimmedResult.maskBlock)
                outputBand.GetMaskBand().FlushCache()

            }
        }

    }

}
