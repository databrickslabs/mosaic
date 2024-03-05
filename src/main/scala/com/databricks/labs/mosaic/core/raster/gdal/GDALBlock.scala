package com.databricks.labs.mosaic.core.raster.gdal

import scala.reflect.ClassTag

case class GDALBlock[T: ClassTag](
    block: Array[T],
    maskBlock: Array[Double],
    noDataValue: Double,
    xOffset: Int,
    yOffset: Int,
    width: Int,
    height: Int,
    padding: Padding
)(implicit
    num: Numeric[T]
) {

    def elementAt(index: Int): T = block(index)

    def maskAt(index: Int): Double = maskBlock(index)

    def elementAt(x: Int, y: Int): T = block(y * width + x)

    def maskAt(x: Int, y: Int): Double = maskBlock(y * width + x)

    def rasterElementAt(x: Int, y: Int): T = block((y - yOffset) * width + (x - xOffset))

    def rasterMaskAt(x: Int, y: Int): Double = maskBlock((y - yOffset) * width + (x - xOffset))

    def valuesAt(x: Int, y: Int, kernelWidth: Int, kernelHeight: Int): Array[Double] = {
        val kernelCenterX = kernelWidth / 2
        val kernelCenterY = kernelHeight / 2
        val values = Array.fill[Double](kernelWidth * kernelHeight)(noDataValue)
        var n = 0
        for (i <- 0 until kernelHeight) {
            for (j <- 0 until kernelWidth) {
                val xIndex = x + (j - kernelCenterX)
                val yIndex = y + (i - kernelCenterY)
                if (xIndex >= 0 && xIndex < width && yIndex >= 0 && yIndex < height) {
                    val maskValue = maskAt(xIndex, yIndex)
                    val value = elementAt(xIndex, yIndex)
                    if (maskValue != 0.0 && num.toDouble(value) != noDataValue) {
                        values(n) = num.toDouble(value)
                        n += 1
                    }
                }
            }
        }
        val result = values.filter(_ != noDataValue)
        // always return only one NoDataValue if no valid values are found
        // one and only one NoDataValue can be returned
        // in all cases that have some valid values, the NoDataValue will be filtered out
        if (result.isEmpty) {
            Array(noDataValue)
        } else {
            result
        }
    }

    def convolveAt(x: Int, y: Int, kernel: Array[Array[Double]]): Double = {
        val kernelWidth = kernel.head.length
        val kernelHeight = kernel.length
        val kernelCenterX = kernelWidth / 2
        val kernelCenterY = kernelHeight / 2
        var sum = 0.0
        for (i <- 0 until kernelHeight) {
            for (j <- 0 until kernelWidth) {
                val xIndex = x + (j - kernelCenterX)
                val yIndex = y + (i - kernelCenterY)
                if (xIndex >= 0 && xIndex < width && yIndex >= 0 && yIndex < height) {
                    val maskValue = maskAt(xIndex, yIndex)
                    val value = elementAt(xIndex, yIndex)
                    if (maskValue != 0.0 && num.toDouble(value) != noDataValue) {
                        sum += num.toDouble(value) * kernel(i)(j)
                    }
                }
            }
        }
        sum
    }

    def avgFilterAt(x: Int, y: Int, kernelSize: Int): Double = {
        val values = valuesAt(x, y, kernelSize, kernelSize)
        values.sum / values.length
    }

    def minFilterAt(x: Int, y: Int, kernelSize: Int): Double = {
        val values = valuesAt(x, y, kernelSize, kernelSize)
        values.min
    }

    def maxFilterAt(x: Int, y: Int, kernelSize: Int): Double = {
        val values = valuesAt(x, y, kernelSize, kernelSize)
        values.max
    }

    def medianFilterAt(x: Int, y: Int, kernelSize: Int): Double = {
        val values = valuesAt(x, y, kernelSize, kernelSize)
        val n = values.length
        scala.util.Sorting.quickSort(values)
        values(n / 2)
    }

    def modeFilterAt(x: Int, y: Int, kernelSize: Int): Double = {
        val values = valuesAt(x, y, kernelSize, kernelSize)
        val counts = values.groupBy(identity).mapValues(_.length)
        counts.maxBy(_._2)._1
    }

    def trimBlock(stride: Int): GDALBlock[Double] = {
        val resultBlock = padding.removePadding(block.map(num.toDouble), width, stride)
        val resultMask = padding.removePadding(maskBlock, width, stride)

        val newOffset = padding.newOffset(xOffset, yOffset, stride)
        val newSize = padding.newSize(width, height, stride)

        new GDALBlock[Double](
          resultBlock,
          resultMask,
          noDataValue,
          newOffset._1,
          newOffset._2,
          newSize._1,
          newSize._2,
          Padding.NoPadding
        )
    }

}

object GDALBlock {

    def getSize(offset: Int, maxSize: Int, blockSize: Int, stride: Int, paddingStrides: Int): Int = {
        if (offset + blockSize + paddingStrides * stride > maxSize) {
            maxSize - offset
        } else {
            blockSize + paddingStrides * stride
        }
    }

    def apply(
        band: MosaicRasterBandGDAL,
        stride: Int,
        xOffset: Int,
        yOffset: Int,
        blockSize: Int
    ): GDALBlock[Double] = {
        val noDataValue = band.noDataValue
        val rasterWidth = band.xSize
        val rasterHeight = band.ySize
        // Always read blockSize + stride pixels on every side
        // This is fine since kernel size is always much smaller than blockSize

        val padding = Padding(
          left = xOffset != 0,
          right = xOffset + blockSize < rasterWidth - 1, // not sure about -1
          top = yOffset != 0,
          bottom = yOffset + blockSize < rasterHeight - 1
        )

        val xo = Math.max(0, xOffset - stride)
        val yo = Math.max(0, yOffset - stride)

        val xs = getSize(xo, rasterWidth, blockSize, stride, padding.horizontalStrides)
        val ys = getSize(yo, rasterHeight, blockSize, stride, padding.verticalStrides)

        val block = Array.ofDim[Double](xs * ys)
        val maskBlock = Array.ofDim[Double](xs * ys)

        band.getBand.ReadRaster(xo, yo, xs, ys, block)
        band.getBand.GetMaskBand().ReadRaster(xo, yo, xs, ys, maskBlock)

        GDALBlock(
          block,
          maskBlock,
          noDataValue,
          xo,
          yo,
          xs,
          ys,
          padding
        )
    }

}
