package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.{GDAL, GDALBlock}
import com.databricks.labs.gbx.rasterx.operator.GDALTranslate
import org.gdal.gdal.{Band, Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants.GF_Write

object Convolve {

    def convolve(ds: Dataset, options: Map[String, String], kernel: Array[Array[Double]]): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val driver = ds.GetDriver()
        val outShortName = driver.getShortName
        val extension = GDAL.getExtension(outShortName)
        val tmpPath = s"/vsimem/convolve_$uuid.$extension"

        // Create a copy via gdal_translate to ensure proper format, compression, etc.
        val (cpy, _) = GDALTranslate.executeTranslate(tmpPath, ds, "gdal_translate", options)
        cpy.FlushCache()
        cpy.delete()

        val outputRaster = gdal.Open(tmpPath, GF_Write)

        val kernelH = kernel.length
        require(kernelH % 2 == 1, "Kernel size must be odd")
        val stride = kernelH / 2
        val resultBuf: Array[Double] = Array.emptyDoubleArray
        val trimBuf: Array[Double] = Array.emptyDoubleArray

        for (bandIndex <- 1 to ds.GetRasterCount()) {
            val band = ds.GetRasterBand(bandIndex)
            val outputBand = outputRaster.GetRasterBand(bandIndex)
            convolve(band, kernel, outputBand, stride, resultBuf, trimBuf)
        }

        outputRaster.FlushCache()

        val errorMsg = gdal.GetLastErrorMsg
        val newOptions = Map(
          "path" -> tmpPath,
          "parentPath" -> options.getOrElse("path", ""),
          "driver" -> driver.getShortName,
          "last_command" -> s"convolve -wo KERNEL=${kernel.map(_.mkString(",")).mkString(";")}",
          "last_error" -> errorMsg,
          "all_parents" -> ds.GetDescription(),
          "size" -> -1.toString, // Size will be determined later
          "format" -> driver.getShortName,
          "compression" -> options.getOrElse("compression", "ZSTD"),
          "isZipped" -> "false",
          "isSubset" -> "false"
        )
        outputRaster.FlushCache()
        (outputRaster, newOptions)
    }

    private def convolve(
        band: Band,
        kernel: Array[Array[Double]],
        outputBand: Band,
        stride: Int,
        _resultBuf: Array[Double],
        _trimBuf: Array[Double]
    ): Unit = {
        val xSize = band.GetXSize()
        val ySize = band.GetYSize()
        val xBlockSize = band.GetBlockXSize()
        val yBlockSize = band.GetBlockYSize()

        var resultBuf = _resultBuf
        var trimBuf = _trimBuf

        var yOffset = 0
        while (yOffset < ySize) {
            val h = math.min(yBlockSize, ySize - yOffset)
            var xOffset = 0
            while (xOffset < xSize) {
                val w = math.min(xBlockSize, xSize - xOffset)

                val current = GDALBlock(
                  band,
                  stride,
                  xOffset,
                  yOffset,
                  w,
                  h
                )

                val resultLen = current.block.length
                if (resultBuf.length != resultLen) resultBuf = new Array[Double](resultLen)
                val trimmedW = current.width - 2 * stride
                val trimmedH = current.height - 2 * stride
                val trimLen = math.max(0, trimmedW) * math.max(0, trimmedH)
                if (trimBuf.length != trimLen) trimBuf = new Array[Double](trimLen)

                var y = 0
                while (y < current.height) {
                    var x = 0
                    val rowBase = y * current.width
                    while (x < current.width) {
                        resultBuf(rowBase + x) = current.convolveAt(x, y, kernel)
                        x += 1
                    }
                    y += 1
                }

                if (trimLen > 0) {
                    var ty = 0
                    while (ty < trimmedH) {
                        val srcRow = (ty + stride) * current.width + stride
                        val dstRow = ty * trimmedW
                        System.arraycopy(resultBuf, srcRow, trimBuf, dstRow, trimmedW)
                        ty += 1
                    }
                    outputBand.WriteRaster(xOffset, yOffset, trimmedW, trimmedH, trimBuf)
                }

                xOffset += xBlockSize
            }
            yOffset += yBlockSize
        }
    }

}
