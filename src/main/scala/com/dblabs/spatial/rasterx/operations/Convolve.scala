package com.dblabs.spatial.rasterx.operations

import com.dblabs.spatial.rasterx.gdal.{GDAL, GDALBlock}
import com.dblabs.spatial.rasterx.operator.GDALTranslate
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

        for (bandIndex <- 1 to ds.GetRasterCount()) {
            val band = ds.GetRasterBand(bandIndex)
            val outputBand = outputRaster.GetRasterBand(bandIndex)
            convolve(band, kernel, outputBand)
        }

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

    private def convolve(band: Band, kernel: Array[Array[Double]], outputBand: Band): Unit = {
        val kernelSize = kernel.length
        require(kernelSize % 2 == 1, "Kernel size must be odd")

        val xSize = band.GetXSize()
        val xBlockSize = band.GetBlockXSize()
        val ySize = band.GetYSize()
        val yBlockSize = band.GetBlockYSize()
        val stride = kernelSize / 2

        for (yOffset <- 0 until ySize by yBlockSize) {
            for (xOffset <- 0 until xSize by xBlockSize) {

                val currentBlock = GDALBlock(
                  band,
                  stride,
                  xOffset,
                  yOffset,
                  xBlockSize,
                  yBlockSize
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

}
