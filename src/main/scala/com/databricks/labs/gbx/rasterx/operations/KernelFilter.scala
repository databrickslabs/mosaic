package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.{GDAL, GDALBlock}
import com.databricks.labs.gbx.rasterx.operator.GDALTranslate
import org.gdal.gdal.{Band, Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants.GF_Write

object KernelFilter {

    def filter(ds: Dataset, kernelSize: Int, operation: String): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val outShortName = ds.GetDriver().getShortName
        val extension = GDAL.getExtension(outShortName)
        val resultFileName = s"/vsimem/kernel_filter_$uuid.$extension"

        // Create a copy via gdal_translate to ensure proper format, compression, etc.
        val (cpyDs, _) = GDALTranslate.executeTranslate(resultFileName, ds, "gdal_translate", Map.empty)
        cpyDs.FlushCache()
        cpyDs.delete()

        val resDs = gdal.Open(resultFileName, GF_Write)

        for (bandIndex <- 1 to ds.GetRasterCount) {
            val inBand = ds.GetRasterBand(bandIndex)
            val outBand = resDs.GetRasterBand(bandIndex)
            filterBand(inBand, outBand, kernelSize, operation)
        }

        val newOptions = Map(
          "path" -> resultFileName,
          "parentPath" -> "",
          "driver" -> outShortName,
          "last_command" -> s"gdal_kernel_filter -k $kernelSize -o $operation",
          "last_error" -> "",
          "all_parents" -> ""
        )

        (resDs, newOptions)
    }

    def filterBand(inBand: Band, outBand: Band, kernelSize: Int, operation: String): Unit = {
        val blockSize = inBand.GetBlockXSize()
        val stride = kernelSize / 2
        val xSize = inBand.GetXSize()
        val ySize = inBand.GetYSize()

        for (yOffset <- 0 until ySize by blockSize) {
            for (xOffset <- 0 until xSize by blockSize) {

                val currentBlock = GDALBlock(
                  inBand,
                  stride,
                  xOffset,
                  yOffset,
                  blockSize,
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

                outBand.WriteRaster(xOffset, yOffset, trimmedResult.width, trimmedResult.height, trimmedResult.block)
                outBand.FlushCache()
                // Commented out due to "ERROR 8: GDALRasterBand::RasterIO(): attempt to write to a nodata implicit mask band."
                // seems that this write was a noop anyways
                // we need to figure a stable way for alpha bands / nodata masks to work with these operations
//                outBand.GetMaskBand().WriteRaster(xOffset, yOffset, trimmedResult.width, trimmedResult.height, trimmedResult.maskBlock)
//                outBand.GetMaskBand().FlushCache()

            }
        }
    }

}
