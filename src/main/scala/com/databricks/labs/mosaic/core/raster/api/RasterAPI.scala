package com.databricks.labs.mosaic.core.raster.api

import org.apache.spark.sql.catalyst.InternalRow
import com.databricks.labs.mosaic.core.raster.{MosaicRaster, MosaicRasterGDAL, RasterReader}
import org.gdal.gdal.gdal

abstract class RasterAPI(reader: RasterReader) extends Serializable {

    def enable(): Unit
    def name: String
    def raster(input: Array[Byte]): MosaicRaster = reader.fromBytes(input)
    def raster(inputData: InternalRow): MosaicRaster = reader.fromBytes(inputData.getBinary(0))
    def raster(inputData: Any): MosaicRaster = reader.fromBytes(inputData.asInstanceOf[Array[Byte]])
    def raster(inputData: Any, inputPath: Any): MosaicRaster = {
        if (inputPath == "") {
            raster(inputData)
        } else {
            reader.fromBytes(inputData.asInstanceOf[Array[Byte]], inputPath.asInstanceOf[String])
        }
    }

}

object RasterAPI extends Serializable {

    def apply(name: String): RasterAPI =
        name match {
            case "GDAL" => GDAL
        }

    def getReader(name: String): RasterReader =
        name match {
            case "GDAL" => MosaicRasterGDAL
        }

    object GDAL extends RasterAPI(MosaicRasterGDAL) {

        override def name: String = "GDAL"

        override def enable(): Unit = if (gdal.GetDriverCount() == 0) gdal.AllRegister()
    }

}
