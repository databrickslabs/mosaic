package com.databricks.labs.mosaic.core.raster

import org.gdal.gdal.Dataset

abstract class MosaicRaster(path: String) extends Serializable {

    def metadata: Map[String, String]

    def subdatasets: Map[String, String]

    def numBands: Int

    def SRID: Int

    def proj4String: String

    def xSize: Int

    def ySize: Int

    def getBand(bandId: Int): MosaicRasterBand

    def extent: Seq[Double]

    def geoTransform(pixel: Int, line: Int): Seq[Double]

    def getRaster: Dataset

    def cleanUp(): Unit

}
