package com.databricks.labs.mosaic.core.raster

import scala.collection.JavaConverters.dictionaryAsScalaMapConverter
import scala.util.Try

import org.gdal.gdal.{gdal, Dataset}
import org.gdal.gdalconst.gdalconstConstants.GA_ReadOnly
import org.gdal.osr.SpatialReference
import org.locationtech.proj4j.CRSFactory

class MosaicRasterGDAL(raster: Dataset) extends MosaicRaster {

    val crsFactory: CRSFactory = new CRSFactory

    override def metadata: Map[String, String] = raster.GetMetadata_Dict.asScala.toMap.asInstanceOf[Map[String, String]]

    override def subdatasets: Map[String, String] =
        raster
            .GetMetadata_List("SUBDATASETS")
            .toArray
            .map(_.toString)
            .grouped(2)
            .map({ case Array(p, d) => (p.split("=").last, d.split("=").last) })
            .toMap

    override def SRID: Int = {
        Try(crsFactory.readEpsgFromParameters(proj4String))
            .filter(_ != null)
            .getOrElse("EPSG:0")
            .split(":")
            .last
            .toInt
    }

    override def proj4String: String = Try(raster.GetSpatialRef.ExportToProj4).filter(_ != null).getOrElse("")

    override def getBand(bandId: Int): MosaicRasterBand = {
        if (bandId > 0 && numBands >= bandId) {
            new MosaicRasterBandGDAL(raster.GetRasterBand(bandId), bandId)
        } else {
            throw new ArrayIndexOutOfBoundsException()
        }
    }

    override def numBands: Int = raster.GetRasterCount()

    override def extent: Seq[Double] = {
        val minx = geoTransformArray(0)
        val maxy = geoTransformArray(3)
        val maxx = minx + geoTransformArray(1) * xSize
        val miny = maxy + geoTransformArray(5) * ySize
        Seq(minx, miny, maxx, maxy)
    }

    override def xSize: Int = raster.GetRasterXSize

    override def ySize: Int = raster.GetRasterYSize

    def geoTransformArray: Seq[Double] = raster.GetGeoTransform()

    override def getRaster: Dataset = this.raster

    override def geoTransform(pixel: Int, line: Int): Seq[Double] = {
        val Xp = geoTransformArray(0) + pixel * geoTransformArray(1) + line * geoTransformArray(2)
        val Yp = geoTransformArray(3) + pixel * geoTransformArray(4) + line * geoTransformArray(5)
        Array(Xp, Yp)
    }

    def spatialRef: SpatialReference = raster.GetSpatialRef()

}

object MosaicRasterGDAL extends RasterReader {

    override def fromBytes(bytes: Array[Byte]): MosaicRaster = {
        val virtualPath = s"/vsimem/${java.util.UUID.randomUUID.toString}"
        fromBytes(bytes, virtualPath)
    }

    def fromBytes(bytes: Array[Byte], path: String): MosaicRaster = {
        enableGDAL()
        gdal.FileFromMemBuffer(path, bytes)
        val dataset = gdal.Open(path, GA_ReadOnly)
        MosaicRasterGDAL(dataset)
    }

    def apply(raster: Dataset): MosaicRaster = new MosaicRasterGDAL(raster)

    private def enableGDAL(): Unit = {
        System.getProperty("os.name").toLowerCase() match {
            case o: String if o.contains("nux") => System.load("/usr/lib/jni/libgdalalljni.so")
            case _                              => //throw new UnsupportedOperationException("This method is only enabled on Ubuntu Linux.")
        }
        gdal.AllRegister()
    }

}
