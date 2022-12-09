package com.databricks.labs.mosaic.core.raster

import scala.collection.JavaConverters.dictionaryAsScalaMapConverter
import scala.util.Try
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants.GA_ReadOnly
import org.gdal.osr.SpatialReference
import org.locationtech.proj4j.CRSFactory

import java.util
import scala.util.hashing.MurmurHash3

case class MosaicRasterGDAL(raster: Dataset, path: String) extends MosaicRaster(path) {

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

    // noinspection ZeroIndexToHead
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

    // noinspection ZeroIndexToHead
    override def geoTransform(pixel: Int, line: Int): Seq[Double] = {
        val Xp = geoTransformArray(0) + pixel * geoTransformArray(1) + line * geoTransformArray(2)
        val Yp = geoTransformArray(3) + pixel * geoTransformArray(4) + line * geoTransformArray(5)
        Array(Xp, Yp)
    }

    def spatialRef: SpatialReference = raster.GetSpatialRef()

    override def cleanUp(): Unit = {
        raster.delete()
        gdal.Unlink(path)
    }
}

object MosaicRasterGDAL extends RasterReader {

    override def fromBytes(bytes: Array[Byte]): MosaicRaster = {
        // We use both the hash code and murmur hash
        // to ensure that the hash is unique.
        val hashCode = util.Arrays.hashCode(bytes)
        val murmur = MurmurHash3.arrayHash(bytes)
        val virtualPath = s"/vsimem/$hashCode/$murmur"
        fromBytes(bytes, virtualPath)
    }

    def fromBytes(bytes: Array[Byte], path: String): MosaicRaster = {
        gdal.FileFromMemBuffer(path, bytes)
        val dataset = gdal.Open(path, GA_ReadOnly)
        MosaicRasterGDAL(dataset, path)
    }

    def apply(dataset: Dataset, path: String) = new MosaicRasterGDAL(dataset, path)

}