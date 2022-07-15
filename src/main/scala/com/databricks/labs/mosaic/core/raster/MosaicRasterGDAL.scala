package com.databricks.labs.mosaic.core.raster

import com.databricks.labs.mosaic.utils.NativeUtils
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants.GA_ReadOnly
import org.gdal.osr.SpatialReference
import org.locationtech.proj4j.CRSFactory
import scala.util.{Try, Success, Failure}

class MosaicRasterGDAL(raster: Dataset) extends MosaicRaster {
  override def numBands: Int = raster.GetRasterCount()

  override def SRID: Int = {
    val crsFactory = new CRSFactory
    Try(crsFactory.readEpsgFromParameters(proj4String)) match {
      case Success(null) => 0
      case Success(s) if s contains ":" => ":".split(s).last.toInt
      case Failure(_) => 0
    }
  }

  override def proj4String: String = raster.GetSpatialRef.ExportToProj4

  override def xSize: Int = raster.GetRasterXSize

  override def ySize: Int = raster.GetRasterYSize

  override def getBand(bandId: Int): MosaicRasterBand = new MosaicRasterBandGDAL(raster.GetRasterBand(bandId), bandId)

  override def extent: Seq[Double] = {
    val minx = geoTransformArray(0)
    val maxy = geoTransformArray(3)
    val maxx = minx + geoTransformArray(1) * xSize
    val miny = maxy + geoTransformArray(5) * ySize
    Seq(minx, miny, maxx, maxy)
  }

  def spatialRef: SpatialReference = raster.GetSpatialRef()

  def geoTransformArray: Seq[Double] = raster.GetGeoTransform()

  override def geoTransform(pixel: Int, line: Int): Seq[Double] = {
    val Xp = geoTransformArray(0) + pixel * geoTransformArray(1) + line * geoTransformArray(2)
    val Yp = geoTransformArray(3) + pixel * geoTransformArray(4) + line * geoTransformArray(5)
    Array(Xp, Yp)
  }

}

object MosaicRasterGDAL extends RasterReader {

  def apply(raster: Dataset): MosaicRaster = new MosaicRasterGDAL(raster)

  override def fromBytes(bytes: Array[Byte]): MosaicRaster = {
    enableGDAL()
    val virtualPath = s"/vsimem/${java.util.UUID.randomUUID.toString}.TIF"
    gdal.FileFromMemBuffer(virtualPath, bytes)
    val dataset = gdal.Open(virtualPath, GA_ReadOnly)
    MosaicRasterGDAL(dataset)
  }

  private def enableGDAL(): Unit = {
    val libs = Seq(
      "/GDAL.libs/libcrypto-de69073a.so.0.9.8e",
      "/GDAL.libs/libcurl-9bc4ffbf.so.4.7.0",
      "/GDAL.libs/libexpat-c5a39682.so.1.6.11",
      "/GDAL.libs/libgeos--no-undefined-82dbfb1f.so",
      "/GDAL.libs/libgeos_c-d134951e.so.1.14.3",
      "/GDAL.libs/libhdf5-13db72d8.so.200.0.0",
      "/GDAL.libs/libhdf5_hl-76c8603c.so.200.0.0",
      "/GDAL.libs/libjasper-21c09ccf.so.1.0.0",
      "/GDAL.libs/libjson-c-ca0558d5.so.2.0.1",
      "/GDAL.libs/libnetcdf-f68a7300.so.13.1.1",
      "/GDAL.libs/libopenjp2-f1d08cc2.so.2.3.1",
      "/GDAL.libs/libsqlite3-13a07f98.so.0.8.6",
      "/GDAL.libs/libtiff-d64010d8.so.5.5.0",
      "/GDAL.libs/libwebp-25902a0b.so.7.1.0",
      "/GDAL.libs/libproj-268837f3.so.22.2.1",
      "/GDAL.libs/libgdal-39073f84.so.30.0.1",
      "/GDAL.libs/libgdalalljni.so",
    )
    libs.foreach(NativeUtils.loadLibraryFromJar)
    gdal.AllRegister()
  }
}