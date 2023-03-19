package com.databricks.labs.mosaic.core.raster

import org.gdal.gdal.{gdal, Dataset}
import org.gdal.gdalconst.gdalconstConstants._
import org.gdal.osr.SpatialReference
import org.locationtech.proj4j.CRSFactory

import java.io.File
import java.nio.file.{Files, Paths}
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.util.Locale
import scala.collection.JavaConverters.dictionaryAsScalaMapConverter
import scala.util.Try

/** GDAL implementation of the MosaicRaster trait. */
//noinspection DuplicatedCode
case class MosaicRasterGDAL(raster: Dataset, path: String, memSize: Long) extends MosaicRaster(path, memSize) {

    import com.databricks.labs.mosaic.core.raster.MosaicRasterGDAL.toWorldCoord

    val crsFactory: CRSFactory = new CRSFactory

    override def metadata: Map[String, String] = {
        Option(raster.GetMetadataDomainList())
            .map(_.toArray)
            .map(domain =>
                domain
                    .map(domainName =>
                        Option(raster.GetMetadata_Dict(domainName.toString))
                            .map(_.asScala.toMap.asInstanceOf[Map[String, String]])
                            .getOrElse(Map.empty[String, String])
                    )
                    .reduceOption(_ ++ _).getOrElse(Map.empty[String, String])
            )
            .getOrElse(Map.empty[String, String])

    }

    override def subdatasets: Map[String, String] = {
        val subdatasetsMap = Option(raster.GetMetadata_Dict("SUBDATASETS"))
            .map(_.asScala.toMap.asInstanceOf[Map[String, String]])
            .getOrElse(Map.empty[String, String])
        val keys = subdatasetsMap.keySet
        keys.flatMap(key =>
            if (key.toUpperCase(Locale.ROOT).contains("NAME")) {
                val path = subdatasetsMap(key)
                Seq(
                  key -> path.split(":").last,
                  path.split(":").last -> path
                )
            } else Seq(key -> subdatasetsMap(key))
        ).toMap
    }

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
            MosaicRasterBandGDAL(raster.GetRasterBand(bandId), bandId)
        } else {
            throw new ArrayIndexOutOfBoundsException()
        }
    }

    override def numBands: Int = raster.GetRasterCount()

    // noinspection ZeroIndexToHead
    override def extent: Seq[Double] = {
        val minx = getGeoTransform(0)
        val maxy = getGeoTransform(3)
        val maxx = minx + getGeoTransform(1) * xSize
        val miny = maxy + getGeoTransform(5) * ySize
        Seq(minx, miny, maxx, maxy)
    }

    override def xSize: Int = raster.GetRasterXSize

    override def ySize: Int = raster.GetRasterYSize

    def getGeoTransform: Array[Double] = raster.GetGeoTransform()

    def getGeoTransform(extent: (Int, Int, Int, Int)): Array[Double] = {
        val gt = getGeoTransform
        val (xmin, _, _, ymax) = extent
        val (xUpperLeft, yUpperLeft) = toWorldCoord(gt, xmin, ymax)
        Array(xUpperLeft, gt(1), gt(2), yUpperLeft, gt(4), gt(5))
    }

    override def getRaster: Dataset = this.raster

    def spatialRef: SpatialReference = raster.GetSpatialRef()

    override def cleanUp(): Unit = {

        /** Nothing to clean up = NOOP */
    }

    override def transformBands[T](f: MosaicRasterBand => T): Seq[T] = for (i <- 1 to numBands) yield f(getBand(i))

    /**
      * Write the raster to a file. GDAL cannot write directly to dbfs. Raster
      * is written to a local file first. "../../tmp/_" is used for the
      * temporary file. The file is then copied to the checkpoint directory. The
      * local copy is then deleted. Temporary files are written as GeoTiffs.
      * Files with subdatasets are not supported. They should be flattened
      * first.
      *
      * @param stageId
      *   the UUI of the computation stage generating the raster. Used to avoid
      *   writing collisions.
      * @param rasterId
      *   the UUID of the raster. Used to avoid writing collisions.
      * @param extent
      *   the extent to clip the raster to. This is used for writing out partial
      *   rasters.
      * @param checkpointPath
      *   the path to the checkpoint directory.
      * @return
      *   A path to the written raster.
      */
    override def saveCheckpoint(stageId: String, rasterId: Long, extent: (Int, Int, Int, Int), checkpointPath: String): String = {
        val tmpDir = Files.createTempDirectory(s"mosaic_$stageId").toFile.getAbsolutePath
        val outPath = s"$tmpDir/raster_${rasterId.toString.replace("-", "_")}.tif"
        Files.createDirectories(Paths.get(outPath).getParent)
        val (xmin, ymin, xmax, ymax) = extent
        val xSize = xmax - xmin
        val ySize = ymax - ymin
        val outputDs = gdal.GetDriverByName("GTiff").Create(outPath, xSize, ySize, numBands, GDT_Float64)
        for (i <- 1 to numBands) {
            val band = getBand(i)
            val data = band.values(xmin, ymin, xSize, ySize)
            val maskData = band.maskValues(xmin, ymin, xSize, ySize)
            val noDataValue = band.noDataValue

            val outBand = outputDs.GetRasterBand(i)
            val maskBand = outBand.GetMaskBand()

            outBand.SetNoDataValue(noDataValue)
            outBand.WriteRaster(0, 0, xSize, ySize, data)
            maskBand.WriteRaster(0, 0, xSize, ySize, maskData)
            outBand.FlushCache()
            maskBand.FlushCache()
        }
        outputDs.SetGeoTransform(getGeoTransform(extent))
        outputDs.FlushCache()

        val destinationPath = Paths.get(checkpointPath.replace("dbfs:/", "/dbfs/"), s"raster_$rasterId.tif")
        Files.createDirectories(destinationPath)
        Files.copy(Paths.get(outPath), destinationPath, REPLACE_EXISTING)
        Files.delete(Paths.get(outPath))
        destinationPath.toAbsolutePath.toString.replace("dbfs:/", "/dbfs/")
    }

}

//noinspection ZeroIndexToHead
object MosaicRasterGDAL extends RasterReader {

    def apply(dataset: Dataset, path: String, memSize: Long): MosaicRasterGDAL = new MosaicRasterGDAL(dataset, path, memSize)

    /**
      * Reads a raster from a file system path. Reads a subdataset if the path
      * is to a subdataset.
      *
      * @example
      *   Raster: path = "file:///path/to/file.tif" Subdataset: path =
      *   "file:///path/to/file.tif:subdataset"
      * @param inPath
      *   The path to the raster file.
      * @return
      *   A MosaicRaster object.
      */
    override def readRaster(inPath: String): MosaicRaster = {
        val path = inPath.replace("dbfs:/", "/dbfs/").replace("file:/", "/")
        val dataset = gdal.Open(path, GA_ReadOnly)
        val size = new File(path).length()
        MosaicRasterGDAL(dataset, path, size)
    }

    /**
      * Reads a raster band from a file system path. Reads a subdataset band if
      * the path is to a subdataset.
      *
      * @example
      *   Raster: path = "file:///path/to/file.tif" Subdataset: path =
      *   "file:///path/to/file.tif:subdataset"
      * @param path
      *   The path to the raster file.
      * @param bandIndex
      *   The band index to read.
      * @return
      *   A MosaicRaster object.
      */
    override def readBand(path: String, bandIndex: Int): MosaicRasterBand = {
        val raster = readRaster(path)
        raster.getBand(bandIndex)
    }

    /**
      * Take a geo transform matrix and x and y coordinates of a pixel and
      * returns the x and y coors in the projection of the raster. As per GDAL
      * documentation, the origin is the top left corner of the top left pixel
      * @see
      *   https://gdal.org/tutorials/raster_api_tut.html
      *
      * @param geoTransform
      *   The geo transform matrix of the raster.
      *
      * @param x
      *   The x coordinate of the pixel.
      * @param y
      *   The y coordinate of the pixel.
      * @return
      *   A tuple of doubles with the x and y coordinates in the projection of
      *   the raster.
      */
    override def toWorldCoord(geoTransform: Seq[Double], x: Int, y: Int): (Double, Double) = {
        val Xp = geoTransform(0) + x * geoTransform(1) + y * geoTransform(2)
        val Yp = geoTransform(3) + x * geoTransform(4) + y * geoTransform(5)
        (Xp, Yp)
    }

    /**
      * Take a geo transform matrix and x and y coordinates of a point and
      * returns the x and y coordinates of the raster pixel.
      * @see
      *   // Reference:
      *   https://gis.stackexchange.com/questions/221292/retrieve-pixel-value-with-geographic-coordinate-as-input-with-gdal
      *
      * @param geoTransform
      *   The geo transform matrix of the raster.
      * @param xGeo
      *   The x coordinate of the point.
      * @param yGeo
      *   The y coordinate of the point.
      * @return
      *   A tuple of integers with the x and y coordinates of the raster pixel.
      */
    override def fromWorldCoord(geoTransform: Seq[Double], xGeo: Double, yGeo: Double): (Int, Int) = {
        val x = ((xGeo - geoTransform(0)) / geoTransform(1)).toInt
        val y = ((yGeo - geoTransform(3)) / geoTransform(5)).toInt
        (x, y)
    }

}
