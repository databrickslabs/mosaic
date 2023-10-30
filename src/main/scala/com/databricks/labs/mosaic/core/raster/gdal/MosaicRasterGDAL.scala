package com.databricks.labs.mosaic.core.raster.gdal

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.io.{RasterCleaner, RasterReader, RasterWriter}
import com.databricks.labs.mosaic.core.raster.operator.clip.RasterClipByVector
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.POLYGON
import com.databricks.labs.mosaic.utils.PathUtils
import org.apache.orc.util.Murmur3
import org.gdal.gdal.gdal.GDALInfo
import org.gdal.gdal.{Dataset, InfoOptions, gdal}
import org.gdal.gdalconst.gdalconstConstants._
import org.gdal.osr
import org.gdal.osr.SpatialReference
import org.locationtech.proj4j.CRSFactory

import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util.{Locale, UUID, Vector => JVector}
import scala.collection.JavaConverters.dictionaryAsScalaMapConverter
import scala.util.Try

/** GDAL implementation of the MosaicRaster trait. */
//noinspection DuplicatedCode
class MosaicRasterGDAL(
    _uuid: Long,
    raster: => Dataset,
    path: String,
    isTemp: Boolean,
    parentPath: String,
    driverShortName: String,
    memSize: Long
) extends RasterWriter
      with RasterCleaner {

    // Factory for creating CRS objects
    protected val crsFactory: CRSFactory = new CRSFactory

    // Only use this with GDAL rasters
    private val wsg84 = new osr.SpatialReference()
    wsg84.ImportFromEPSG(4326)
    wsg84.SetAxisMappingStrategy(osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)

    /**
      * @return
      *   The raster's driver short name.
      */
    def getDriversShortName: String = driverShortName

    /**
      * @return
      *   The raster's path on disk. Usually this is a parent file for the tile.
      */
    def getParentPath: String = parentPath

    /**
      * @return
      *   The diagonal size of a raster.
      */
    def diagSize: Double = math.sqrt(xSize * xSize + ySize * ySize)

    /** @return Returns pixel x size. */
    def pixelXSize: Double = getGeoTransform(1)

    /** @return Returns pixel y size. */
    def pixelYSize: Double = getGeoTransform(5)

    /** @return Returns the origin x coordinate. */
    def originX: Double = getGeoTransform(0)

    /** @return Returns the origin y coordinate. */
    def originY: Double = getGeoTransform(3)

    /** @return Returns the max x coordinate. */
    def xMax: Double = originX + xSize * pixelXSize

    /** @return Returns the max y coordinate. */
    def yMax: Double = originY + ySize * pixelYSize

    /** @return Returns the min x coordinate. */
    def xMin: Double = originX

    /** @return Returns the min y coordinate. */
    def yMin: Double = originY

    /** @return Returns the diagonal size of a pixel. */
    def pixelDiagSize: Double = math.sqrt(pixelXSize * pixelXSize + pixelYSize * pixelYSize)

    /** @return Returns file extension. */
    def getRasterFileExtension: String = getRaster.GetDriver().GetMetadataItem("DMD_EXTENSION")

    /** @return Returns the raster's bands as a Seq. */
    def getBands: Seq[MosaicRasterBandGDAL] = (1 to numBands).map(getBand)

    /**
      * Flushes the cache of the raster. This is needed to ensure that the
      * raster is written to disk. This is needed for operations like
      * RasterProject.
      * @return
      *   Returns the raster object.
      */
    def flushCache(): MosaicRasterGDAL = {
        // Note: Do not wrap GDAL objects into Option
        if (getRaster != null) getRaster.FlushCache()
        this.destroy()
        this.refresh()
    }

    /**
      * Opens a raster from a file system path.
      * @param path
      *   The path to the raster file.
      * @return
      *   A MosaicRaster object.
      */
    def openRaster(path: String): Dataset = {
        MosaicRasterGDAL.openRaster(path, Some(driverShortName))
    }

    /**
      * @return
      *   Returns the raster's metadata as a Map.
      */
    def metadata: Map[String, String] = {
        Option(raster.GetMetadataDomainList())
            .map(_.toArray)
            .map(domain =>
                domain
                    .map(domainName =>
                        Option(raster.GetMetadata_Dict(domainName.toString))
                            .map(_.asScala.toMap.asInstanceOf[Map[String, String]])
                            .getOrElse(Map.empty[String, String])
                    )
                    .reduceOption(_ ++ _)
                    .getOrElse(Map.empty[String, String])
            )
            .getOrElse(Map.empty[String, String])

    }

    /**
      * @return
      *   Returns the raster's subdatasets as a Map.
      */
    def subdatasets: Map[String, String] = {
        val dict = raster.GetMetadata_Dict("SUBDATASETS")
        val subdatasetsMap = Option(dict)
            .map(_.asScala.toMap.asInstanceOf[Map[String, String]])
            .getOrElse(Map.empty[String, String])
        val keys = subdatasetsMap.keySet
        keys.flatMap(key =>
            if (key.toUpperCase(Locale.ROOT).contains("NAME")) {
                val path = subdatasetsMap(key)
                val pieces = path.split(":")
                Seq(
                  key -> pieces.last,
                  s"${pieces.last}_vsimem" -> path,
                  pieces.last -> s"${pieces.head}:$parentPath:${pieces.last}"
                )
            } else Seq(key -> subdatasetsMap(key))
        ).toMap
    }

    /**
      * @return
      *   Returns the raster's SRID. This is the EPSG code of the raster's CRS.
      */
    def SRID: Int = {
        Try(crsFactory.readEpsgFromParameters(proj4String))
            .filter(_ != null)
            .getOrElse("EPSG:0")
            .split(":")
            .last
            .toInt
    }

    /**
      * @return
      *   Returns the raster's proj4 string.
      */
    def proj4String: String = {

        try {
            raster.GetSpatialRef.ExportToProj4
        } catch {
            case _: Any => ""
        }
    }

    /**
      * @param bandId
      *   The band index to read.
      * @return
      *   Returns the raster's band as a MosaicRasterBand object.
      */
    def getBand(bandId: Int): MosaicRasterBandGDAL = {
        if (bandId > 0 && numBands >= bandId) {
            new MosaicRasterBandGDAL(raster.GetRasterBand(bandId), bandId)
        } else {
            throw new ArrayIndexOutOfBoundsException()
        }
    }

    /**
      * @return
      *   Returns the raster's number of bands.
      */
    def numBands: Int = raster.GetRasterCount()

    // noinspection ZeroIndexToHead
    /**
      * @return
      *   Returns the raster's extent as a Seq(xmin, ymin, xmax, ymax).
      */
    def extent: Seq[Double] = {
        val minX = getGeoTransform(0)
        val maxY = getGeoTransform(3)
        val maxX = minX + getGeoTransform(1) * xSize
        val minY = maxY + getGeoTransform(5) * ySize
        Seq(minX, minY, maxX, maxY)
    }

    /**
      * @return
      *   Returns x size of the raster.
      */
    def xSize: Int = raster.GetRasterXSize

    /**
      * @return
      *   Returns y size of the raster.
      */
    def ySize: Int = raster.GetRasterYSize

    /**
      * @return
      *   Returns the raster's geotransform as a Seq.
      */
    def getGeoTransform: Array[Double] = raster.GetGeoTransform()

    /**
      * @return
      *   Underlying GDAL raster object.
      */
    def getRaster: Dataset = this.raster

    /**
      * @return
      *   Returns the raster's spatial reference.
      */
    def spatialRef: SpatialReference = raster.GetSpatialRef()

    /**
      * Applies a function to each band of the raster.
      * @param f
      *   The function to apply.
      * @return
      *   Returns a Seq of the results of the function.
      */
    def transformBands[T](f: => MosaicRasterBandGDAL => T): Seq[T] = for (i <- 1 to numBands) yield f(getBand(i))

    /**
      * @return
      *   Returns MosaicGeometry representing bounding box of the raster.
      */
    def bbox(geometryAPI: GeometryAPI, destCRS: SpatialReference = wsg84): MosaicGeometry = {
        val gt = getGeoTransform

        val sourceCRS = spatialRef
        val transform = new osr.CoordinateTransformation(sourceCRS, destCRS)

        val bbox = geometryAPI.geometry(
          Seq(
            Seq(gt(0), gt(3)),
            Seq(gt(0) + gt(1) * xSize, gt(3)),
            Seq(gt(0) + gt(1) * xSize, gt(3) + gt(5) * ySize),
            Seq(gt(0), gt(3) + gt(5) * ySize)
          ).map(geometryAPI.fromCoords),
          POLYGON
        )

        val geom1 = org.gdal.ogr.ogr.CreateGeometryFromWkb(bbox.toWKB)
        geom1.Transform(transform)

        geometryAPI.geometry(geom1.ExportToWkb(), "WKB")
    }

    /**
      * @return
      *   True if the raster is empty, false otherwise. May be expensive to
      *   compute since it requires reading the raster and computing statistics.
      */
    def isEmpty: Boolean = {
        import org.json4s._
        import org.json4s.jackson.JsonMethods._
        implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

        val vector = new JVector[String]()
        vector.add("-stats")
        vector.add("-json")
        val infoOptions = new InfoOptions(vector)
        val gdalInfo = GDALInfo(raster, infoOptions)
        val json = parse(gdalInfo).extract[Map[String, Any]]

        if (json.contains("STATISTICS_VALID_PERCENT")) {
            json("STATISTICS_VALID_PERCENT").asInstanceOf[Double] == 0.0
        } else if (subdatasets.nonEmpty) {
            false
        } else {
            getBandStats.values.map(_.getOrElse("mean", 0.0)).forall(_ == 0.0)
        }
    }

    /**
      * @return
      *   Returns the raster's path.
      */
    def getPath: String = path

    /**
      * @return
      *   Returns the raster for a given cell ID. Used for tessellation.
      */
    def getRasterForCell(cellID: Long, indexSystem: IndexSystem, geometryAPI: GeometryAPI): MosaicRasterGDAL = {
        val cellGeom = indexSystem.indexToGeometry(cellID, geometryAPI)
        val geomCRS = indexSystem.osrSpatialRef
        RasterClipByVector.clip(this, cellGeom, geomCRS, geometryAPI)
    }

    /**
      * Cleans up the raster driver and references.
      *
      * Unlinks the raster file. After this operation the raster object is no
      * longer usable. To be used as last step in expression after writing to
      * bytes.
      */
    def cleanUp(): Unit = {
        val isInMem = path.contains("/vsimem/")
        val isSubdataset = PathUtils.isSubdataset(path)
        val filePath = if (isSubdataset) PathUtils.fromSubdatasetPath(path) else path
        if (isInMem) {
            // Delete the raster from the virtual file system
            // Note that Unlink is not the same as Delete
            // Unlink may leave PAM residuals
            Try(gdal.GetDriverByName(driverShortName).Delete(path))
            Try(gdal.GetDriverByName(driverShortName).Delete(filePath))
            Try(gdal.Unlink(path))
            Try(gdal.Unlink(filePath))
        }
        if (isTemp) {
            try {
                Try(gdal.GetDriverByName(driverShortName).Delete(path))
                Try(Files.deleteIfExists(Paths.get(path)))
                Try(Files.deleteIfExists(Paths.get(filePath)))
            } catch {
                case _: Throwable => ()
            }
        }
    }

    /**
      * @note
      *   If memory size is -1 this will destroy the raster and you will need to
      *   refresh it to use it again.
      * @return
      *   Returns the amount of memory occupied by the file in bytes.
      */
    def getMemSize: Long = {
        if (memSize == -1) {
            if (PathUtils.isInMemory(path)) {
                val tempPath = PathUtils.createTmpFilePath(this.uuid.toString, GDAL.getExtension(driverShortName))
                writeToPath(tempPath)
                val size = Files.size(Paths.get(tempPath))
                Files.delete(Paths.get(tempPath))
                size
            } else {
                Files.size(Paths.get(path))
            }
        } else {
            memSize
        }

    }

    /**
      * Writes a raster to a file system path. This method disposes of the
      * raster object. If the raster is needed again, load it from the path.
      *
      * @param path
      *   The path to the raster file.
      * @return
      *   A boolean indicating if the write was successful.
      */
    def writeToPath(path: String, dispose: Boolean = true): String = {
        val isInMem = PathUtils.isInMemory(getPath)
        if (isInMem) {
            val driver = raster.GetDriver()
            val ds = driver.CreateCopy(path, this.flushCache().getRaster)
            RasterCleaner.dispose(ds)
        } else {
            Files.copy(Paths.get(getPath), Paths.get(path), StandardCopyOption.REPLACE_EXISTING).toString
        }
        if (dispose) RasterCleaner.dispose()
        path
    }

    /**
      * Writes a raster to a byte array.
      *
      * @return
      *   A byte array containing the raster data.
      */
    def writeToBytes(dispose: Boolean = true): Array[Byte] = {
        if (PathUtils.isInMemory(path)) {
            // Create a temporary directory to store the raster
            // This is needed because Files cannot read from /vsimem/ directly
            val path = PathUtils.createTmpFilePath(uuid.toString, GDAL.getExtension(driverShortName))
            writeToPath(path, dispose)
            val byteArray = Files.readAllBytes(Paths.get(path))
            Files.delete(Paths.get(path))
            byteArray
        } else {
            val byteArray = Files.readAllBytes(Paths.get(path))
            if (dispose) RasterCleaner.dispose(this)
            byteArray
        }
    }

    /**
      * Destroys the raster object. After this operation the raster object is no
      * longer usable. If the raster is needed again, use the refresh method.
      */
    def destroy(): Unit = {
        val raster = getRaster
        if (raster != null) {
            raster.FlushCache()
            raster.delete()
        }
    }

    /**
      * Refreshes the raster object. This is needed after writing to a file
      * system path. GDAL only properly writes to a file system path if the
      * raster object is destroyed. After refresh operation the raster object is
      * usable again.
      */
    def refresh(): MosaicRasterGDAL = {
        new MosaicRasterGDAL(uuid, openRaster(path), path, isTemp, parentPath, driverShortName, memSize)
    }

    /**
      * @return
      *   Returns the raster's UUID.
      */
    def uuid: Long = _uuid

    /**
      * @return
      *   Returns the raster's size.
      */
    def getDimensions: (Int, Int) = (xSize, ySize)

    /**
      * @return
      *   Returns the raster's band statistics.
      */
    def getBandStats: Map[Int, Map[String, Double]] = {
        (1 to numBands)
            .map(i => {
                val band = raster.GetRasterBand(i)
                val min = Array.ofDim[Double](1)
                val max = Array.ofDim[Double](1)
                val mean = Array.ofDim[Double](1)
                val stddev = Array.ofDim[Double](1)
                band.GetStatistics(true, true, min, max, mean, stddev)
                i -> Map(
                  "min" -> min(0),
                  "max" -> max(0),
                  "mean" -> mean(0),
                  "stddev" -> stddev(0)
                )
            })
            .toMap
    }

    /**
      * Creates a temporary copy of the raster on disk on the worker node.
      * @return
      *   Returns the new raster.
      */
    def asTemp: MosaicRasterGDAL = {
        val temp = PathUtils.createTmpFilePath(uuid.toString, GDAL.getExtension(driverShortName))
        writeToPath(temp)
        val ds = openRaster(temp)
        MosaicRasterGDAL(ds, temp, isTemp = true, parentPath, driverShortName, memSize)
    }

    /**
      * @param subsetName
      *   The name of the subdataset to get.
      * @return
      *   Returns the raster's subdataset with given name.
      */
    def getSubdataset(subsetName: String): MosaicRasterGDAL = {
        subdatasets
        val path = Option(raster.GetMetadata_Dict("SUBDATASETS"))
            .map(_.asScala.toMap.asInstanceOf[Map[String, String]])
            .getOrElse(Map.empty[String, String])
            .values
            .find(_.toUpperCase(Locale.ROOT).endsWith(subsetName.toUpperCase(Locale.ROOT)))
            .getOrElse(throw new Exception(s"Subdataset $subsetName not found"))
        val ds = openRaster(path)
        // Avoid costly IO to compute MEM size here
        // It will be available when the raster is serialized for next operation
        // If value is needed then it will be computed when getMemSize is called
        MosaicRasterGDAL(ds, path, isTemp = false, parentPath, driverShortName, -1)
    }

}

//noinspection ZeroIndexToHead
/** Companion object for MosaicRasterGDAL Implements RasterReader APIs */
object MosaicRasterGDAL extends RasterReader {

    /**
      * Opens a raster from a file system path with a given driver.
      * @param driverShortName
      *   The driver short name to use. If None, then GDAL will try to identify
      *   the driver from the file extension
      * @param path
      *   The path to the raster file.
      * @return
      *   A MosaicRaster object.
      */
    def openRaster(path: String, driverShortName: Option[String]): Dataset = {
        driverShortName match {
            case Some(driverShortName) =>
                val drivers = new JVector[String]()
                drivers.add(driverShortName)
                gdal.OpenEx(path, GA_ReadOnly, drivers)
            case None                  => gdal.Open(path, GA_ReadOnly)
        }
    }

    /**
      * Identifies the driver of a raster from a file system path.
      * @param parentPath
      *   The path to the raster file.
      * @return
      *   A string representing the driver short name.
      */
    def indentifyDriver(parentPath: String): String = {
        val isSubdataset = PathUtils.isSubdataset(parentPath)
        val path = PathUtils.getCleanPath(parentPath, parentPath.endsWith(".zip"))
        val readPath =
            if (isSubdataset) PathUtils.getSubdatasetPath(path)
            else PathUtils.getZipPath(path)
        val dataset = gdal.Open(readPath, GA_ReadOnly)
        val driver = dataset.GetDriver()
        val driverShortName = driver.getShortName
        dataset.delete()
        driverShortName
    }

    /**
      * Creates a MosaicRaster object from a GDAL raster object.
      * @param dataset
      *   The GDAL raster object.
      * @param path
      *   The path to the raster file in vsimem or in temp dir.
      * @param isTemp
      *   A boolean indicating if the raster is temporary.
      * @param parentPath
      *   The path to the file of the raster on disk.
      * @param driverShortName
      *   The driver short name of the raster.
      * @param memSize
      *   The size of the raster in memory.
      * @return
      *   A MosaicRaster object.
      */
    def apply(
        dataset: => Dataset,
        path: String,
        isTemp: Boolean,
        parentPath: String,
        driverShortName: String,
        memSize: Long
    ): MosaicRasterGDAL = {
        val uuid = Murmur3.hash64(path.getBytes())
        val raster = new MosaicRasterGDAL(uuid, dataset, path, isTemp, parentPath, driverShortName, memSize)
        raster
    }

    /**
      * Creates a MosaicRaster object from a file system path.
      * @param path
      *   The path to the raster file.
      * @param isTemp
      *   A boolean indicating if the raster is temporary.
      * @param parentPath
      *   The path to the file of the raster on disk.
      * @param driverShortName
      *   The driver short name of the raster.
      * @param memSize
      *   The size of the raster in memory.
      * @return
      *   A MosaicRaster object.
      */
    def apply(path: String, isTemp: Boolean, parentPath: String, driverShortName: String, memSize: Long): MosaicRasterGDAL = {
        val uuid = Murmur3.hash64(path.getBytes())
        val dataset = openRaster(path, Some(driverShortName))
        val raster = new MosaicRasterGDAL(uuid, dataset, path, isTemp, parentPath, driverShortName, memSize)
        raster
    }

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
    override def readRaster(inPath: String, parentPath: String): MosaicRasterGDAL = {
        val isSubdataset = PathUtils.isSubdataset(inPath)
        val localCopy = PathUtils.copyToTmp(inPath)
        val path = PathUtils.getCleanPath(localCopy, localCopy.endsWith(".zip"))
        val uuid = Murmur3.hash64(path.getBytes())
        val readPath =
            if (isSubdataset) PathUtils.getSubdatasetPath(path)
            else PathUtils.getZipPath(path)
        val dataset = openRaster(readPath, None)
        val driverShortName = dataset.GetDriver().getShortName

        // Avoid costly IO to compute MEM size here
        // It will be available when the raster is serialized for next operation
        // If value is needed then it will be computed when getMemSize is called
        // We cannot just use memSize value of the parent due to the fact that the raster could be a subdataset
        val raster = new MosaicRasterGDAL(uuid, dataset, path, true, parentPath, driverShortName, -1)
        raster
    }

    /**
      * Reads a raster from a byte array.
      * @param contentBytes
      *   The byte array containing the raster data.
      * @param driverShortName
      *   The driver short name of the raster.
      * @return
      *   A MosaicRaster object.
      */
    override def readRaster(contentBytes: => Array[Byte], parentPath: String, driverShortName: String): MosaicRasterGDAL = {
        if (Option(contentBytes).isEmpty || contentBytes.isEmpty) {
            new MosaicRasterGDAL(-1L, null, "", false, parentPath, "", -1)
        } else {
            // This is a temp UUID for purposes of reading the raster through GDAL from memory
            // The stable UUID is kept in metadata of the raster
            val uuid = Murmur3.hash64(UUID.randomUUID().toString.getBytes())
            val extension = GDAL.getExtension(driverShortName)
            val virtualPath = s"/vsimem/$uuid.$extension"
            gdal.FileFromMemBuffer(virtualPath, contentBytes)
            // Try reading as a virtual file, if that fails, read as a zipped virtual file
            val dataset = Option(
              openRaster(virtualPath, Some(driverShortName))
            ).getOrElse({
                // Unlink the previous virtual file
                gdal.Unlink(virtualPath)
                // Create a virtual zip file
                val virtualZipPath = s"/vsimem/$uuid.zip"
                val zippedPath = s"/vsizip/$virtualZipPath"
                gdal.FileFromMemBuffer(virtualZipPath, contentBytes)
                openRaster(zippedPath, Some(driverShortName))
            })
            val raster = new MosaicRasterGDAL(uuid, dataset, virtualPath, false, parentPath, driverShortName, contentBytes.length)
            raster
        }
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
    override def readBand(path: String, bandIndex: Int, parentPath: String): MosaicRasterBandGDAL = {
        val raster = readRaster(path, parentPath)
        // TODO: Raster and Band are coupled, this can cause a pointer leak
        raster.getBand(bandIndex)
    }

}
