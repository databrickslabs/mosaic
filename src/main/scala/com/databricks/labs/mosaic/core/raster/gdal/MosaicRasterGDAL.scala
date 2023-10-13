package com.databricks.labs.mosaic.core.raster.gdal

import com.databricks.labs.mosaic.GDAL
import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster._
import com.databricks.labs.mosaic.core.raster.io.{RasterCleaner, RasterReader}
import com.databricks.labs.mosaic.core.raster.operator.clip.RasterClipByVector
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.POLYGON
import com.databricks.labs.mosaic.utils.PathUtils
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
    var raster: Dataset,
    path: String,
    isTemp: Boolean,
    parentPath: String,
    driverShortName: String,
    memSize: Long
) extends MosaicRaster(isInMem = false, parentPath, driverShortName) {

    protected val crsFactory: CRSFactory = new CRSFactory

    // Only use this with GDAL rasters
    private val wsg84 = new osr.SpatialReference()
    wsg84.ImportFromEPSG(4326)
    wsg84.SetAxisMappingStrategy(osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)

    def openRaster(path: String): Dataset = {
        MosaicRasterGDAL.openRaster(path, driverShortName)
    }

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
                    .reduceOption(_ ++ _)
                    .getOrElse(Map.empty[String, String])
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
                val pieces = path.split(":")
                Seq(
                  key -> pieces.last,
                  s"${pieces.last}_vsimem" -> path,
                  pieces.last -> s"${pieces.head}:$parentPath:${pieces.last}"
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
        val minX = getGeoTransform(0)
        val maxY = getGeoTransform(3)
        val maxX = minX + getGeoTransform(1) * xSize
        val minY = maxY + getGeoTransform(5) * ySize
        Seq(minX, minY, maxX, maxY)
    }

    override def xSize: Int = raster.GetRasterXSize

    override def ySize: Int = raster.GetRasterYSize

    override def getGeoTransform: Array[Double] = raster.GetGeoTransform()

    override def getRaster: Dataset = this.raster

    def spatialRef: SpatialReference = raster.GetSpatialRef()

    override def transformBands[T](f: MosaicRasterBand => T): Seq[T] = for (i <- 1 to numBands) yield f(getBand(i))

    /**
      * @return
      *   Returns MosaicGeometry representing bounding box of the raster.
      */
    override def bbox(geometryAPI: GeometryAPI, destCRS: SpatialReference = wsg84): MosaicGeometry = {
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

    override def isEmpty: Boolean = {
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
        } else {
            false
        }
    }

    override def getPath: String = path

    override def getRasterForCell(cellID: Long, indexSystem: IndexSystem, geometryAPI: GeometryAPI): MosaicRaster = {
        val cellGeom = indexSystem.indexToGeometry(cellID, geometryAPI)
        // buffer by diagonal size of the raster pixel to avoid clipping issues
        // add 1% to avoid rounding errors
        val bufferR = pixelDiagSize * 1.01
        val bufferedCell = cellGeom.buffer(bufferR)
        val geomCRS =
            if (cellGeom.getSpatialReference == 0) wsg84
            else {
                val geomCRS = new SpatialReference()
                geomCRS.ImportFromEPSG(cellGeom.getSpatialReference)
                // debug for this
                geomCRS.SetAxisMappingStrategy(osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
                geomCRS
            }
        RasterClipByVector.clip(this, bufferedCell, geomCRS, geometryAPI)
    }

    /**
      * Cleans up the raster driver and references.
      *
      * Unlinks the raster file. After this operation the raster object is no
      * longer usable. To be used as last step in expression after writing to
      * bytes.
      */
    override def cleanUp(): Unit = {
        val isInMem = path.startsWith("/vsimem/")
        if (isInMem) {
            gdal.Unlink(path)
        }
        if (isTemp) {
            try {
                Files.delete(Paths.get(path))
            } catch {
                case _: Throwable => ()
            }
        }
    }

    /** @return Returns the amount of memory occupied by the file in bytes. */
    override def getMemSize: Long = {
        if (memSize == -1) {
            if (PathUtils.isInMemory(path)) {
                val tempPath = PathUtils.createTmpFilePath(getExtension)
                writeToPath(tempPath)
                this.refresh()
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
      * Writes a raster to a file system path. This method destroys the raster
      * object. In order to use the raster object after this operation, use the
      * refresh method first.
      *
      * @param path
      *   The path to the raster file.
      * @return
      *   A boolean indicating if the write was successful.
      */
    override def writeToPath(path: String, destroy: Boolean = true): String = {
        val isInMem = PathUtils.isInMemory(getPath)
        if (isInMem) {
            val driver = raster.GetDriver()
            this.flushCache()
            val ds = driver.CreateCopy(path, raster)
            RasterCleaner.destroy(ds)
        } else {
            Files.copy(Paths.get(getPath), Paths.get(path), StandardCopyOption.REPLACE_EXISTING).toString
        }
        if (destroy) this.destroy()
        path
    }

    /**
      * Writes a raster to a byte array.
      *
      * @return
      *   A byte array containing the raster data.
      */
    override def writeToBytes(destroy: Boolean = true): Array[Byte] = {
        if (PathUtils.isInMemory(path)) {
            // Create a temporary directory to store the raster
            // This is needed because Files cannot read from /vsimem/ directly
            val path = PathUtils.createTmpFilePath(getExtension)
            writeToPath(path, destroy)
            val byteArray = Files.readAllBytes(Paths.get(path))
            Files.delete(Paths.get(path))
            byteArray
        } else {
            val byteArray = Files.readAllBytes(Paths.get(path))
            byteArray
        }
    }

    override def destroy(): Unit = {
        val raster = getRaster
        if (raster != null) {
            raster.FlushCache()
            raster.delete()
            this.raster = null
        }
    }

    /**
      * Refreshes the raster object. This is needed after writing to a file
      * system path. GDAL only properly writes to a file system path if the
      * raster object is destroyed. After refresh operation the raster object is
      * usable again.
      */
    override def refresh(): Unit = {
        this.raster = openRaster(path)
    }

    override def getExtension: String = {
        val driver = gdal.GetDriverByName(driverShortName)
        val extension = driver.GetMetadataItem("DMD_EXTENSION")
        extension
    }

    override def getDimensions: (Int, Int) = (xSize, ySize)

    override def getBandStats: Map[Int, Map[String, Double]] = {
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

    override def asTemp: MosaicRaster = {
        val temp = PathUtils.createTmpFilePath(getExtension)
        writeToPath(temp)
        if (PathUtils.isInMemory(path)) RasterCleaner.dispose(this)
        else this.destroy()
        val ds = openRaster(temp)
        MosaicRasterGDAL(ds, temp, isTemp = true, parentPath, driverShortName, memSize)
    }

    override def getSubdataset(subsetName: String): MosaicRaster = {
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
object MosaicRasterGDAL extends RasterReader {

    def openRaster(path: String, driverShortName: String): Dataset = {
        val drivers = new JVector[String]()
        drivers.add(driverShortName)
        gdal.OpenEx(path, GA_ReadOnly, drivers)
    }

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
        driver.delete()
        driverShortName
    }

    def apply(
        dataset: Dataset,
        path: String,
        isTemp: Boolean,
        parentPath: String,
        driverShortName: String,
        memSize: Long
    ): MosaicRasterGDAL = {
        val raster = new MosaicRasterGDAL(dataset, path, isTemp, parentPath, driverShortName, memSize)
        raster
    }

    def apply(path: String, isTemp: Boolean, parentPath: String, driverShortName: String, memSize: Long): MosaicRasterGDAL = {
        val dataset = openRaster(path, driverShortName)
        val raster = new MosaicRasterGDAL(dataset, path, isTemp, parentPath, driverShortName, memSize)
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
    override def readRaster(inPath: String, parentPath: String, driverShortName: String, readDirect: Boolean = false): MosaicRaster = {
        val isSubdataset = PathUtils.isSubdataset(inPath)
        val localCopy = if (readDirect) inPath else PathUtils.copyToTmp(inPath)
        val path = PathUtils.getCleanPath(localCopy, localCopy.endsWith(".zip"))
        val readPath =
            if (isSubdataset) PathUtils.getSubdatasetPath(path)
            else PathUtils.getZipPath(path)
        val dataset = openRaster(readPath, driverShortName)

        // Avoid costly IO to compute MEM size here
        // It will be available when the raster is serialized for next operation
        // If value is needed then it will be computed when getMemSize is called
        // We cannot just use memSize value of the parent due to the fact that the raster could be a subdataset
        val raster = new MosaicRasterGDAL(dataset, path, true, parentPath, driverShortName, -1)
        raster
    }

    override def readRaster(contentBytes: Array[Byte], parentPath: String, driverShortName: String): MosaicRaster = {
        if (Option(contentBytes).isEmpty || contentBytes.isEmpty) {
            new MosaicRasterGDAL(null, "", false, parentPath, "", -1)
        } else {
            // This is a temp UUID for purposes of reading the raster through GDAL from memory
            // The stable UUID is kept in metadata of the raster
            val uuid = UUID.randomUUID().toString
            val extension = GDAL.getExtension(driverShortName)
            val virtualPath = s"/vsimem/$uuid.$extension"
            gdal.FileFromMemBuffer(virtualPath, contentBytes)
            // Try reading as a virtual file, if that fails, read as a zipped virtual file
            val dataset = Option(
              openRaster(virtualPath, driverShortName)
            ).getOrElse({
                val virtualPath = s"/vsimem/$uuid.zip"
                val zippedPath = s"/vsizip/$virtualPath"
                gdal.FileFromMemBuffer(virtualPath, contentBytes)
                openRaster(zippedPath, driverShortName)
            })
            val raster = new MosaicRasterGDAL(dataset, virtualPath, false, parentPath, driverShortName, contentBytes.length)
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
    override def readBand(path: String, bandIndex: Int, parentPath: String, driverShortName: String): MosaicRasterBand = {
        val raster = readRaster(path, parentPath, driverShortName)
        raster.getBand(bandIndex)
    }

}
