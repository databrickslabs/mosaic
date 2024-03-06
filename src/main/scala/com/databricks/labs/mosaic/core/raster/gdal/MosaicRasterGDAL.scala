package com.databricks.labs.mosaic.core.raster.gdal

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL.readRaster
import com.databricks.labs.mosaic.core.raster.io.RasterCleaner.dispose
import com.databricks.labs.mosaic.core.raster.io.{RasterCleaner, RasterReader, RasterWriter}
import com.databricks.labs.mosaic.core.raster.operator.clip.RasterClipByVector
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.POLYGON
import com.databricks.labs.mosaic.gdal.MosaicGDAL
import com.databricks.labs.mosaic.utils.{FileUtils, PathUtils, SysUtils}
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants._
import org.gdal.osr
import org.gdal.osr.SpatialReference
import org.locationtech.proj4j.CRSFactory

import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util.{Locale, Vector => JVector}
import scala.collection.JavaConverters.dictionaryAsScalaMapConverter
import scala.util.{Failure, Success, Try}

/** GDAL implementation of the MosaicRaster trait. */
//noinspection DuplicatedCode
case class MosaicRasterGDAL(
    raster: Dataset,
    createInfo: Map[String, String],
    memSize: Long
) extends RasterWriter
      with RasterCleaner {

    def path: String = createInfo("path")

    def parentPath: String = createInfo("parentPath")

    def driverShortName: Option[String] = createInfo.get("driver")

    def getWriteOptions: MosaicRasterWriteOptions = MosaicRasterWriteOptions(this)

    def getCompression: String = {
        val compression = Option(raster.GetMetadata_Dict("IMAGE_STRUCTURE"))
            .map(_.asScala.toMap.asInstanceOf[Map[String, String]])
            .getOrElse(Map.empty[String, String])
            .getOrElse("COMPRESSION", "NONE")
        compression
    }

    def getSpatialReference: SpatialReference = {
        val spatialRef =
            if (raster != null) {
                raster.GetSpatialRef
            } else {
                val tmp = refresh()
                val result = tmp.raster.GetSpatialRef
                dispose(tmp)
                result
            }
        if (spatialRef == null) {
            MosaicGDAL.WSG84
        } else {
            spatialRef
        }
    }

    def isSubDataset: Boolean = {
        val isSubdataset = PathUtils.isSubdataset(path)
        isSubdataset
    }

    // Factory for creating CRS objects
    protected val crsFactory: CRSFactory = new CRSFactory

    /**
      * @return
      *   The raster's driver short name.
      */
    def getDriversShortName: String =
        driverShortName.getOrElse(
          Try(raster.GetDriver().getShortName).getOrElse("NONE")
        )

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
    def getRasterFileExtension: String = GDAL.getExtension(getDriversShortName)

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
        MosaicRasterGDAL.openRaster(path, driverShortName)
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
        val dict = Try(raster.GetMetadata_Dict("SUBDATASETS"))
            .getOrElse(new java.util.Hashtable[String, String]())
        val subdatasetsMap = Option(dict)
            .map(_.asScala.toMap.asInstanceOf[Map[String, String]])
            .getOrElse(Map.empty[String, String])
        val keys = subdatasetsMap.keySet
        val sanitizedParentPath = PathUtils.getCleanPath(parentPath)
        keys.flatMap(key =>
            if (key.toUpperCase(Locale.ROOT).contains("NAME")) {
                val path = subdatasetsMap(key)
                val pieces = path.split(":")
                Seq(
                  key -> pieces.last,
                  s"${pieces.last}_tmp" -> path,
                  pieces.last -> s"${pieces.head}:$sanitizedParentPath:${pieces.last}"
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
      *   Sets the raster's SRID. This is the EPSG code of the raster's CRS.
      */
    def setSRID(srid: Int): MosaicRasterGDAL = {
        val srs = new osr.SpatialReference()
        srs.ImportFromEPSG(srid)
        raster.SetSpatialRef(srs)
        val driver = raster.GetDriver()
        val newPath = PathUtils.createTmpFilePath(GDAL.getExtension(getDriversShortName))
        driver.CreateCopy(newPath, raster)
        val newRaster = MosaicRasterGDAL.openRaster(newPath, driverShortName)
        dispose(this)
        val createInfo = Map(
          "path" -> newPath,
          "parentPath" -> parentPath,
          "driver" -> getDriversShortName
        )
        MosaicRasterGDAL(newRaster, createInfo, -1)
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
            MosaicRasterBandGDAL(raster.GetRasterBand(bandId), bandId)
        } else {
            throw new ArrayIndexOutOfBoundsException()
        }
    }

    /**
      * @return
      *   Returns the raster's number of bands.
      */
    def numBands: Int = {
        val bandCount = Try(raster.GetRasterCount())
        bandCount match {
            case Success(value) => value
            case Failure(_)     => 0
        }
    }

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
      * Applies a function to each band of the raster.
      * @param f
      *   The function to apply.
      * @return
      *   Returns a Seq of the results of the function.
      */
    def transformBands[T](f: MosaicRasterBandGDAL => T): Seq[T] = for (i <- 1 to numBands) yield f(getBand(i))

    /**
      * @return
      *   Returns MosaicGeometry representing bounding box of the raster.
      */
    def bbox(geometryAPI: GeometryAPI, destCRS: SpatialReference = MosaicGDAL.WSG84): MosaicGeometry = {
        val gt = getGeoTransform

        val sourceCRS = getSpatialReference
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
        val bands = getBands
        if (bands.isEmpty) {
            subdatasets.values
                .filter(_.toLowerCase(Locale.ROOT).startsWith(getDriversShortName.toLowerCase(Locale.ROOT)))
                .flatMap(bp => readRaster(createInfo + ("path" -> bp)).getBands)
                .takeWhile(_.isEmpty)
                .nonEmpty
        } else {
            bands.takeWhile(_.isEmpty).nonEmpty
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
        val isSubdataset = PathUtils.isSubdataset(path)
        val filePath = if (isSubdataset) PathUtils.fromSubdatasetPath(path) else path
        val pamFilePath = s"$filePath.aux.xml"
        val cleanPath = filePath.replace("/vsizip/", "")
        val zipPath = if (cleanPath.endsWith("zip")) cleanPath else s"$cleanPath.zip"
        if (path != PathUtils.getCleanPath(parentPath)) {
            Try(gdal.GetDriverByName(getDriversShortName).Delete(path))
            Try(Files.deleteIfExists(Paths.get(cleanPath)))
            Try(Files.deleteIfExists(Paths.get(path)))
            Try(Files.deleteIfExists(Paths.get(filePath)))
            Try(Files.deleteIfExists(Paths.get(pamFilePath)))
            if (Files.exists(Paths.get(zipPath))) {
                Try(Files.deleteIfExists(Paths.get(zipPath.replace(".zip", ""))))
            }
            Try(Files.deleteIfExists(Paths.get(zipPath)))
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
            val toRead = if (path.startsWith("/vsizip/")) path.replace("/vsizip/", "") else path
            if (Files.notExists(Paths.get(toRead))) {
                throw new Exception(s"File not found: ${gdal.GetLastErrorMsg()}")
            }
            Files.size(Paths.get(toRead))
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
        if (isSubDataset) {
            val driver = raster.GetDriver()
            val ds = driver.CreateCopy(path, this.flushCache().getRaster, 1)
            if (ds == null) {
                val error = gdal.GetLastErrorMsg()
                throw new Exception(s"Error writing raster to path: $error")
            }
            ds.FlushCache()
            ds.delete()
            if (dispose) RasterCleaner.dispose(this)
            path
        } else {
            val thisPath = Paths.get(this.path)
            val fromDir = thisPath.getParent
            val toDir = Paths.get(path).getParent
            val stemRegex = PathUtils.getStemRegex(this.path)
            PathUtils.wildcardCopy(fromDir.toString, toDir.toString, stemRegex)
            if (dispose) RasterCleaner.dispose(this)
            s"$toDir/${thisPath.getFileName}"
        }
    }

    /**
      * Writes a raster to a byte array.
      *
      * @return
      *   A byte array containing the raster data.
      */
    def writeToBytes(dispose: Boolean = true): Array[Byte] = {
        val isSubdataset = PathUtils.isSubdataset(path)
        val readPath = {
            val tmpPath =
                if (isSubdataset) {
                    val tmpPath = PathUtils.createTmpFilePath(getRasterFileExtension)
                    writeToPath(tmpPath, dispose = false)
                    tmpPath
                } else {
                    path
                }
            if (Files.isDirectory(Paths.get(tmpPath))) {
                val parentDir = Paths.get(tmpPath).getParent.toString
                val fileName = Paths.get(tmpPath).getFileName.toString
                val prompt = SysUtils.runScript(Array("/bin/sh", "-c", s"cd $parentDir && zip -r0 $fileName.zip $fileName"))
                if (prompt._3.nonEmpty) throw new Exception(s"Error zipping file: ${prompt._3}. Please verify that zip is installed. Run 'apt install zip'.")
                s"$tmpPath.zip"
            } else {
                tmpPath
            }
        }
        val byteArray = FileUtils.readBytes(readPath)
        if (dispose) RasterCleaner.dispose(this)
        if (readPath != PathUtils.getCleanPath(parentPath)) {
            Files.deleteIfExists(Paths.get(readPath))
            if (readPath.endsWith(".zip")) {
                val nonZipPath = readPath.replace(".zip", "")
                if (Files.isDirectory(Paths.get(nonZipPath))) {
                    SysUtils.runCommand(s"rm -rf $nonZipPath")
                }
                Files.deleteIfExists(Paths.get(readPath.replace(".zip", "")))
            }
        }
        byteArray
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
        MosaicRasterGDAL(openRaster(path), createInfo, memSize)
    }

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
      * @return
      *   Returns the raster's band valid pixel count.
      */
    def getValidCount: Map[Int, Long] = {
        (1 to numBands)
            .map(i => {
                val band = raster.GetRasterBand(i)
                val validCount = band.AsMDArray().GetStatistics().getValid_count
                i -> validCount
            })
            .toMap
    }

    /**
      * @param subsetName
      *   The name of the subdataset to get.
      * @return
      *   Returns the raster's subdataset with given name.
      */
    def getSubdataset(subsetName: String): MosaicRasterGDAL = {
        val path = subdatasets.get(s"${subsetName}_tmp")
        val gdalError = gdal.GetLastErrorMsg()
        val error = path match {
            case Some(_) => ""
            case None    => s"""
                            |Subdataset $subsetName not found!
                            |Available subdatasets:
                            |     ${subdatasets.keys.filterNot(_.startsWith("SUBDATASET_")).mkString(", ")}
                            |     """.stripMargin
        }
        val sanitized = PathUtils.getCleanPath(path.getOrElse(PathUtils.NO_PATH_STRING))
        val subdatasetPath = PathUtils.getSubdatasetPath(sanitized)

        val ds = openRaster(subdatasetPath)
        // Avoid costly IO to compute MEM size here
        // It will be available when the raster is serialized for next operation
        // If value is needed then it will be computed when getMemSize is called
        val createInfo = Map(
          "path" -> path.getOrElse(PathUtils.NO_PATH_STRING),
          "parentPath" -> parentPath,
          "driver" -> getDriversShortName,
          "last_error" -> {
              if (gdalError.nonEmpty || error.nonEmpty) s"""
                                                           |GDAL Error: $gdalError
                                                           |$error
                                                           |""".stripMargin
              else ""
          }
        )
        MosaicRasterGDAL(ds, createInfo, -1)
    }

    def convolve(kernel: Array[Array[Double]]): MosaicRasterGDAL = {
        val resultRasterPath = PathUtils.createTmpFilePath(getRasterFileExtension)

        this.raster
            .GetDriver()
            .CreateCopy(resultRasterPath, this.raster, 1)
            .delete()

        val outputRaster = gdal.Open(resultRasterPath, GF_Write)
        
        for (bandIndex <- 1 to this.numBands) {
            val band = this.getBand(bandIndex)
            val outputBand = outputRaster.GetRasterBand(bandIndex)
            band.convolve(kernel, outputBand)
        }

        val createInfo = Map(
          "path" -> resultRasterPath,
          "parentPath" -> parentPath,
          "driver" -> getDriversShortName
        )

        val result = MosaicRasterGDAL(outputRaster, createInfo, this.memSize)
        result.flushCache()
    }

    def filter(kernelSize: Int, operation: String): MosaicRasterGDAL = {
        val resultRasterPath = PathUtils.createTmpFilePath(getRasterFileExtension)

        this.raster
            .GetDriver()
            .CreateCopy(resultRasterPath, this.raster, 1)
            .delete()

        val outputRaster = gdal.Open(resultRasterPath, GF_Write)

        for (bandIndex <- 1 to this.numBands) {
            val band = this.getBand(bandIndex)
            val outputBand = outputRaster.GetRasterBand(bandIndex)
            band.filter(kernelSize, operation, outputBand)
        }

        val createInfo = Map(
          "path" -> resultRasterPath,
          "parentPath" -> parentPath,
          "driver" -> getDriversShortName
        )

        val result = MosaicRasterGDAL(outputRaster, createInfo, this.memSize)
        result.flushCache()
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
    def identifyDriver(parentPath: String): String = {
        val isSubdataset = PathUtils.isSubdataset(parentPath)
        val path = PathUtils.getCleanPath(parentPath)
        val readPath =
            if (isSubdataset) PathUtils.getSubdatasetPath(path)
            else PathUtils.getZipPath(path)
        val driver = gdal.IdentifyDriverEx(readPath)
        val driverShortName = driver.getShortName
        driverShortName
    }

    /**
      * Reads a raster from a file system path. Reads a subdataset if the path
      * is to a subdataset.
      *
      * @example
      *   Raster: path = "file:///path/to/file.tif" Subdataset: path =
      *   "file:///path/to/file.tif:subdataset"
      * @param createInfo
      *   The create info for the raster. This should contain the following
      *   keys:
      *   - path: The path to the raster file.
      *   - parentPath: The path of the parent raster file.
      * @return
      *   A MosaicRaster object.
      */
    override def readRaster(createInfo: Map[String, String]): MosaicRasterGDAL = {
        val inPath = createInfo("path")
        val isSubdataset = PathUtils.isSubdataset(inPath)
        val path = PathUtils.getCleanPath(inPath)
        val readPath =
            if (isSubdataset) PathUtils.getSubdatasetPath(path)
            else PathUtils.getZipPath(path)
        val dataset = openRaster(readPath, None)
        val error =
            if (dataset == null) {
                val error = gdal.GetLastErrorMsg()
                s"""
                Error reading raster from path: $readPath
                Error: $error
            """
            } else ""
        val driverShortName = Try(dataset.GetDriver().getShortName).getOrElse("NONE")
        // Avoid costly IO to compute MEM size here
        // It will be available when the raster is serialized for next operation
        // If value is needed then it will be computed when getMemSize is called
        // We cannot just use memSize value of the parent due to the fact that the raster could be a subdataset
        val raster = MosaicRasterGDAL(
          dataset,
          createInfo ++
              Map(
                "driver" -> driverShortName,
                "last_error" -> error
              ),
          -1
        )
        raster
    }

    /**
      * Reads a raster from a byte array.
      * @param contentBytes
      *   The byte array containing the raster data.
      * @param createInfo
      *   Mosaic creation info of the raster. Note: This is not the same as the
      *   metadata of the raster. This is not the same as GDAL creation options.
      * @return
      *   A MosaicRaster object.
      */
    override def readRaster(contentBytes: Array[Byte], createInfo: Map[String, String]): MosaicRasterGDAL = {
        if (Option(contentBytes).isEmpty || contentBytes.isEmpty) {
            MosaicRasterGDAL(null, createInfo, -1)
        } else {
            // This is a temp UUID for purposes of reading the raster through GDAL from memory
            // The stable UUID is kept in metadata of the raster
            val driverShortName = createInfo("driver")
            val extension = GDAL.getExtension(driverShortName)
            val tmpPath = PathUtils.createTmpFilePath(extension)
            Files.write(Paths.get(tmpPath), contentBytes)
            // Try reading as a tmp file, if that fails, rename as a zipped file
            val dataset = openRaster(tmpPath, Some(driverShortName))
            if (dataset == null) {
                val zippedPath = s"$tmpPath.zip"
                Files.move(Paths.get(tmpPath), Paths.get(zippedPath), StandardCopyOption.REPLACE_EXISTING)
                val readPath = PathUtils.getZipPath(zippedPath)
                val ds = openRaster(readPath, Some(driverShortName))
                if (ds == null) {
                    // the way we zip using uuid is not compatible with GDAL
                    // we need to unzip and read the file if it was zipped by us
                    val parentDir = Paths.get(zippedPath).getParent
                    val prompt = SysUtils.runScript(Array("/bin/sh", "-c", s"cd $parentDir && unzip -o $zippedPath -d $parentDir"))
                    // zipped files will have the old uuid name of the raster
                    // we need to get the last extracted file name, but the last extracted file name is not the raster name
                    // we can't list folders due to concurrent writes
                    val extension = GDAL.getExtension(driverShortName)
                    val lastExtracted = SysUtils.getLastOutputLine(prompt)
                    val unzippedPath = PathUtils.parseUnzippedPathFromExtracted(lastExtracted, extension)
                    val dataset = openRaster(unzippedPath, Some(driverShortName))
                    if (dataset == null) {
                        throw new Exception(s"Error reading raster from bytes: ${prompt._3}")
                    }
                    MosaicRasterGDAL(dataset, createInfo + ("path" -> unzippedPath), contentBytes.length)
                } else {
                    MosaicRasterGDAL(ds, createInfo + ("path" -> readPath), contentBytes.length)
                }
            } else {
                MosaicRasterGDAL(dataset, createInfo + ("path" -> tmpPath), contentBytes.length)
            }
        }
    }

    /**
      * Reads a raster band from a file system path. Reads a subdataset band if
      * the path is to a subdataset.
      *
      * @example
      *   Raster: path = "file:///path/to/file.tif" Subdataset: path =
      *   "file:///path/to/file.tif:subdataset"
      * @param createInfo
      *   The create info for the raster. This should contain the following
      *   keys:
      *   - path: The path to the raster file.
      *   - parentPath: The path of the parent raster file.
      *   - driver: Optional: The driver short name of the raster file
      * @param bandIndex
      *   The band index to read.
      * @return
      *   A MosaicRaster object.
      */
    override def readBand(bandIndex: Int, createInfo: Map[String, String]): MosaicRasterBandGDAL = {
        val raster = readRaster(createInfo)
        // TODO: Raster and Band are coupled, this can cause a pointer leak
        raster.getBand(bandIndex)
    }

}
