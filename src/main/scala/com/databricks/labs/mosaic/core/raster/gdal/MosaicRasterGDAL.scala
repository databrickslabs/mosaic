package com.databricks.labs.mosaic.core.raster.gdal

import com.databricks.labs.mosaic.MOSAIC_NO_DRIVER
import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.api.GDAL.getCheckpointDir
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL.{identifyDriver, readRaster}
import com.databricks.labs.mosaic.core.raster.io.RasterHydrator.pathAsDataset
import com.databricks.labs.mosaic.core.raster.io.{RasterCleaner, RasterHydrator, RasterReader, RasterWriter}
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
import java.util.{Locale, UUID}
import scala.collection.JavaConverters.dictionaryAsScalaMapConverter
import scala.util.{Failure, Success, Try}

/**
  * Mosaic's GDAL internal object for rasters.
  * - Constructor invoked from various functions, including the
  *   [[MosaicRasterGDAL]] scala companion object.
  * - When invoked, raster is already a GDAL [[Dataset]].
  * - "path" expected to be either "no_path" or fuse accessible.
  * - same for "parent_path"
  * - 0.4.3+ dataset is set to internal `_ds` object which is then
  *   used exclusively to avoid having to construct new `this`.
  */
//noinspection DuplicatedCode
case class MosaicRasterGDAL(
                               datasetInit: Dataset,
                               createInfoInit: Map[String, String],
                               memSizeInit: Long
                           ) extends RasterWriter
    with RasterCleaner
    with RasterHydrator {

    // Factory for creating CRS objects
    protected val crsFactory: CRSFactory = new CRSFactory

    /**
     * Make use of an internal Dataset
     * - allows efficiently populating without destroying the object
     * - exclusively used / managed, e.g. set to null on `destroy`,
     *   then can be tested to reload from path as needed.
     */
    private var dataset: Dataset = datasetInit

    /**
     * Make use of an internal Map.
     * - will be replaced on any change (immutable)
     */
    private var createInfo: Map[String, String] = createInfoInit

    /**
     * Make use of internal memSize
     * - avoid expensive recalculations
     */
    private var memSize: Long = memSizeInit


    /////////////////////////////////////////
    // GDAL Dataset
    /////////////////////////////////////////

    /**
      * For the provided geometry and CRS, get bounding box polygon.
      * @param geometryAPI
      *   Default is JTS.
      * @param destCRS
      *   CRS for the bbox, default is [[MosaicGDAL.WSG84]].
      * @return
      *   Returns [[MosaicGeometry]] representing bounding box polygon.
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

    /** @return The diagonal size of a raster. */
    def diagSize: Double = math.sqrt(xSize * xSize + ySize * ySize)

    // noinspection ZeroIndexToHead
    /** @return Returns the raster's extent as a Seq(xmin, ymin, xmax, ymax). */
    def extent: Seq[Double] = {
        val minX = getGeoTransform(0)
        val maxY = getGeoTransform(3)
        val maxX = minX + getGeoTransform(1) * xSize
        val minY = maxY + getGeoTransform(5) * ySize
        Seq(minX, minY, maxX, maxY)
    }

    /** @return compression from metadata or "NONE". */
    def getCompression: String = {
        val compression = Option(this.getDatasetHydrated.GetMetadata_Dict("IMAGE_STRUCTURE"))
            .map(_.asScala.toMap.asInstanceOf[Map[String, String]])
            .getOrElse(Map.empty[String, String])
            .getOrElse("COMPRESSION", "NONE")
        compression
    }

    /** @return Returns the raster's geotransform as a Seq. */
    def getGeoTransform: Array[Double] = this.getDatasetHydrated.GetGeoTransform()

    /**
      * 0.4.3 file memory size or pixel size * datatype over bands; r
      * returns -1 if those are unobtainable.
      *
      * @return
      *   Returns the amount of memory occupied by the file in bytes or estimated size.
      */
    def getMemSize: Long = {
        if (this.getDatasetHydrated != null && memSize == -1) {
            val toRead = if (getPath.startsWith("/vsizip/")) getPath.replace("/vsizip/", "") else getCleanPath
            memSize = Try(
                if (Files.notExists(Paths.get(toRead))) getBytesCount
                else Files.size(Paths.get(toRead))
            ).getOrElse(-1)
        }
        memSize
    }

    /** @return freshly calculated memSize from the (latest) internal path. */
    def calcMemSize(): Long = {
        memSize = -1
        this.getMemSize
    }

    /**
      * Get spatial reference.
      * - may be already set on the raster
      * - if not, load and detect it.
      * - defaults to [[MosaicGDAL.WSG84]]
      * @return
      *   Raster's [[SpatialReference]] object.
      */
    def getSpatialReference: SpatialReference = {
        Option(getDatasetHydrated.GetSpatialRef) match {
            case Some(spatialRef) => spatialRef
            case _ => MosaicGDAL.WSG84
        }
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
                .filter(_.toLowerCase(Locale.ROOT).startsWith(this.getDriverShortName.toLowerCase(Locale.ROOT)))
                .flatMap(bp => readRaster(createInfo + ("path" -> bp)).getBands)
                .takeWhile(_.isEmpty)
                .nonEmpty
        } else {
            bands.takeWhile(_.isEmpty).nonEmpty
        }
    }

    /** @return Returns the raster's metadata as a Map. */
    def metadata: Map[String, String] = {
        Option(this.getDatasetHydrated.GetMetadataDomainList())
            .map(_.toArray)
            .map(domain =>
                domain
                    .map(domainName =>
                        Option(this.getDatasetHydrated.GetMetadata_Dict(domainName.toString))
                            .map(_.asScala.toMap.asInstanceOf[Map[String, String]])
                            .getOrElse(Map.empty[String, String])
                    )
                    .reduceOption(_ ++ _)
                    .getOrElse(Map.empty[String, String])
            )
            .getOrElse(Map.empty[String, String])
    }

    /** @return Returns the raster's number of bands. */
    def numBands: Int = {
        val bandCount = Try(this.getDatasetHydrated.GetRasterCount())
        bandCount match {
            case Success(value) => value
            case Failure(_)     => 0
        }
    }

    /** @return Returns the origin x coordinate. */
    def originX: Double = this.getGeoTransform(0)

    /** @return Returns the origin y coordinate. */
    def originY: Double = this.getGeoTransform(3)

    /** @return Returns the diagonal size of a pixel. */
    def pixelDiagSize: Double = math.sqrt(pixelXSize * pixelXSize + pixelYSize * pixelYSize)

    /** @return Returns pixel x size. */
    def pixelXSize: Double = this.getGeoTransform(1)

    /** @return Returns pixel y size. */
    def pixelYSize: Double = this.getGeoTransform(5)

    /** @return Returns the raster's proj4 string. */
    def proj4String: String = {
        try {
            this.getDatasetHydrated.GetSpatialRef.ExportToProj4
        } catch {
            case _: Any => ""
        }
    }

    /** rehydrate the underlying GDAL raster dataset object. This is for forcing a refresh. */
    override def reHydrate(): Unit = {
        this.destroy()
        this.getDatasetHydrated
        this.calcMemSize()
    }

    /**
     * Sets the raster's SRID. This is the EPSG code of the raster's CRS.
     * - it will update the memSize.
     * - this is an in-place op in 0.4.3+.
     */
    def setSRID(srid: Int): Unit = {
        // (1) srs from srid
        val srs = new osr.SpatialReference()
        srs.ImportFromEPSG(srid)

        // (2) set srs on internal datasource
        this.getDatasetHydrated.SetSpatialRef(srs)
        val driver = dataset.GetDriver()
        val _driverShortName = driver.getShortName

        // (3) populate new file with the new srs
        val tmpPath = PathUtils.createTmpFilePath(GDAL.getExtension(_driverShortName))
        driver.CreateCopy(tmpPath, dataset)

        // (4) destroy internal datasource and driver
        this.destroy()
        driver.delete()

        // (5) update the internal createInfo
        val _parentPath = this.getParentPath
        this.updateCreateInfo(
            Map(
            "path" -> tmpPath,
            "parentPath" -> _parentPath,
            "driver" -> _driverShortName
            )
        )

        // (6) re-calculate internal memSize
        // - also ensures internal dataset is hydrated
        calcMemSize()
    }

    /** @return Returns the raster's SRID. This is the EPSG code of the raster's CRS. */
    def SRID: Int = {
        Try(crsFactory.readEpsgFromParameters(proj4String))
            .filter(_ != null)
            .getOrElse("EPSG:0")
            .split(":")
            .last
            .toInt
    }

    /** @return Returns the min x coordinate. */
    def xMin: Double = originX

    /** @return Returns the max x coordinate. */
    def xMax: Double = originX + xSize * pixelXSize

    /** @return Returns x size of the raster. */
    def xSize: Int = this.getDatasetHydrated.GetRasterXSize

    /** @return Returns the min y coordinate. */
    def yMin: Double = originY

    /** @return Returns the max y coordinate. */
    def yMax: Double = originY + ySize * pixelYSize

    /** @return Returns y size of the raster. */
    def ySize: Int = this.getDatasetHydrated.GetRasterYSize

    /////////////////////////////////////////
    // Apply Functions
    /////////////////////////////////////////

    /**
      * Applies a convolution filter to the raster.
      * - operator applied per band.
      * @param kernel
      *   [[Array[Double]]] kernel to apply to the raster.
      * @return
      *   [[MosaicRasterGDAL]] object.
      */
    def convolve(kernel: Array[Array[Double]]): MosaicRasterGDAL = {
        val tmpPath = PathUtils.createTmpFilePath(this.getRasterFileExtension)

        val tmpDriver = this.getDatasetHydrated.GetDriver()
        val tmpDs = tmpDriver.CreateCopy(tmpPath, dataset, 1)
        RasterCleaner.destroy(tmpDs)
        tmpDriver.delete()

        val outputDataset = gdal.Open(tmpPath, GF_Write)

        for (bandIndex <- 1 to this.numBands) {
            val band = this.getBand(bandIndex)
            val outputBand = outputDataset.GetRasterBand(bandIndex)
            band.convolve(kernel, outputBand)
        }

        val newCreateInfo = Map(
            "path" -> tmpPath,
            "parentPath" -> this.getParentPath,
            "driver" -> this.getDriverShortName
        )

        val result = MosaicRasterGDAL(outputDataset, newCreateInfo, -1)
        result.reHydrate() // also calc's memSize again.
        result
    }

    /**
     * Applies a filter to the raster.
     *
     * @param kernelSize
     *   Number of pixels to compare; it must be odd.
     * @param operation
     *   Op to apply, e.g. ‘avg’, ‘median’, ‘mode’, ‘max’, ‘min’.
     * @return
     *   Returns a new [[MosaicRasterGDAL]] with the filter applied.
     */
    def filter(kernelSize: Int, operation: String): MosaicRasterGDAL = {
        val tmpPath = PathUtils.createTmpFilePath(getRasterFileExtension)

        val tmpDriver = this.getDatasetHydrated.GetDriver()
        val tmpDs = tmpDriver.CreateCopy(tmpPath, dataset, 1)
        RasterCleaner.destroy(tmpDs)
        tmpDriver.delete()

        val outputDataset = gdal.Open(tmpPath, GF_Write)

        for (bandIndex <- 1 to this.numBands) {
            val band = this.getBand(bandIndex)
            val outputBand = outputDataset.GetRasterBand(bandIndex)
            band.filter(kernelSize, operation, outputBand)
        }

        val newCreateInfo = Map(
            "path" -> tmpPath,
            "parentPath" -> this.getParentPath,
            "driver" -> getDriverShortName
        )

        val result = MosaicRasterGDAL(outputDataset, newCreateInfo, -1)
        result.reHydrate() // also calc's memSize again.
        result
    }

    /**
      * Applies a function to each band of the raster.
      * @param f
      *   The function to apply.
      * @return
      *   Returns a Seq of the results of the function.
      */
    def transformBands[T](f: MosaicRasterBandGDAL => T): Seq[T] = for (i <- 1 to numBands) yield f(getBand(i))

    /**
      * Applies clipping to get cellid raster.
      * @param cellID
      *   Clip the raster based on the cell id geometry.
      * @param indexSystem
      *   Default is H3.
      * @param geometryAPI
      *   Default is JTS.
      * @return
      *   Returns [[MosaicRasterGDAL]] for a given cell ID. Used for tessellation.
      */
    def getRasterForCell(cellID: Long, indexSystem: IndexSystem, geometryAPI: GeometryAPI): MosaicRasterGDAL = {
        val cellGeom = indexSystem.indexToGeometry(cellID, geometryAPI)
        val geomCRS = indexSystem.osrSpatialRef
        RasterClipByVector.clip(this, cellGeom, geomCRS, geometryAPI)
    }

    /////////////////////////////////////////
    // Subdataset Functions
    /////////////////////////////////////////

    /**
      * Get a particular subdataset by name.
      * @param subsetName
      *   The name of the subdataset to get.
      * @return
      *   Returns [[MosaicRasterGDAL]].
      */
    def getSubdataset(subsetName: String): MosaicRasterGDAL = {
        val sPath = subdatasets.get(s"${subsetName}_tmp")
        val gdalError = gdal.GetLastErrorMsg()
        val error = sPath match {
            case Some(_) => ""
            case None    => s"""
                               |Subdataset $subsetName not found!
                               |Available subdatasets:
                               |     ${subdatasets.keys.filterNot(_.startsWith("SUBDATASET_")).mkString(", ")}
                               |     """.stripMargin
        }
        val sanitized = PathUtils.getCleanPath(sPath.getOrElse(PathUtils.NO_PATH_STRING))
        val subdatasetPath = PathUtils.getSubdatasetPath(sanitized)

        val ds = pathAsDataset(subdatasetPath, getDriverShortNameOpt)
        // Avoid costly IO to compute MEM size here
        // It will be available when the raster is serialized for next operation
        // If value is needed then it will be computed when getMemSize is called
        val newCreateInfo = Map(
            "path" -> sPath.getOrElse(PathUtils.NO_PATH_STRING),
            "parentPath" -> this.getParentPath,
            "driver" -> getDriverShortName,
            "last_error" -> {
                if (gdalError.nonEmpty || error.nonEmpty) s"""
                                                             |GDAL Error: $gdalError
                                                             |$error
                                                             |""".stripMargin
                else ""
            }
        )
        MosaicRasterGDAL(ds, newCreateInfo, -1)
    }

    /**
      * Test if path is a subdataset.
      * @return boolean
      */
    def isSubDataset: Boolean = {
        val isSubdataset = PathUtils.isSubdataset(this.getPath)
        isSubdataset
    }

    /** @return Returns the raster's subdatasets as a Map. */
    def subdatasets: Map[String, String] = {
        val dict = Try(this.getDatasetHydrated.GetMetadata_Dict("SUBDATASETS"))
            .getOrElse(new java.util.Hashtable[String, String]())
        val subdatasetsMap = Option(dict)
            .map(_.asScala.toMap.asInstanceOf[Map[String, String]])
            .getOrElse(Map.empty[String, String])
        val keys = subdatasetsMap.keySet
        val sanitizedParentPath = this.getCleanParentPath
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

    /////////////////////////////////////////
    // Band Functions
    /////////////////////////////////////////

    /**
      * @param bandId
      *   The band index to read.
      * @return
      *   Returns the raster's band as a [[MosaicRasterBandGDAL]] object.
      */
    def getBand(bandId: Int): MosaicRasterBandGDAL = {
        if (bandId > 0 && numBands >= bandId) {
            MosaicRasterBandGDAL(this.getDatasetHydrated.GetRasterBand(bandId), bandId)
        } else {
            throw new ArrayIndexOutOfBoundsException()
        }
    }

    /** @return Returns a map of the raster band(s) statistics. */
    def getBandStats: Map[Int, Map[String, Double]] = {
        (1 to numBands)
            .map(i => {
                val band = this.getDatasetHydrated.GetRasterBand(i)
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

    /** @return Returns a map of raster band(s) valid pixel count. */
    def getValidCount: Map[Int, Long] = {
        (1 to numBands)
            .map(i => {
                val band = this.getDatasetHydrated.GetRasterBand(i)
                val validCount = band.AsMDArray().GetStatistics().getValid_count
                i -> validCount
            })
            .toMap
    }

    /** @return Returns the total bytes based on pixels * datatype per band, can be alt to memsize. */
    def getBytesCount: Long = {
        (1 to numBands)
            .map(i => this.getDatasetHydrated.GetRasterBand(i))
            .map(b => Try(
                b.GetXSize().toLong * b.GetYSize().toLong * gdal.GetDataTypeSize(b.getDataType).toLong
            ).getOrElse(0L))
            .sum
    }

    /////////////////////////////////////////
    // Raster Lifecycle Functions
    /////////////////////////////////////////

    /**
      * Destroys the raster object. After this operation the raster object is no
      * longer usable. If the raster is needed again, use the refreshFromPath method.
      * - calls to [[RasterCleaner]] static method.
      */
    override def destroy(): Unit = {
        RasterCleaner.destroy(this.datasetInit)
        RasterCleaner.destroy(this.dataset)
        this.dataset = null // <- important to trigger refresh
    }

    /** @return write options for this raster's dataset. */
    def getWriteOptions: MosaicRasterWriteOptions = MosaicRasterWriteOptions(this)

    /**
     * Writes a raster to a byte array.
     *
     * @param doDestroy
     *   A boolean indicating if the raster object should be destroyed after writing.
     *   - file paths handled separately.
     * @return
     *   A byte array containing the raster data.
     */
    override def writeToBytes(doDestroy: Boolean): Array[Byte] = {
        val readPath = {
            val tmpPath =
                if (isSubDataset) {
                    val tmpPath = PathUtils.createTmpFilePath(getRasterFileExtension)
                    writeToPath(tmpPath, doDestroy = false) // destroy 1x at end
                    tmpPath
                } else {
                    this.getPath
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

        if (doDestroy) this.destroy()
        byteArray
    }

    /**
     * Writes a raster to a specified file system path.
     *
     * @param newPath
     *   The path to write the raster.
     * @param doDestroy
     *   A boolean indicating if the raster object should be destroyed after writing.
     *   - file paths handled separately.
     * @return
     *   The path where written (may differ, e.g. due to subdatasets).
     */
    override def writeToPath(newPath: String, doDestroy: Boolean): String = {
        if (isSubDataset) {
            val tmpDriver = this.getDatasetHydrated.GetDriver()
            val tmpDs = tmpDriver.CreateCopy(newPath, dataset, 1)
            tmpDriver.delete()
            if (tmpDs == null) {
                val error = gdal.GetLastErrorMsg()
                throw new Exception(s"Error writing raster to path: $error")
            } else RasterCleaner.destroy(tmpDs)
            if (doDestroy) this.destroy()
            newPath
        } else {
            val thisPath = Paths.get(this.getPath)
            val fromDir = thisPath.getParent
            val toDir = Paths.get(newPath).getParent
            val stemRegex = PathUtils.getStemRegex(this.getPath)
            PathUtils.wildcardCopy(fromDir.toString, toDir.toString, stemRegex)
            if (doDestroy) this.destroy()
            s"$toDir/${thisPath.getFileName}"
        }
    }

    def isCheckpointPath: Boolean = {
        this.getCleanPath.startsWith(GDAL.getCheckpointDir)
    }

    /**
     * Writes a raster to the configured checkpoint directory.
     *
     * @param doDestroy
     *   A boolean indicating if the raster object should be destroyed after writing.
     *   - file paths handled separately.
     *   Skip deletion of interim file writes, if any.
     * @return
     *   The path where written (may differ, e.g. due to subdatasets).
     */
    override def writeToCheckpointDir(doDestroy: Boolean): String = {
        if (isCheckpointPath) {
            getPath
        } else {
            if (isSubDataset) {
                val uuid = UUID.randomUUID().toString
                val ext = GDAL.getExtension(this.getDriverShortName)
                val writePath = s"${getCheckpointDir}/$uuid.$ext"

                val tmpDriver = this.getDatasetHydrated.GetDriver()
                val tmpDs = tmpDriver.CreateCopy(writePath, dataset, 1)
                tmpDriver.delete()
                if (tmpDs == null) {
                    val error = gdal.GetLastErrorMsg()
                    throw new Exception(s"Error writing raster to path: $error")
                } else RasterCleaner.destroy(tmpDs)
                if (doDestroy) this.destroy()
                writePath
            } else {
                val thisPath = Paths.get(this.getPath)
                val fromDir = thisPath.getParent
                val toDir = GDAL.getCheckpointDir
                val stemRegex = PathUtils.getStemRegex(this.getPath)
                PathUtils.wildcardCopy(fromDir.toString, toDir, stemRegex)
                if (doDestroy) this.destroy()
                s"$toDir/${thisPath.getFileName}"
            }
        }
    }

    ///////////////////////////////////////////////////
    // Additional Getters + Updaters
    ///////////////////////////////////////////////////

    /** @return Returns the raster's bands as a Seq. */
    def getBands: Seq[MosaicRasterBandGDAL] = (1 to numBands).map(getBand)

    /** Returns immutable internal map. */
    def getCreateInfo: Map[String, String] = createInfo

    /** @return Returns a tuple with the raster's size. */
    def getDimensions: (Int, Int) = (xSize, ySize)

    /**
     * If not currently set:
     * - will try from driver.
     * - will set the found name.
     * @return The raster's driver short name or [[MOSAIC_NO_DRIVER]].
     */
    def getDriverShortName: String = {
        this.getDriverShortNameOpt match {
            case Some(name) if name != MOSAIC_NO_DRIVER => name
            case _ =>
                // (1) try from hydrated dataset
                val _n1 = Try(this.getDatasetHydrated.GetDriver().getShortName)
                if (_n1.isSuccess) {
                    this.updateCreateInfoDriver(_n1.get)
                    _n1.get
                } else {
                    // (2) try to identify from parent path
                    val _n2 = Try(identifyDriver(this.getParentPath))
                    if (_n2.isSuccess) {
                        this.updateCreateInfoDriver(_n2.get)
                        _n2.get
                    } else {
                        // (3) try to identify from path
                        val _n3 = Try(identifyDriver(this.getPath))
                        if (_n3.isSuccess) {
                            this.updateCreateInfoDriver(_n3.get)
                            _n3.get
                        } else {
                            this.updateCreateInfoDriver(MOSAIC_NO_DRIVER)
                            MOSAIC_NO_DRIVER
                        }
                    }
                }
        }
    }

    /** @return The raster's path on disk. Usually this is a parent file for the tile. */
    def getParentPath: String = createInfo.get("parentPath").getOrElse(PathUtils.NO_PATH_STRING)

    def getCleanParentPath: String = PathUtils.getCleanPath(getParentPath)

    /** @return Returns the raster's path. */
    def getPath: String = createInfo.get("path").getOrElse(PathUtils.NO_PATH_STRING)

    def getCleanPath: String = PathUtils.getCleanPath(getPath)

    /** The driver name as option */
    def getDriverShortNameOpt: Option[String] = createInfo.get("driver")

    /** Update the internal map. */
    def updateCreateInfo(newMap: Map[String, String]): Unit = createInfo = newMap

    /** Update path on internal map */
    def updateCreateInfoPath(path: String): Unit = {
        createInfo += ("path" -> path)
    }

    /** Update parentPath on internal map. */
    def updateCreateInfoParentPath(parentPath: String): Unit = {
        createInfo += ("parentPath" -> parentPath)
    }

    /** Update driver on internal map. */
    def updateCreateInfoDriver(driver: String): Unit = {
        createInfo += ("driver" -> driver)
    }

    /** Update last error on internal map. */
    def updateCreateInfoError(msg: String, fullMsg: String = ""): Unit = {
        createInfo += ("last_error" -> msg, "full_error" -> fullMsg)
    }

    /** Update last command on internal map. */
    def updateCreateInfoLastCmd(cmd: String): Unit = {
        createInfo += ("last_command" -> cmd)
    }

    /** Update last command on internal map. */
    def updateCreateInfoAllParents(parents: String): Unit = {
        createInfo += ("all_parents" -> parents)
    }

    /** @return Underlying GDAL raster dataset object, hydrated if possible. */
    override def getDatasetHydrated: Dataset = {
        // focus exclusively on internal `_ds` object
        // - only option is to try to reload from path
        // - use the option variation to avoid cyclic dependency call
        if (dataset == null) {
            Try(dataset = pathAsDataset(this.getPath, this.getDriverShortNameOpt))
        }
        dataset
    }

    /** @return Returns file extension. */
    def getRasterFileExtension: String = GDAL.getExtension(this.getDriverShortName)

}


//noinspection ZeroIndexToHead
/** Companion object for MosaicRasterGDAL Implements RasterReader APIs */
object MosaicRasterGDAL extends RasterReader{

    /** @return a new empty [[MosaicRasterGDAL]] object. */
    def empty: MosaicRasterGDAL = {
        MosaicRasterGDAL(
            datasetInit = null,
            createInfoInit = Map.empty[String, String],
            memSizeInit = -1)
    }

    /**
      * Identifies the driver of a raster from a file system path.
      * @param aPath
      *   The path to the raster file.
      * @return
      *   A string representing the driver short name.
      */
    def identifyDriver(parentPath: String): String = {
        val isSubdataset = PathUtils.isSubdataset(parentPath)
        val cleanParentPath = PathUtils.getCleanPath(parentPath)
        val readPath =
            if (isSubdataset) PathUtils.getSubdatasetPath(cleanParentPath)
            else PathUtils.getZipPath(cleanParentPath)
        val driver = gdal.IdentifyDriverEx(readPath)
        val driverShortName = driver.getShortName
        driver.delete()
        driverShortName
    }

    /**
      * Reads a raster band from a file system path. Reads a subdataset band if
      * the path is to a subdataset.
      * @example
      *   Raster: path = "/path/to/file.tif" Subdataset: path =
      *   "FORMAT:/path/to/file.tif:subdataset"
      * @param bandIndex
      *   The band index to read (1+ indexed).
      * @param createInfo
      *   Map of create info for the raster.
      * @return
      *   A [[MosaicRasterGDAL]] object.
      */
    override def readBand(bandIndex: Int, createInfo: Map[String, String]): MosaicRasterBandGDAL = {
        val raster = readRaster(createInfo)
        // Note: Raster and Band are coupled, this can cause a pointer leak
        raster.getBand(bandIndex)
    }

    /**
      * Reads a raster from a byte array. Expects "driver" in createInfo.
      * @param contentBytes
      *   The byte array containing the raster data.
      * @param createInfo
      *   Mosaic creation info of the raster. Note: This is not the same as the
      *   metadata of the raster. This is not the same as GDAL creation options.
      * @return
      *   A [[MosaicRasterGDAL]] object.
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
            val ds = pathAsDataset(tmpPath, Some(driverShortName))
            if (ds == null) {
                val zippedPath = s"$tmpPath.zip"
                Files.move(Paths.get(tmpPath), Paths.get(zippedPath), StandardCopyOption.REPLACE_EXISTING)
                val readPath = PathUtils.getZipPath(zippedPath)
                val ds1 = pathAsDataset(readPath, Some(driverShortName))
                if (ds1 == null) {
                    // the way we zip using uuid is not compatible with GDAL
                    // we need to unzip and read the file if it was zipped by us
                    val parentDir = Paths.get(zippedPath).getParent
                    val prompt = SysUtils.runScript(Array("/bin/sh", "-c", s"cd $parentDir && unzip -o $zippedPath -d $parentDir"))
                    // zipped files will have the old uuid name of the raster
                    // we need to get the last extracted file name, but the last extracted file name is not the raster name
                    // we can't list folders due to concurrent writes
                    val lastExtracted = SysUtils.getLastOutputLine(prompt)
                    val unzippedPath = PathUtils.parseUnzippedPathFromExtracted(lastExtracted, extension)
                    val ds2 = pathAsDataset(unzippedPath, Some(driverShortName))
                    if (ds2 == null) {
                        // TODO: 0.4.3 do we want to just return a tile with error instead of exception?
                        throw new Exception(s"Error reading raster from bytes: ${prompt._3}")
                    }
                    MosaicRasterGDAL(ds2, createInfo + ("path" -> unzippedPath), contentBytes.length)
                } else {
                    MosaicRasterGDAL(ds1, createInfo + ("path" -> readPath), contentBytes.length)
                }
            } else {
                MosaicRasterGDAL(ds, createInfo + ("path" -> tmpPath), contentBytes.length)
            }
        }
    }

    /**
      * Reads a raster from a file system path. Reads a subdataset if the path
      * is to a subdataset.
      * @example
      *   Raster: path = "/path/to/file.tif" Subdataset: path =
      *   "FORMAT:/path/to/file.tif:subdataset"
      * @param createInfo
      *   Map of create info for the raster.
      * @return
      *   A [[MosaicRasterGDAL]] object.
      */
    override def readRaster(createInfo: Map[String, String]): MosaicRasterGDAL = {
        val inPath = createInfo("path")
        val isSubdataset = PathUtils.isSubdataset(inPath)
        val cleanPath = PathUtils.getCleanPath(inPath)
        val readPath =
            if (isSubdataset) PathUtils.getSubdatasetPath(cleanPath)
            else PathUtils.getZipPath(cleanPath)
        val ds = pathAsDataset(readPath, None)
        val error =
            if (ds == null) {
                val error = gdal.GetLastErrorMsg()
                s"""
                Error reading raster from path: $readPath
                Error: $error
            """
            } else ""
        val driverShortName = Try(ds.GetDriver().getShortName).getOrElse(MOSAIC_NO_DRIVER)
        // Avoid costly IO to compute MEM size here
        // It will be available when the raster is serialized for next operation
        // If value is needed then it will be computed when getMemSize is called
        // We cannot just use memSize value of the parent due to the fact that the raster could be a subdataset
        val raster = MosaicRasterGDAL(
          ds,
          createInfo ++
              Map(
                "driver" -> driverShortName,
                "last_error" -> error
              ),
          -1
        )
        raster
    }

}
