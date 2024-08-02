package com.databricks.labs.mosaic.core.raster.gdal

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.io.RasterIO
import com.databricks.labs.mosaic.core.raster.operator.clip.RasterClipByVector
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.POLYGON
import com.databricks.labs.mosaic.gdal.MosaicGDAL
import com.databricks.labs.mosaic.utils.{FileUtils, PathUtils, SysUtils}
import com.databricks.labs.mosaic._
import com.databricks.labs.mosaic.core.raster.io.RasterIO.createTmpFileFromDriver
import com.databricks.labs.mosaic.functions.ExprConfig
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants._
import org.gdal.osr
import org.gdal.osr.SpatialReference
import org.locationtech.proj4j.CRSFactory

import java.nio.file.{Files, Paths}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.collection.JavaConverters.dictionaryAsScalaMapConverter
import scala.util.Try

/**
 * Internal object for a deserialized tile from [[RasterTile]]. 0.4.3+ only
 * constructs with createInfo and then nothing else happens until the object is
 * used.
 *   - setting a dataset will cause an internal re-hydrate, can set multiple
 *     times if needed and will subsequently overwrite [[RASTER_PATH_KEY]],
 *     [[RASTER_DRIVER_KEY]], and [[RASTER_PARENT_PATH_KEY]].
 *   - changes to createInfo (updates) for driver or path will also cause an
 *     internal re-hydrate and will overwrite any existing dataset.
 *   - when this object is initialized (via path, byte array, or dataset) the
 *     used path applies the configured fuse directory, default is checkpoint
 *     dir but may be overridden as well.
 *
 * @param createInfoInit
 *   - Init Map[String. String] (immutable)
 *   - Defaults to empty Map (see `apply` functions)
 *   - Internally, use a var that can be modified
 *     through the life of the tile: e.g. if one of the `updateCreateInfo*` functions called.
 * @param exprConfigOpt
 *    Option [[ExprConfig]]
 */
case class RasterGDAL(
                         createInfoInit: Map[String, String],
                         exprConfigOpt: Option[ExprConfig]
                     ) extends RasterIO {

    val DIR_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmm") // yyyyMMddHHmmss

    // Factory for creating CRS objects
    protected val crsFactory: CRSFactory = new CRSFactory

    // identify an intentionally empty [[RasterGDAL]]
    private var emptyRasterGDAL = false

    // See [[RasterIO]] for public APIs using these
    var fuseDirOpt: Option[String] = None

    // Populated throughout the lifecycle,
    // - After init, defers in part to [[DatasetGDAL]]
    private var createInfo = createInfoInit

    // Internally work on a option [[RasterGDAL]]
    // This will maintain:
    // (1) the 'path' [[String]] from which it was loaded
    // (2) the 'driverShortName' [[String]] used to load
    // (3) the 'dataset' [[Dataset]] itself may or may not be hydrated
    val datasetGDAL = DatasetGDAL() // <- val - new object in 0.4.3+

    /** @inheritdoc */
    override def initAndHydrate(forceInit: Boolean = false): RasterGDAL = {
        if (forceInit) initFlag = true
        this.withDatasetHydratedOpt() // <- init and other flags handled inline
        this // fluent
    }

    /** @inheritdoc */
    override def isDatasetHydrated: Boolean = datasetGDAL.isHydrated

    /** @inheritdoc */
    override def isDatasetRefreshFlag: Boolean = initFlag || datasetNewFlag || pathNewFlag

    /** @inheritdoc */
    override def withDatasetHydratedOpt(): Option[Dataset] = {
        this._handleFlags()
        // option just for the [[Dataset]]
        // - strips out the [[DatasetGDAL]] object
        datasetGDAL.getDatasetOpt
    }

    /**
     * Make use of an internal Dataset
     *   - allows efficiently populating without destroying the object
     *   - exclusively used / managed, e.g. set to None on `destroy`, then can
     *     be tested to reload from path as needed.
     *   - if any affecting changes are made after init, then use a
     *     reconstituted dataset in place of initial.
     */
    private var initFlag = true // 1x setup (starts as true)
    private var (datasetNewFlag, pathNewFlag) = (false, false) // <- flags that must be handled

    /** @return hydrated dataset or null (for internal use). */
    private def _datasetHydrated: Dataset = {
        this.withDatasetHydratedOpt() match {
            case Some(dataset) => dataset
            case _             => null
        }
    }

    /**
     * Flags needing to be handled are init | dataset | path.
     *   - The strategy is to load [[Dataset]], then write to fuse dir.
     *
     * @return
     * `this` fluent (for internal use).
     */
    private def _handleFlags(): RasterGDAL =
        Try {
            try {
                // make sure createinfo in sync
                // - calls _initCreateInfo
                // - also [[DatasetGDAL]] and its objects
                // - this could be only on an `initFlag` test,
                //   but seems better to always do it
                this.getCreateInfo

                // !!! avoid cyclic dependencies !!!
                /*
                 * Call to setup a tile (handle flags):
                 *   (1) initFlag    - if dataset exists, do (2); otherwise do (3).
                 *   (2) datasetNewFlag - need to write to fuse and set path.
                 *   (3) pathNewFlag    - need to load dataset and write to fuse (path then replaced in createInfo).
                 * If empty (not a "real" [[RasterGDAL]] object), don't do anything.
                 */
                if (!this.isEmptyRasterGDAL) {
                    if (this.isDatasetRefreshFlag) {
                        // conditionally write dataset to fuse
                        // - the flags mean other conditions already handled
                        // - datasetNewFlag means the dataset was just loaded (so don't load here)
                        if (!datasetNewFlag && (initFlag || pathNewFlag)) {
                            // load from path (aka 1,3)
                            // - concerned only with a driver set on createInfo (if any),
                            //   passed as a option; otherwise, file extension is testsed.

                            // for either init or path flag
                            // - update path and driver on dataset
                            datasetGDAL.updatePath(this.getRawPath)
                            if (!datasetGDAL.isHydrated) {
                                datasetGDAL.updateDriverName(this.getDriverName())
                            }
                        }
                    }
                    // if update path called, and doDestroy was passed then
                    // this condition will be met
                    if (!datasetGDAL.isHydrated) {
                        RasterIO.rawPathAsDatasetOpt(this.getRawPath, datasetGDAL.driverNameOpt, exprConfigOpt) match {
                            case Some(dataset) =>
                                this.updateDataset(dataset)
                            case _ =>
                                this.updateCreateInfoError(s"handleFlags - expected path '$getRawPath' to load to dataset, " +
                                    s"but it did not: hydrated? ${isDatasetHydrated}")
                        }
                    }
                }
            } finally {
                this._resetFlags
            }
            this
        }.getOrElse(this)

    /** @return [[RasterGDAL]] `this` (fluent). */
    private def _resetFlags: RasterGDAL = {
        datasetNewFlag = false
        pathNewFlag = false
        initFlag = false
        this
    }

    // ///////////////////////////////////////
    // GDAL Dataset
    // ///////////////////////////////////////

    /** @return freshly calculated memSize from the (latest) internal path. */
    def calcMemSize(): Long = {
        this.updateCreateInfoMemSize(-1)
        this.refreshMemSize
    }

    /**
     * For the provided geometry and CRS, get bounding box polygon.
     * @param geometryAPI
     *   Default is JTS.
     * @param destCRS
     *   CRS for the bbox, default is [[MosaicGDAL.WSG84]].
     * @return
     *   Returns [[MosaicGeometry]] representing bounding box polygon, default
     *   is empty polygon.
     */
    def bbox(geometryAPI: GeometryAPI, destCRS: SpatialReference = MosaicGDAL.WSG84): MosaicGeometry =
        Try {
            val gt = this.getGeoTransformOpt.get
            val sourceCRS = this.getSpatialReference
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
        }.getOrElse(geometryAPI.geometry(POLYGON_EMPTY_WKT, "WKT"))

    /** @return The diagonal size of a tile. */
    def diagSize: Double = math.sqrt(xSize * xSize + ySize * ySize)

    // noinspection ZeroIndexToHead
    /**
     * @return
     *   Returns the tile's extent as a Seq(xmin, ymin, xmax, ymax), default
     *   all 0s.
     */
    def extent: Seq[Double] =
        Try {
            val gt = this.getGeoTransformOpt.get
            val minX = gt(0)
            val maxY = gt(3)
            val maxX = minX + gt(1) * xSize
            val minY = maxY + gt(5) * ySize
            Seq(minX, minY, maxX, maxY)
        }.getOrElse(Seq(0, 0, 0, 0))

    /** @return compression from metadata or "NONE". */
    def getCompression: String =
        Try {
            Option(this._datasetHydrated.GetMetadata_Dict("IMAGE_STRUCTURE"))
                .map(_.asScala.toMap.asInstanceOf[Map[String, String]])
                .get("COMPRESSION")
        }.getOrElse("None")

    /** @return Returns a tuple with the tile's size. */
    def getDimensions: (Int, Int) = (xSize, ySize)

    /** @return Returns the tile's geotransform as a Option Seq. */
    def getGeoTransformOpt: Option[Array[Double]] =
        Try {
            this._datasetHydrated.GetGeoTransform()
        }.toOption

    /**
     * @return
     *   Returns the total bytes based on pixels * datatype per band, can be
     *   alt to memsize, default is -1.
     */
    def getPixelBytesCount: Long =
        Try {
            (1 to this.numBands)
                .map(i => this._datasetHydrated.GetRasterBand(i))
                .map(b =>
                    Try(
                        b.GetXSize().toLong * b.GetYSize().toLong * gdal.GetDataTypeSize(b.getDataType).toLong
                    ).getOrElse(0L)
                )
                .sum
        }.getOrElse(-1L)

    /**
     * Get spatial reference.
     *   - may be already set on the tile
     *   - if not, load and detect it.
     *   - defaults to [[MosaicGDAL.WSG84]]
     * @return
     *   Raster's [[SpatialReference]] object.
     */
    def getSpatialReference: SpatialReference =
        Try {
            this._datasetHydrated.GetSpatialRef
        }.getOrElse(MosaicGDAL.WSG84)

    /** @return Returns a map of tile band(s) valid pixel count, default 0. */
    def getValidCount: Map[Int, Long] =
        Try {
            (1 to numBands)
                .map(i => {
                    val band = this._datasetHydrated.GetRasterBand(i)
                    val validCount = band.AsMDArray().GetStatistics().getValid_count
                    i -> validCount
                })
                .toMap
        }.getOrElse(Map.empty[Int, Long])

    /**
     * @return
     *   True if the tile is empty, false otherwise. May be expensive to
     *   compute since it requires reading the tile and computing statistics.
     */
    def isEmpty: Boolean =
        Try {
            val driverSN = this.getDriverNameOpt.get // <- allow the exception
            val bands = this.getBands
            // is the sequence empty?
            if (bands.isEmpty) {
                // test subdatasets
                // - generate a RasterGDAL
                val subRasters: Array[RasterGDAL] = subdatasets.values
                    .filter(_.toLowerCase(Locale.ROOT).startsWith(driverSN.toLowerCase(Locale.ROOT)))
                    .map(bp => RasterGDAL(createInfo + (RASTER_PATH_KEY -> bp), exprConfigOpt))
                    .toArray

                val subResult: Boolean = subRasters.map(_.getBands).takeWhile(_.isEmpty).nonEmpty
                // clean up these interim RasterGDAL objects.
                subRasters.foreach(_.flushAndDestroy())

                subResult
            } else {
                // is at least 1 RasterBandGDAL non-empty?
                bands.takeWhile(_.isEmpty).nonEmpty
            }
        }.getOrElse(true)

    /** @return Returns the tile's metadata as a Map, defaults to empty. */
    def metadata: Map[String, String] = Try {
        this.withDatasetHydratedOpt() match {
            case Some(_) =>
                datasetGDAL.metadata
            case _ =>
                Map.empty[String, String]
        }
    }.getOrElse(Map.empty[String, String])

    /** @return Returns the tile's number of bands, defaults to 0. */
    def numBands: Int =
        Try {
            this._datasetHydrated.GetRasterCount()
        }.getOrElse(0)

    /** @return Returns the origin x coordinate, defaults to -1. */
    def originX: Double =
        Try {
            this.getGeoTransformOpt.get(0)
        }.getOrElse(-1)

    /** @return Returns the origin y coordinate, defaults to -1. */
    def originY: Double =
        Try {
            this.getGeoTransformOpt.get(3)
        }.getOrElse(-1)

    /** @return Returns the diagonal size of a pixel, defaults to 0. */
    def pixelDiagSize: Double = math.sqrt(pixelXSize * pixelXSize + pixelYSize * pixelYSize)

    /** @return Returns pixel x size, defaults to 0. */
    def pixelXSize: Double =
        Try {
            this.getGeoTransformOpt.get(1)
        }.getOrElse(0)

    /** @return Returns pixel y size, defaults to 0. */
    def pixelYSize: Double =
        Try {
            this.getGeoTransformOpt.get(5)
        }.getOrElse(0)

    /** @return Returns the tile's proj4 string, defaults to "". */
    def proj4String: String =
        Try {
            this._datasetHydrated.GetSpatialRef.ExportToProj4
        }.getOrElse("")

    /**
     * 0.4.3 file memory size or pixel size * datatype over bands; r returns -1
     * if those are unobtainable.
     *
     * @return
     *   Returns the amount of memory occupied by the file in bytes or
     *   estimated size.
     */
    def refreshMemSize: Long = {
        if (this._datasetHydrated != null && this.getMemSize == -1) {
            val toRead = getPathGDAL.asFileSystemPath

            val sz: Long = Try {
                if (Files.notExists(Paths.get(toRead))) this.getPixelBytesCount
                else Files.size(Paths.get(toRead))
            }.getOrElse(-1L)
            if (sz > -1) this.updateCreateInfoMemSize(sz)
        }
        this.getMemSize
    }

    /**
     * Sets the tile's SRID. This is the EPSG code of the tile's CRS.
     *   - this is an in-place op in 0.4.3+.
     * @param dataset
     *   The [[Dataset]] to update the SRID
     * @param srid
     *   The srid to set.
     * @return
     *   `this` [[RasterGDAL]] (fluent).
     */
    def setSRID(srid: Int): RasterGDAL =
    Try {
        // (1) make sure dataset hydrated
        this.initAndHydrate()

        datasetGDAL.getDatasetOpt match {
            case Some(dataset) =>
                // (2) srs from srid
                val srs = new osr.SpatialReference()
                srs.ImportFromEPSG(srid)

                // (3) set srs on internal datasource
                // - see (4) as well
                dataset.SetSpatialRef(srs)
                val tmpDriver = dataset.GetDriver()
                val tmpDriverSN = tmpDriver.getShortName

                // (4) populate new file with the new srs
                // - flushes cache with destroy
                // - wraps in try / finally for driver delete
                try {
                    val tmpPath = RasterIO.createTmpFileFromDriver(tmpDriverSN, exprConfigOpt)
                    tmpDriver.CreateCopy(tmpPath, dataset)

                    // (5) update the internal createInfo
                    // - uses a best effort to get a parent path with a file ext
                    // - flushes cache with destroy
                    // - deletes the driver
                    this.updateCreateInfoLastCmd("setSRID")
                    this.updateCreateInfoRawParentPath(this.getRawPath)
                    this.updateCreateInfoRawPath(tmpPath, skipFlag = false)
                    this.updateCreateInfoDriver(tmpDriverSN)

                } finally {
                    tmpDriver.delete()
                    this.flushAndDestroy() // <- make sure all written to path
                }
            case _ =>
                // handle dataset is None
                this.updateCreateInfoLastCmd("setSRID")
                this.updateCreateInfoError("setSRID - `datasetGDAL.getDatasetOpt` unsuccessful")
        }

        // (6) for external callers
        // - return a `this` object populated with the same path
        this
    }.getOrElse {
        this.updateCreateInfoLastCmd("setSRID")
        this.updateCreateInfoError("setSRID - initAndHydrate unsuccessful")
        this
    }

    /**
     * @return
     *   Returns the tile's SRID. This is the EPSG code of the tile's CRS.
     */
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

    /** @return Returns x size of the tile, default 0. */
    def xSize: Int =
        Try {
            this._datasetHydrated.GetRasterXSize
        }.getOrElse(0)

    /** @return Returns the min y coordinate. */
    def yMin: Double = originY

    /** @return Returns the max y coordinate. */
    def yMax: Double = originY + ySize * pixelYSize

    /** @return Returns y size of the tile, default 0. */
    def ySize: Int =
        Try {
            this._datasetHydrated.GetRasterYSize
        }.getOrElse(0)

    // ///////////////////////////////////////
    // Subdataset Functions
    // ///////////////////////////////////////

    /**
     * This is a simple Getter.
     *   - When a [[RasterGDAL]] object was derived from a subdataset,
     *     important to maintain the parent subdataset name.
     *
     * @return
     *   Option subdataset name as string.
     */
    def getCreateInfoSubdatasetNameOpt: Option[String] = {
        if (datasetGDAL.subdatasetNameOpt.isEmpty) {
            datasetGDAL.subdatasetNameOpt = this.createInfo.get(RASTER_SUBDATASET_NAME_KEY)
        }
        datasetGDAL.subdatasetNameOpt
    }

    /** @return Returns the tile's subdatasets as a Map, default empty. */
    def subdatasets: Map[String, String] =
        Try {
            this.withDatasetHydratedOpt() match {
                case Some(_) =>
                    // use parent if it exists; otherwise path
                    if (getParentPathGDAL.isPathSetAndExists) datasetGDAL.subdatasets(getPathGDAL)
                    else datasetGDAL.subdatasets(getPathGDAL)
                case _ =>
                    Map.empty[String, String]
            }
        }.getOrElse(Map.empty[String, String])

    /**
     * Set the subdataset name.
     * - This is a simple setter, for referencing.
     *
     * @param name
     *   Name of the subdataset.
     * @return
     *   [[RasterGDAL]] `this` (fluent).
     */
    def updateCreateInfoSubdatasetName(name: String): RasterGDAL = {
        this.createInfo += (RASTER_SUBDATASET_NAME_KEY -> name)
        datasetGDAL.updateSubdatasetName(name)
        this
    }

    // ///////////////////////////////////////
    // Band Functions
    // ///////////////////////////////////////

    /**
     * @param bandId
     *   The band index to read.
     * @return
     *   Returns the tile's band as a [[RasterBandGDAL]] object.
     */
    def getBand(bandId: Int): RasterBandGDAL = {
        // TODO 0.4.3 - Throw exception or return empty ?
        if (bandId > 0 && this.numBands >= bandId) {
            RasterBandGDAL(this._datasetHydrated.GetRasterBand(bandId), bandId)
        } else {
            throw new ArrayIndexOutOfBoundsException()
        }
    }

    /**
     * This is a simple Getter.
     *   - When a [[RasterGDAL]] object was derived from a band, important to
     *     maintain the parent band number.
     *
     * @return
     *   Option band number as int.
     */
    def getCreateInfoBandIndexOpt: Option[Int] = {
        if (datasetGDAL.bandIdxOpt.isEmpty) {
            datasetGDAL.bandIdxOpt = Option(this.createInfo(RASTER_BAND_INDEX_KEY).toInt)
        }
        datasetGDAL.bandIdxOpt
    }

    /** @return Returns the tile's bands as a Seq, defaults to empty Seq. */
    def getBands: Seq[RasterBandGDAL] = Try{
        (1 to this.numBands).map(this.getBand)
    }.getOrElse(Seq.empty[RasterBandGDAL])

    /**
     * @return
     *   Returns a map of the tile band(s) statistics, default empty.
     */
    def getBandStats: Map[Int, Map[String, Double]] =
        Try {
            (1 to numBands)
                .map(i => {
                    val band = this._datasetHydrated.GetRasterBand(i)
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
        }.getOrElse(Map.empty[Int, Map[String, Double]])

    /** Update band num (including on metadata), return `this` (fluent). */
    def updateCreateInfoBandIndex(num: Int): RasterGDAL = {
        // need dataset hydrated for metadata set
        this.initAndHydrate().createInfo += (RASTER_BAND_INDEX_KEY -> num.toString)
        datasetGDAL.updateBandIdx(num)
        this
    }

    // ///////////////////////////////////////
    // Apply Functions
    // ///////////////////////////////////////

    /**
     * Applies a convolution filter to the tile.
     *   - operator applied per band.
     *   - this will not succeed if dataset not hydratable.
     * @param kernel
     *   [[Array[Double]]] kernel to apply to the tile.
     * @return
     *   New [[RasterGDAL]] object with kernel applied.
     */
    def convolve(kernel: Array[Array[Double]]): RasterGDAL =
        Try {
            // (1) hydrate the dataset
            this.withDatasetHydratedOpt() // want to trigger hydrate

            // (2) write dataset to tmpPath
            // - This will be populated as we operate on the tmpPath
            // TODO Should this be `datasetOrPathCopy` ???
            val tmpPath = RasterIO.createTmpFileFromDriver(getDriverName(), exprConfigOpt)

            if (datasetGDAL.datasetCopyToPath(tmpPath, doDestroy = false, skipUpdatePath = true)) {

                // (3) perform the op using dataset from the tmpPath
                val outputDataset = gdal.Open(tmpPath, GF_Write) // open to write

                for (bandIndex <- 1 to this.numBands) {
                    val band = this.getBand(bandIndex)
                    val outputBand = outputDataset.GetRasterBand(bandIndex)
                    band.convolve(kernel, outputBand) // <- convolve op
                }

                // (4) finalize
                val driver = outputDataset.GetDriver()
                RasterIO.flushAndDestroy(outputDataset)
                try {
                    // initially un-hydrated with tmp path
                    // - we need to write to a fuse path
                    val result = RasterGDAL(
                        Map(
                            RASTER_PATH_KEY -> tmpPath,
                            RASTER_PARENT_PATH_KEY -> {
                                this.identifyPseudoPathOpt() match {
                                    case Some(path) => path
                                    case _          => NO_PATH_STRING
                                }
                            },
                            RASTER_DRIVER_KEY -> driver.getShortName
                        ),
                        exprConfigOpt
                    )

                    result
                } finally {
                    driver.delete()
                }
            } else {
                val result = RasterGDAL()
                result.updateCreateInfoLastCmd("convolve")
                result.updateCreateInfoError("convolve - datasetCopyToPath = false")

                result
            }
        }.getOrElse {
            val result = RasterGDAL()
            result.updateCreateInfoLastCmd("convolve")
            result.updateCreateInfoError("convolve - kernel unsuccessful")

            result
        }

    /**
     * Applies a filter to the tile.
     *   - operator applied per band.
     *   - this will throw an exception if dataset not hydratable.
     *
     * @param kernelSize
     *   Number of pixels to compare; it must be odd.
     * @param operation
     *   Op to apply, e.g. ‘avg’, ‘median’, ‘mode’, ‘max’, ‘min’.
     * @return
     *   New [[RasterGDAL]] with the kernel applied.
     */
    def filter(kernelSize: Int, operation: String): RasterGDAL =
        Try {
            // (1) hydrate the dataset
            this.withDatasetHydratedOpt() // want to trigger hydrate

            // (2) write dataset to tmpPath
            // - This will be populated as we operate on the tmpPath
            // TODO Should this be `datasetOrPathCopy` ???
            val tmpPath = RasterIO.createTmpFileFromDriver(getDriverName(), exprConfigOpt)
            if (datasetGDAL.datasetCopyToPath(tmpPath, doDestroy = false, skipUpdatePath = true)) {

                // (3) perform the op using dataset from the tmpPath
                val outputDataset = gdal.Open(tmpPath, GF_Write) // open to write

                for (bandIndex <- 1 to this.numBands) {
                    val band = this.getBand(bandIndex)
                    val outputBand = outputDataset.GetRasterBand(bandIndex)
                    band.filter(kernelSize, operation, outputBand) // <- filter op
                }

                // (4) finalize
                val driver = outputDataset.GetDriver()
                RasterIO.flushAndDestroy(outputDataset)
                try {
                    // initially un-hydrated with tmp path
                    // - we need to write to a fuse path
                    val result = RasterGDAL(
                        Map(
                            RASTER_PATH_KEY -> tmpPath,
                            RASTER_PARENT_PATH_KEY -> {
                                this.identifyPseudoPathOpt() match {
                                    case Some(path) => path
                                    case _          => NO_PATH_STRING
                                }
                            },
                            RASTER_DRIVER_KEY -> driver.getShortName
                        ),
                        exprConfigOpt
                    )

                    result
                } finally {
                    driver.delete()
                }
            } else {
                val result = RasterGDAL()
                result.updateCreateInfoLastCmd("filter")
                result.updateCreateInfoError("filter - datasetCopyToPath = false")

                result
            }
        }.getOrElse {
            val result = RasterGDAL()
            result.updateCreateInfoLastCmd("filter")
            result.updateCreateInfoError("filter - kernel unsuccessful")

            result
        }

    /**
     * Applies clipping to get cellid tile.
     * @param cellID
     *   Clip the tile based on the cell id geometry.
     * @param indexSystem
     *   Default is H3.
     * @param geometryAPI
     *   Default is JTS.
     * @return
     *   New [[RasterGDAL]] for a given cell ID. Used for tessellation.
     */
    def getRasterForCell(cellID: Long, indexSystem: IndexSystem, geometryAPI: GeometryAPI): RasterGDAL = {
        val cellGeom = indexSystem.indexToGeometry(cellID, geometryAPI)
        val geomCRS = indexSystem.osrSpatialRef
        RasterClipByVector.clip(this, cellGeom, geomCRS, geometryAPI, exprConfigOpt)
    }

    /**
     * Get a particular subdataset by name.
     * @param subsetName
     *   The name of the subdataset to get.
     * @return
     *   Returns new [[RasterGDAL]].
     */
    def getSubdataset(subsetName: String): RasterGDAL = {
        Try {
            // try to get the subdataset requested
            // - allow failure on extracting subdataset,
            // then handle with empty [[RasterGDAL]]
            this.initAndHydrate()
            val dsGDAL = datasetGDAL.getSubdatasetObj(getRawParentPath, subsetName, exprConfigOpt)

            // pull out the needed info
            // - use option on dataset
            // to trigger exception if null
            val pathRawSub = dsGDAL.getPath
            val dsSubOpt = dsGDAL.getDatasetOpt


            // Avoid costly IO to compute MEM size here
            // It will be available when the tile is serialized for next operation
            // If value is needed then it will be computed when getMemSize is called
            val gdalError = gdal.GetLastErrorMsg ()
            val newCreateInfo = Map(
                RASTER_PATH_KEY -> pathRawSub,
                RASTER_PARENT_PATH_KEY -> this.getRawParentPath,
                RASTER_DRIVER_KEY -> this.getDriverName(),
                RASTER_SUBDATASET_NAME_KEY -> subsetName,
                RASTER_LAST_ERR_KEY -> {
                    if (gdalError.nonEmpty) s"GDAL Error: $gdalError"
                    else ""
                }
            )
            RasterGDAL(dsSubOpt.get, exprConfigOpt, newCreateInfo)
        }.getOrElse {
            val result = RasterGDAL()
            result.updateCreateInfoError(
                s"RasterGDAL - getSubdatasetName '$subsetName' unable to be loaded to dataset",
                fullMsg = s"""
                             |Subdataset $subsetName not found!
                             |Available subdatasets:
                             |     ${subdatasets.keys.filterNot (_.startsWith ("SUBDATASET_") ).mkString (", ")}
                             |     """.stripMargin
            )
            result
        }
    }

    /**
     * Sets the tile's SRID. This is the EPSG code of the tile's CRS.
     *   - this is an in-place op in 0.4.3+.
     * @param dataset
     *   The [[Dataset]] to update the SRID
     * @param srid
     *   The srid to set.
     * @param exprConfigOpt
     *   Option [[ExprConfig]]
     * @return
     *   Option [[Dataset]].
     */
    def transformDatasetWithSRID(dataset: Dataset, srid: Int, exprConfigOpt: Option[ExprConfig]): Dataset = {

        // (1) srs from srid
        val srs = new osr.SpatialReference()
        srs.ImportFromEPSG(srid)

        // (2) set srs on internal datasource
        dataset.SetSpatialRef(srs)
        val tmpDriver = dataset.GetDriver()
        val tmpDriverSN = tmpDriver.getShortName

        // (3) populate new file with the new srs
        // - flushes cache with destroy
        // - wraps in try / finally for driver delete
        try {
            val tmpPath = RasterIO.createTmpFileFromDriver(tmpDriverSN, exprConfigOpt)
            tmpDriver.CreateCopy(tmpPath, dataset)
        } finally {
            tmpDriver.delete()
        }
    }

    /**
     * Applies a function to each band of the tile.
     * @param f
     *   The function to apply.
     * @return
     *   Returns a Seq of the results of the function.
     */
    def transformBands[T](f: RasterBandGDAL => T): Seq[T] = {
        for (i <- 1 to this.numBands) yield f(this.getBand(i))
    }

    // ///////////////////////////////////////
    // Raster Lifecycle Functions
    // ///////////////////////////////////////

    //scalastyle:off println
    /** @inheritdoc */
    override def finalizeRaster(toFuse: Boolean): RasterGDAL =
        Try {
            // (1) call handle flags,
            // to get everything resolved on the tile as needed
            this._handleFlags()    // e.g. will write to fuse path

            // (2) write if current path not fuse or not under the expected dir
            if (
                (!this.isEmptyRasterGDAL && toFuse) &&
                (!this.getPathGDAL.isFusePath || !this.isRawPathInFuseDir)
            ) {
                val driverSN = this.getDriverName()
                val ext = GDAL.getExtension(driverSN)
                val newDir = this.makeNewFuseDir(ext, uuidOpt = None)
                //println(s"...finalizeRaster - newDir? '$newDir'")

                datasetGDAL.datasetOrPathCopy(newDir, doDestroy = true, skipUpdatePath = true) match {
                    case Some(newPath) =>
                        //println(s"...success [pre-update raw path] - finalizeRaster - new path? '$newPath'")
                        this.updateCreateInfoRawPath(newPath, skipFlag = true)
                        //println(s"...success - finalizeRaster - path? '${getRawPath}'")
                    case _ =>
                        this.updateCreateInfoLastCmd("finalizeRaster")
                        this.updateCreateInfoError(s"finalizeRaster - fuse write")
                }
            }

            // (4) return this
            this
        }.getOrElse {
            if (!this.isEmptyRasterGDAL) {
                this.updateCreateInfoLastCmd("finalizeRaster")
                this.updateCreateInfoError(s"finalizeRaster - exception - fuse write")
            }
            this
        }
    //scalastyle:on println

    /** @inheritdoc */
    override def isRawPathInFuseDir: Boolean =
        Try {
            // !!! avoid cyclic dependencies !!!
            // - wrapped to handle false conditions
            this.fuseDirOpt match {
                case Some(dir) => getPathGDAL.asFileSystemPath.startsWith(dir)
                case _         => getPathGDAL.asFileSystemPath.startsWith(GDAL.getCheckpointDir)
            }
        }.getOrElse(false)

    /** @inheritdoc */
    override def flushAndDestroy(): RasterGDAL = {
        datasetGDAL.flushAndDestroy()
        this
    }

    /** @inheritdoc */
    override def getFuseDirOpt: Option[String] = fuseDirOpt

    /** @return write options for this tile's dataset. */
    def getWriteOptions: RasterWriteOptions = RasterWriteOptions(this)

    /** @return whether `this` has a non-empty error. */
    def hasError: Boolean = {
        Try(this.createInfo(RASTER_LAST_ERR_KEY).length > 0).getOrElse(false)
    }

    /** @return new fuse dir underneath the base fuse dir (checkpoint or override) */
    def makeNewFuseDir(ext: String, uuidOpt: Option[String]): String = {
        // (1) uuid used in dir
        // - may be provided (for filename consistency)
        val uuid = uuidOpt match {
            case Some(u) => u
            case _ => RasterIO.genUUID
        }
        // (2) new dir under fuse dir (<timePrefix>_<ext>_<uuid>)
        val rootDir = fuseDirOpt.getOrElse(GDAL.getCheckpointDir)
        val timePrefix = LocalDateTime.now().format(DIR_TIME_FORMATTER)
        val newDir = s"${timePrefix}_${ext}_${uuid}"
        val dir = s"$rootDir/$newDir"
        Files.createDirectories(Paths.get(dir)) // <- create the directories
        dir
    }

    /** @return new fuse path string, defaults to under checkpoint dir (doesn't actually create the file). */
    def makeNewFusePath(ext: String): String = {
        // (1) uuid used in dir and filename
        val uuid = RasterIO.genUUID

        // (2) new dir under fuse dir (<timePrefix>_<uuid>.<ext>)
        val fuseDir = makeNewFuseDir(ext, Option(uuid))

        // (3) return the new fuse path name
        val filename = RasterIO.genFilenameUUID(ext, Option(uuid))
        s"$fuseDir/$filename"
    }

    /** @return `this` [[RasterGDAL]] (fluent). */
    def updateDataset(dataset: Dataset) : RasterGDAL = {
        val doUpdateDriver = dataset != null
        if (doUpdateDriver) datasetNewFlag = true          // <- flag for dataset if not null (normal use)
        else pathNewFlag = true                            // <- flag for path if null
        datasetGDAL.updateDataset(dataset, doUpdateDriver) // <- only update driver if not null
        this
    }

    // /////////////////////////////////////////////////
    // Additional Getters + Updaters
    // /////////////////////////////////////////////////

    /** make sure all [[DatasetGDAL]] `createInfo` relevant fields are initialized (ok to do this often). */
    private def _initCreateInfo: RasterGDAL = {
        // refresh all relevant datasetGDAL keys if they are empty / not set
        // - !!! don't call any getters here !!!
        if (datasetGDAL.pathGDAL.path == NO_PATH_STRING) {
            datasetGDAL.pathGDAL.updatePath(createInfo.getOrElse(RASTER_PATH_KEY, NO_PATH_STRING))
        }
        if (datasetGDAL.parentPathGDAL.path == NO_PATH_STRING) {
            datasetGDAL.parentPathGDAL.updatePath(createInfo.getOrElse(RASTER_PARENT_PATH_KEY, NO_PATH_STRING))
        }
        if (datasetGDAL.driverNameOpt.isEmpty) {
            datasetGDAL.driverNameOpt = createInfo.get(RASTER_DRIVER_KEY) match {
                case Some(name) if name != NO_DRIVER => Some(name)
                case _ => None
            }
        }
        if (datasetGDAL.subdatasetNameOpt.isEmpty) {
            datasetGDAL.subdatasetNameOpt = createInfo.get(RASTER_SUBDATASET_NAME_KEY)
        }
        if (datasetGDAL.bandIdxOpt.isEmpty) {
            datasetGDAL.bandIdxOpt = {
                createInfo.get(RASTER_BAND_INDEX_KEY) match {
                    // bandIx >= 1 is valid
                    case Some(bandIdx) if bandIdx.toInt > 0 => Some(bandIdx.toInt)
                    case _ => None
                }
            }
        }
        this
    }

    /** Returns immutable internal map, representing `createInfo` at initialization (not the lastest). */
    def getCreateInfoFromInit: Map[String, String] = createInfoInit

    /** Returns immutable internal map, representing latest KVs (blends from `datasetGDAL`). */
    def getCreateInfo: Map[String, String] = {
        this._initCreateInfo
        this.createInfo ++= datasetGDAL.asCreateInfo
        this.createInfo
    }

    /** Return [[datasetGDAL]]. */
    def getDatasetGDAL: DatasetGDAL = datasetGDAL

    /** Return the 'path' [[PathGDAL]] (within [[datasetGDAL]]). */
    def getPathGDAL: PathGDAL = getDatasetGDAL.pathGDAL

    /** Return the 'parentPath' [[PathGDAL]] (within [[datasetGDAL]]). */
    def getParentPathGDAL: PathGDAL = getDatasetGDAL.parentPathGDAL

    /** @inheritdoc */
    override def getDatasetOpt: Option[Dataset] = {
        this._initCreateInfo
        datasetGDAL.getDatasetOpt
    }

    /** @inheritdoc */
    override def getDriverNameOpt: Option[String] = datasetGDAL.driverNameOpt

    /**
     * @return
     *   The tile's path on disk, or NO_PATH_STRING. Usually this is a parent
     *   file for the tile.
     */
    def getRawParentPath: String = {
        this._initCreateInfo
        datasetGDAL.parentPathGDAL.path
    }

    /** @return Returns the tile's path, or NO_PATH_STRING. */
    def getRawPath: String = {
        this._initCreateInfo
        datasetGDAL.pathGDAL.path
    }

    /** @return memSize (from CreateInfo) */
    def getMemSize: Long = Try(createInfo(RASTER_MEM_SIZE_KEY).toLong).getOrElse(-1)

    /** @inheritdoc */
    override def getPathOpt: Option[String] = {
        val p = getRawPath
        if (p == NO_PATH_STRING) None
        else Option(p)
    }

    /** @inheritdoc */
    override def getParentPathOpt: Option[String] = {
        val p = getRawParentPath
        if (p == NO_PATH_STRING) None
        else Option(p)
    }

    /** @inheritdoc */
    override def isEmptyRasterGDAL: Boolean = emptyRasterGDAL

    /** Set empty indicator for the object (not the dataset), returns [[RasterGDA]] (fluent). */
    def setEmptyRasterGDAL(empty: Boolean): RasterGDAL = {
        emptyRasterGDAL = empty
        this
    }

    /** @inheritdoc */
    override def setFuseDirOpt(dirOpt: Option[String]): RasterGDAL = {
        this.fuseDirOpt = dirOpt
        this
    }

    /** Update the internal map, return `this` (fluent) - skipFlag. */
    def updateCreateInfo(newMap: Map[String, String], skipFlags: Boolean): RasterGDAL = {
        // !!! avoid cyclic dependencies !!!
        if (!skipFlags) pathNewFlag = true
        createInfo = newMap
        this._initCreateInfo
        this
    }

    /** Update driver on internal map + `datasetGDAL`, return `this` (fluent). */
    def updateCreateInfoDriver(driver: String): RasterGDAL = {
        this.createInfo += (RASTER_DRIVER_KEY -> driver)
        this._initCreateInfo
        this.datasetGDAL.updateDriverName(driver)
        this
    }

    /** Update path on internal map + `datasetGDAL`, return `this` (fluent) - `skipFlag`. */
    def updateCreateInfoRawPath(rawPath: String, skipFlag: Boolean): RasterGDAL = {
        if (!skipFlag) pathNewFlag = true
        this.createInfo += (RASTER_PATH_KEY -> rawPath)
        this._initCreateInfo
        this.getPathGDAL.updatePath(rawPath)
        this
    }

    /** Update parentPath on internal map + `datasetGDAL`, return `this` (fluent). */
    def updateCreateInfoRawParentPath(rawParentPath: String): RasterGDAL = {
        this.createInfo += (RASTER_PARENT_PATH_KEY -> rawParentPath)
        this._initCreateInfo
        this.getParentPathGDAL.updatePath(rawParentPath)
        this
    }

    /** Update last command on internal map, return `this` (fluent). */
    def updateCreateInfoLastCmd(cmd: String): RasterGDAL = {
        this.createInfo += (RASTER_LAST_CMD_KEY -> cmd)
        this
    }

    /** Update last error on internal map, return `this` (fluent). */
    def updateCreateInfoError(msg: String, fullMsg: String = ""): RasterGDAL = {
        this.createInfo += (RASTER_LAST_ERR_KEY -> msg, RASTER_FULL_ERR_KEY -> fullMsg)
        this
    }

    /** Update last command on internal map, return `this` (fluent). */
    def updateCreateInfoAllParents(parents: String): RasterGDAL = {
        this.createInfo += (RASTER_ALL_PARENTS_KEY -> parents)
        this
    }

    /** Update last error on internal map, return `this` (fluent). */
    def updateCreateInfoMemSize(sz: Long): RasterGDAL = {
        this.createInfo += (RASTER_MEM_SIZE_KEY -> sz.toString)
        this
    }

}

/** Singleton / companion object for RasterGDAL. */
object RasterGDAL {

    /**
     * Empty [[RasterGDAL]]
     * + only constructor where `setEmptyRasterGDAL` called.
     *
     * @return
     * Returns an empty [[RasterGDAL]] object (only for empty results). */
    def apply(): RasterGDAL = {
        val result = RasterGDAL(Map.empty[String, String], None)
        result.setEmptyRasterGDAL(true)
        result.updateCreateInfoLastCmd("emptyRasterGDAL")
        result.updateCreateInfoError("emptyRasterGDAL = true")
        result
    }

        /**
         * [[Dataset]] focused:
         * + createInfo defaults to empty map
         * + fuseDirOpt defaults to None
         *
         * @return a [[RasterGDAL]] object from the provided [[Dataset]].
         */
        def apply(
                     dataset: Dataset,
                     exprConfigOpt: Option[ExprConfig],
                     createInfo: Map[String, String] = Map.empty[String, String]
                 ): RasterGDAL = {
            val result = RasterGDAL(createInfo, exprConfigOpt)
            result.updateDataset(dataset) // <- will internally configure.
            result
        }

}
