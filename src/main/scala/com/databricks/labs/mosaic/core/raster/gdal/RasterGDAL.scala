package com.databricks.labs.mosaic.core.raster.gdal

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.io.RasterIO
import com.databricks.labs.mosaic.core.raster.operator.clip.RasterClipByVector
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.POLYGON
import com.databricks.labs.mosaic.gdal.MosaicGDAL
import com.databricks.labs.mosaic._
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
 * constructs with `createInfoInit` and then nothing else happens until the object is
 * used.
 *
 * @param createInfoInit
 *   - Init Map[String. String] (immutable)
 *   - Internally, allows KV modification through the life of the tile:
 *     e.g. if one of the `updateCreateInfo*` functions called.
 * @param exprConfigOpt
 *    Option [[ExprConfig]]
 */
case class RasterGDAL(
                         createInfoInit: Map[String, String],
                         exprConfigOpt: Option[ExprConfig]
                     ) extends RasterIO {

    val DIR_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

    // Factory for creating CRS objects
    protected val crsFactory: CRSFactory = new CRSFactory

    // identify an intentionally empty [[RasterGDAL]]
    private var emptyRasterGDAL = false

    // See [[RasterIO]] for public APIs using these
    var fuseDirOpt: Option[String] = None

    // datasetGDAL will maintain:
    // (1) the 'path' [[String]] from which it was loaded (also 'parentPath')
    // (2) the 'driverShortName' [[String]] used to load
    // (3) the 'dataset' [[Dataset]] itself may or may not be hydrated
    // (4) additional core and extra createInfo KVs
    val datasetGDAL = DatasetGDAL(createInfoInit)

    /** @inheritdoc */
    override def tryInitAndHydrate(): RasterGDAL = {
        this.getDatasetOpt() // <- hydrate attempted (if needed)
        this // fluent
    }

    /** @inheritdoc */
    override def isDatasetHydrated: Boolean = datasetGDAL.isHydrated

    /**
     * Flags needing to be handled are init | dataset | path.
     *   - The strategy is to load [[Dataset]], then write to fuse dir.
     *
     * @return
     * `this` fluent (for internal use).
     */
    private def tryHydrate(): RasterGDAL =
        Try {
            // If empty (not a "real" [[RasterGDAL]] object), don't do anything.
            // If already hydrated, don't do anything.
            if (!this.isEmptyRasterGDAL && !this.isDatasetHydrated) {
                RasterIO.rawPathAsDatasetOpt(this.getRawPath, this.getSubNameOpt, this.getDriverNameOpt, exprConfigOpt) match {
                    case Some(dataset) =>
                        this.updateDataset(dataset)
                    case _ =>
                        this.updateError(s"handleFlags - expected path '${this.getRawPath}' to load to dataset, " +
                            s"but it did not: hydrated? ${this.isDatasetHydrated}")
                }
            }
            this
        }.getOrElse(this)

    // ///////////////////////////////////////
    // GDAL Dataset
    // ///////////////////////////////////////

    /** @return freshly calculated memSize from the (latest) internal path. */
    def calcMemSize(): Long = {
        this.updateMemSize(-1)
        this.refreshMemSize
    }

    /**
     * For the provided geometry and CRS, get bounding box polygon.
     *
     * @param geometryAPI
     * Default is JTS.
     * @param destCRS
     * CRS for the bbox, default is [[MosaicGDAL.WSG84]].
     * @param skipTransform
     * Whether to ignore Spatial Reference on source (as-provided data); this is useful
     * for data that does not have SRS but nonetheless conforms to `destCRS`, (default is false).
     * @return
     * Returns [[MosaicGeometry]] representing bounding box polygon, default
     * is empty polygon as WKB.
     */
    def bbox(geometryAPI: GeometryAPI, destCRS: SpatialReference = MosaicGDAL.WSG84, skipTransform: Boolean = false): MosaicGeometry =
        Try {
            val gt = this.getGeoTransformOpt.get
            val bbox = geometryAPI.geometry(
                Seq(
                    Seq(gt(0), gt(3)),
                    Seq(gt(0) + gt(1) * xSize, gt(3)),
                    Seq(gt(0) + gt(1) * xSize, gt(3) + gt(5) * ySize),
                    Seq(gt(0), gt(3) + gt(5) * ySize)
                ).map(geometryAPI.fromCoords),
                POLYGON
            )
            val geom = org.gdal.ogr.ogr.CreateGeometryFromWkb(bbox.toWKB)
            if (!skipTransform) {
                // source CRS defaults to WGS84
                val sourceCRS = this.getSpatialReference
                if (sourceCRS.GetName() != destCRS.GetName()) {
                    // perform transform if needed
                    // - transform is "in-place", so same object
                    val transform = new osr.CoordinateTransformation(sourceCRS, destCRS)
                    geom.Transform(transform)
                }
            }
            val result = geometryAPI.geometry(geom.ExportToWkb(), "WKB")

            result
        }.getOrElse(geometryAPI.geometry(POLYGON_EMPTY_WKT, "WKT"))

    /** @return The diagonal size of a tile. */
    def diagSize: Double = math.sqrt(xSize * xSize + ySize * ySize)

    // noinspection ZeroIndexToHead

    /**
     * @return
     * Returns the tile's extent as a Seq(xmin, ymin, xmax, ymax), default
     * all 0s.
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
            Option(this.getDatasetOrNull().GetMetadata_Dict("IMAGE_STRUCTURE"))
                .map(_.asScala.toMap.asInstanceOf[Map[String, String]])
                .get("COMPRESSION")
        }.getOrElse("None")

    /** @return Returns a tuple with the tile's size. */
    def getDimensions: (Int, Int) = (xSize, ySize)

    /** @return Returns the tile's geotransform as a Option Seq. */
    def getGeoTransformOpt: Option[Array[Double]] =
        Try {
            this.getDatasetOrNull().GetGeoTransform()
        }.toOption

    /**
     * @return
     * Returns the total bytes based on pixels * datatype per band, can be
     * alt to memsize, default is -1.
     */
    def getPixelBytesCount: Long =
        Try {
            (1 to this.numBands)
                .map(i => this.getDatasetOrNull().GetRasterBand(i))
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
     *
     * @return
     * Raster's [[SpatialReference]] object.
     */
    def getSpatialReference: SpatialReference =
        Try {
            val srs = this.getDatasetOrNull().GetSpatialRef // <- dataset available
            if (srs != null) srs // <- SRS available
            else MosaicGDAL.WSG84 // <- SRS not available
        }.getOrElse(MosaicGDAL.WSG84) // <- dataset not available

    /** @return Returns a map of tile band(s) valid pixel count, default 0. */
    def getValidCount: Map[Int, Long] =
        Try {
            (1 to numBands)
                .map(i => {
                    val band = this.getDatasetOrNull().GetRasterBand(i)
                    val validCount = band.AsMDArray().GetStatistics().getValid_count
                    i -> validCount
                })
                .toMap
        }.getOrElse(Map.empty[Int, Long])

    /**
     * @return
     * True if the tile is empty, false otherwise. May be expensive to
     * compute since it requires reading the tile and computing statistics.
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
                    .map(bp => RasterGDAL(
                        this.getCreateInfo(includeExtras = false) +
                            (RASTER_PATH_KEY -> bp), exprConfigOpt)
                    )
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
        this.getDatasetOpt() match {
            case Some(_) =>
                datasetGDAL.metadata
            case _ =>
                Map.empty[String, String]
        }
    }.getOrElse(Map.empty[String, String])

    /** @return Returns the tile's number of bands, defaults to 0. */
    def numBands: Int =
        Try {
            this.getDatasetOrNull().GetRasterCount()
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
            this.getDatasetOrNull().GetSpatialRef.ExportToProj4
        }.getOrElse("")

    /**
     * 0.4.3 file memory size or pixel size * datatype over bands; r returns -1
     * if those are unobtainable.
     *
     * @return
     * Returns the amount of memory occupied by the file in bytes or
     * estimated size.
     */
    def refreshMemSize: Long = {
        if (this.getDatasetOrNull() != null && this.getMemSize == -1) {
            val toRead = getPathGDAL.asFileSystemPath

            val sz: Long = Try {
                if (Files.notExists(Paths.get(toRead))) this.getPixelBytesCount
                else Files.size(Paths.get(toRead))
            }.getOrElse(-1L)
            if (sz > -1) this.updateMemSize(sz)
        }
        this.getMemSize
    }

    /**
     * Sets the tile's SRID. This is the EPSG code of the tile's CRS.
     *   - this is an in-place op in 0.4.3+.
     *
     * @param dataset
     * The [[Dataset]] to update the SRID
     * @param srid
     * The srid to set.
     * @return
     * `this` [[RasterGDAL]] (fluent).
     */
    def setSRID(srid: Int): RasterGDAL =
        Try {
            // (1) attempt dataset hydration
            this.getDatasetOpt() match {
                case Some(dataset) =>
                    // (2) srs from srid
                    var srs: SpatialReference = null
                    if (srid == 0 || srid == 4326) {
                        srs = MosaicGDAL.WSG84
                    } else {
                        srs = new osr.SpatialReference()
                        srs.ImportFromEPSG(srid)
                    }

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
                        this.updateLastCmd("setSRID")
                        this.updateRawParentPath(this.getRawPath) // <- path to parent path
                        this.updateRawPath(tmpPath) // <- tmp to path
                        this.updateDriverName(tmpDriverSN) // <- driver name

                    } finally {
                        tmpDriver.delete()
                        this.flushAndDestroy() // <- make sure all written to path
                    }
                case _ =>
                    // handle dataset is None
                    this.updateLastCmd("setSRID")
                    this.updateError("setSRID - `datasetGDAL.getDatasetOpt` unsuccessful")
            }

            // (6) for external callers
            // - return a `this` object populated with the same path
            this
        }.getOrElse {
            this.updateLastCmd("setSRID")
            this.updateError("setSRID - initAndHydrate unsuccessful")
            this
        }

    /**
     * @return
     * Returns the tile's SRID. This is the EPSG code of the tile's CRS.
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
            this.getDatasetOrNull().GetRasterXSize
        }.getOrElse(0)

    /** @return Returns the min y coordinate. */
    def yMin: Double = originY

    /** @return Returns the max y coordinate. */
    def yMax: Double = originY + ySize * pixelYSize

    /** @return Returns y size of the tile, default 0. */
    def ySize: Int =
        Try {
            this.getDatasetOrNull().GetRasterYSize
        }.getOrElse(0)

    // ///////////////////////////////////////
    // Subdataset Functions
    // ///////////////////////////////////////

    /** @return subdataset name as string (default is empty). */
    def getSubsetName: String = datasetGDAL.getSubsetName

    /** @return Option subdataset name as string (default is None). */
    def getSubNameOpt: Option[String] = datasetGDAL.getSubNameOpt

    /** @return whether this is a subdataset. */
    def isSubdataset: Boolean = datasetGDAL.isSubdataset

    /** @return Returns the tile's subdatasets as a Map, default empty. */
    def subdatasets: Map[String, String] =
        Try {
            this.getDatasetOpt() match {
                case Some(_) =>
                    // use parent if it exists; otherwise path
                    if (getParentPathGDAL.isPathSetAndExists) datasetGDAL.subdatasets(getParentPathGDAL)
                    else datasetGDAL.subdatasets(getPathGDAL)
                case _ =>
                    Map.empty[String, String]
            }
        }.getOrElse(Map.empty[String, String])

    /**
     * Set the subdataset name.
     * - This is a simple setter, for referencing.
     * - Only set on datasetGDAL for single storage / ownership.
     *
     * @param name
     * Name of the subdataset.
     * @return
     * [[RasterGDAL]] `this` (fluent).
     */
    def updateSubsetName(name: String): RasterGDAL = {
        datasetGDAL.updateSubsetName(name)
        this
    }

    // ///////////////////////////////////////
    // Band Functions
    // ///////////////////////////////////////

    /**
     * @param bandId
     * The band index to read.
     * @return
     * Returns the tile's band as a [[RasterBandGDAL]] object.
     */
    def getBand(bandId: Int): RasterBandGDAL = {
        // TODO 0.4.3 - Throw exception or return empty ?
        if (bandId > 0 && this.numBands >= bandId) {
            RasterBandGDAL(this.getDatasetOrNull().GetRasterBand(bandId), bandId)
        } else {
            throw new ArrayIndexOutOfBoundsException()
        }
    }

    /** @return a previously set band number as option int (default None). */
    def getBandIdxOpt: Option[Int] = {
        datasetGDAL.bandIdxOpt
    }

    /** @return Returns the tile's bands as a Seq, defaults to empty Seq. */
    def getBands: Seq[RasterBandGDAL] = Try {
        (1 to this.numBands).map(this.getBand)
    }.getOrElse(Seq.empty[RasterBandGDAL])

    /**
     * @return
     * Returns a map of the tile band(s) statistics, default empty.
     */
    def getBandStats: Map[Int, Map[String, Double]] =
        Try {
            (1 to numBands)
                .map(i => {
                    val band = this.getDatasetOrNull().GetRasterBand(i)
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
    def updateBandIdx(num: Int): RasterGDAL = {
        this.tryInitAndHydrate() // <- need dataset hydrated for metadata set
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
     *
     * @param kernel
     * [[Array[Double]]] kernel to apply to the tile.
     * @return
     * New [[RasterGDAL]] object with kernel applied.
     */
    def convolve(kernel: Array[Array[Double]]): RasterGDAL =
        Try {
            // (1) hydrate the dataset
            this.tryInitAndHydrate()

            // (2) write dataset to tmpPath
            // - This will be populated as we operate on the tmpPath
            // TODO Should this be `datasetOrPathCopy` ???
            val tmpPath = RasterIO.createTmpFileFromDriver(this.getDriverName(), exprConfigOpt)

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
                                    case _ => NO_PATH_STRING
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
                result.updateLastCmd("convolve")
                result.updateError("convolve - datasetCopyToPath = false")

                result
            }
        }.getOrElse {
            val result = RasterGDAL()
            result.updateLastCmd("convolve")
            result.updateError("convolve - kernel unsuccessful")

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
            this.tryInitAndHydrate()

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
                result.updateLastCmd("filter")
                result.updateError("filter - datasetCopyToPath = false")

                result
            }
        }.getOrElse {
            val result = RasterGDAL()
            result.updateLastCmd("filter")
            result.updateError("filter - kernel unsuccessful")

            result
        }

    /**
     * Applies clipping to get cellid tile.
     *
     * @param cellID
     *   Clip the tile based on the cell id geometry.
     * @param indexSystem
     *   Default is H3.
     * @param geometryAPI
     *   Default is JTS.
     * @param skipProject
     *   Whether to ignore Spatial Reference on source (as-provided data); this is useful
     *   for data that does not have SRS but nonetheless conforms to index, (default is false).
     * @return
     *   New [[RasterGDAL]] for a given cell ID. Used for tessellation.
     */
    def getRasterForCell(cellID: Long, indexSystem: IndexSystem, geometryAPI: GeometryAPI, skipProject: Boolean = false): RasterGDAL = {
        val cellGeom = indexSystem.indexToGeometry(cellID, geometryAPI)
        val geomCRS = indexSystem.osrSpatialRef
        RasterClipByVector.clip(
            this,
            cellGeom,
            geomCRS,
            geometryAPI,
            exprConfigOpt,
            skipProject = skipProject
        )
    }

    /**
     * Get a particular subdataset by name.
     * - This does not generate a new file.
     * - It hydrates the dataset with the subset.
     * - It also updates the path to include the subset.
     * @param subsetName
     *   The name of the subdataset to get.
     * @return
     *   Returns new [[RasterGDAL]].
     */
    def getSubdataset(subsetName: String): RasterGDAL =
        Try {
            // try to get the subdataset requested
            // - allow failure on extracting subdataset,
            // then handle with empty [[RasterGDAL]]
            this.tryInitAndHydrate()
            val dsGDAL = datasetGDAL.getSubdatasetObj(getPathGDAL, subsetName, exprConfigOpt)

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
            result.updateError(
                s"RasterGDAL - getSubdatasetName '$subsetName' unable to be loaded to dataset",
                fullMsg = s"""
                             |Subdataset $subsetName not found!
                             |Available subdatasets:
                             |     ${subdatasets.keys.filterNot (_.startsWith ("SUBDATASET_") ).mkString (", ")}
                             |     """.stripMargin
            )

            result
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

    /** @inheritdoc */
    override def finalizeRaster(toFuse: Boolean): RasterGDAL =
        Try {
            // (1) write if current path not fuse or not under the expected dir
            if (
                (!this.isEmptyRasterGDAL && toFuse) &&
                    (!this.getPathGDAL.isFusePath || !this.isRawPathInFuseDir)
            ) {
                // (2) hydrate the dataset
                this.tryInitAndHydrate()

                val driverSN = this.getDriverName()
                val ext = GDAL.getExtension(driverSN)
                val newDir = this.makeNewFuseDir(ext, uuidOpt = None)

                datasetGDAL.datasetOrPathCopy(newDir, doDestroy = true, skipUpdatePath = true) match {
                    case Some(newPath) =>
                        // for clarity, handling update here
                        this.updateRawPath(newPath)
                    case _ =>
                        this.updateLastCmd("finalizeRaster")
                        this.updateError(s"finalizeRaster - fuse write")
                }
            }

            // (3) return this
            this
        }.getOrElse {
            // (4) return empty this
            if (!this.isEmptyRasterGDAL) {
                this.updateLastCmd("finalizeRaster")
                this.updateError(s"finalizeRaster - exception - fuse write")
            }
            this
        }

    /** @inheritdoc */
    override def isRawPathInFuseDir: Boolean =
        Try {
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
        Try(datasetGDAL.getCreateInfoExtras(RASTER_LAST_ERR_KEY).nonEmpty).getOrElse(false)
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
        Files.createDirectories(Paths.get(dir))
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
        datasetGDAL.updateDataset(dataset, doUpdateDriver) // <- only update driver if not null
        this
    }

    // /////////////////////////////////////////////////
    // Additional Getters + Updaters
    // /////////////////////////////////////////////////

    /** Returns immutable internal map, representing `createInfo` at initialization (not the lastest). */
    def getCreateInfoFromInit: Map[String, String] = createInfoInit

    /** Returns immutable internal map, representing latest KVs (blends from `datasetGDAL`). */
    def getCreateInfo(includeExtras: Boolean): Map[String, String] = {
        datasetGDAL.asCreateInfo(includeExtras)
    }

    /** Return [[datasetGDAL]]. */
    def getDatasetGDAL: DatasetGDAL = datasetGDAL

    /** Return the 'path' [[PathGDAL]] (within [[datasetGDAL]]). */
    def getPathGDAL: PathGDAL = getDatasetGDAL.pathGDAL

    /** Return the 'parentPath' [[PathGDAL]] (within [[datasetGDAL]]). */
    def getParentPathGDAL: PathGDAL = getDatasetGDAL.parentPathGDAL

    /** @inheritdoc */
    override def getDatasetOpt(): Option[Dataset] = {
        this.tryHydrate()
        datasetGDAL.getDatasetOpt
    }

    /** @inheritdoc */
    override def getDriverNameOpt: Option[String] = {
        val dn = datasetGDAL.getDriverName
        if (dn == NO_DRIVER) None
        else Some(dn)
    }

    /**
     * @return
     *   The tile's path on disk, or NO_PATH_STRING. Usually this is a parent
     *   file for the tile.
     */
    def getRawParentPath: String = {
        datasetGDAL.getParentPath
    }

    /** @return Returns the tile's path, or NO_PATH_STRING. */
    def getRawPath: String = {
        datasetGDAL.getPath
    }

    /** @return memSize (from CreateInfo) */
    def getMemSize: Long = {
        Try(datasetGDAL.getCreateInfoExtras(RASTER_MEM_SIZE_KEY).toLong).getOrElse(-1)
    }

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

    /** Set empty indicator for the object (not the dataset), returns [[RasterGDAL]] (fluent). */
    def setEmptyRasterGDAL(empty: Boolean): RasterGDAL = {
        emptyRasterGDAL = empty
        this
    }

    /** @inheritdoc */
    override def setFuseDirOpt(dirOpt: Option[String]): RasterGDAL = {
        this.fuseDirOpt = dirOpt
        this
    }

    /** Update the internal map, return `this` (fluent). */
    def updateCreateInfo(newMap: Map[String, String]): RasterGDAL = {
        datasetGDAL.updateCreateInfo(newMap)
        this
    }

    /** Update driver on internal map + `datasetGDAL`, return `this` (fluent). */
    def updateDriverName(driverName: String): RasterGDAL = {
        datasetGDAL.updateDriverName(driverName)
        this
    }

    /** Update path on internal map + `datasetGDAL`, return `this` (fluent). */
    def updateRawPath(rawPath: String): RasterGDAL = {
        datasetGDAL.updatePath(rawPath)
        this
    }

    /** Update parentPath on internal map + `datasetGDAL`, return `this` (fluent). */
    def updateRawParentPath(rawParentPath: String): RasterGDAL = {
        datasetGDAL.updateParentPath(rawParentPath)
        this
    }

    /** Update last command on internal map, return `this` (fluent). */
    def updateLastCmd(cmd: String): RasterGDAL = {
        datasetGDAL.updateCreateInfoEntry(RASTER_LAST_CMD_KEY, cmd, extrasOnly = true)
        this
    }

    /** Update last error on internal map, return `this` (fluent). */
    def updateError(msg: String, fullMsg: String = ""): RasterGDAL = {
        datasetGDAL.updateCreateInfoEntry(RASTER_LAST_ERR_KEY, msg, extrasOnly = true)
        datasetGDAL.updateCreateInfoEntry(RASTER_FULL_ERR_KEY, fullMsg, extrasOnly = true)
        this
    }

    /** Update last command on internal map, return `this` (fluent). */
    def updateAllParents(parents: String): RasterGDAL = {
        datasetGDAL.updateCreateInfoEntry(RASTER_ALL_PARENTS_KEY, parents, extrasOnly = true)
        this
    }

    /** Update last error on internal map, return `this` (fluent). */
    def updateMemSize(sz: Long): RasterGDAL = {
        datasetGDAL.updateCreateInfoEntry(RASTER_MEM_SIZE_KEY, sz.toString, extrasOnly = true)
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
        result.updateLastCmd("emptyRasterGDAL")
        result.updateError("emptyRasterGDAL = true")
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
