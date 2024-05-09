package com.databricks.labs.mosaic.gdal

import com.databricks.labs.mosaic.{MOSAIC_RASTER_BLOCKSIZE_DEFAULT, MOSAIC_RASTER_CHECKPOINT, MOSAIC_RASTER_USE_CHECKPOINT, MOSAIC_TEST_MODE}
import com.databricks.labs.mosaic.functions.{MosaicContext, MosaicExpressionConfig}
import com.databricks.labs.mosaic.utils.PathUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.gdal.gdal.gdal
import org.gdal.osr.SpatialReference

import java.io.{File, FileNotFoundException}
import java.nio.file.{Files, InvalidPathException, Paths}
import scala.language.postfixOps
import scala.util.Try

//noinspection DuplicatedCode
/** GDAL environment preparation and configuration. */
object MosaicGDAL extends Logging {

    private val usrlibsoPath = "/usr/lib/libgdal.so"
    private val usrlibso30Path = "/usr/lib/libgdal.so.30"
    private val usrlibso3003Path = "/usr/lib/libgdal.so.30.0.3"
    private val libjnisoPath = "/usr/lib/libgdalalljni.so"
    private val libjniso30Path = "/usr/lib/libgdalalljni.so.30"
    private val libjniso3003Path = "/usr/lib/libgdalalljni.so.30.0.3"
    private val libogdisoPath = "/usr/lib/ogdi/4.1/libgdal.so"

    val defaultBlockSize = 1024
    val vrtBlockSize = 128 // This is a must value for VRTs before GDAL 3.7
    var blockSize: Int = MOSAIC_RASTER_BLOCKSIZE_DEFAULT.toInt

    // noinspection ScalaWeakerAccess
    val GDAL_ENABLED = "spark.mosaic.gdal.native.enabled"
    var isEnabled = false
    var checkpointPath: String = _
    var useCheckpoint: Boolean = _

    // Only use this with GDAL rasters
    val WSG84: SpatialReference = {
        val wsg84 = new SpatialReference()
        wsg84.ImportFromEPSG(4326)
        wsg84.SetAxisMappingStrategy(org.gdal.osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        wsg84
    }

    /** Returns true if GDAL is enabled. */
    def wasEnabled(spark: SparkSession): Boolean =
        spark.conf.get(GDAL_ENABLED, "false").toBoolean || sys.env.getOrElse("GDAL_ENABLED", "false").toBoolean

    /** Configures the GDAL environment. */
    def configureGDAL(mosaicConfig: MosaicExpressionConfig): Unit = {
        val CPL_TMPDIR = MosaicContext.tmpDir(mosaicConfig)
        val GDAL_PAM_PROXY_DIR = MosaicContext.tmpDir(mosaicConfig)
        gdal.SetConfigOption("GDAL_VRT_ENABLE_PYTHON", "YES")
        gdal.SetConfigOption("GDAL_DISABLE_READDIR_ON_OPEN", "TRUE")
        gdal.SetConfigOption("CPL_TMPDIR", CPL_TMPDIR)
        gdal.SetConfigOption("GDAL_PAM_PROXY_DIR", GDAL_PAM_PROXY_DIR)
        gdal.SetConfigOption("GDAL_PAM_ENABLED", "YES")
        gdal.SetConfigOption("CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE", "NO")
        gdal.SetConfigOption("CPL_LOG", s"$CPL_TMPDIR/gdal.log")
        gdal.SetConfigOption("GDAL_CACHEMAX", "512")
        gdal.SetConfigOption("GDAL_NUM_THREADS", "ALL_CPUS")
        mosaicConfig.getGDALConf.foreach { case (k, v) => gdal.SetConfigOption(k.split("\\.").last, v) }
        setBlockSize(mosaicConfig)
        configureCheckpoint(mosaicConfig)
    }

    def configureCheckpoint(mosaicConfig: MosaicExpressionConfig): Unit = {
        this.checkpointPath = mosaicConfig.getRasterCheckpoint
        this.useCheckpoint = mosaicConfig.isRasterUseCheckpoint
    }

    def setBlockSize(mosaicConfig: MosaicExpressionConfig): Unit = {
        val blockSize = mosaicConfig.getRasterBlockSize
        if (blockSize > 0) {
            this.blockSize = blockSize
        }
    }

    def setBlockSize(size: Int): Unit = {
        if (size > 0) {
            this.blockSize = size
        }
    }

    /** Enables the GDAL environment. */
    def enableGDAL(spark: SparkSession): Unit = {
        // refresh configs in case spark had changes
        val expressionConfig = MosaicExpressionConfig(spark)

        if (!wasEnabled(spark) && !isEnabled) {
            Try {
                isEnabled = true
                loadSharedObjects()
                configureGDAL(expressionConfig)
                gdal.AllRegister()
                spark.conf.set(GDAL_ENABLED, "true")
            } match {
                case scala.util.Success(_)         => logInfo("GDAL environment enabled successfully.")
                case scala.util.Failure(exception) =>
                    logError("GDAL not enabled. Mosaic with GDAL requires that GDAL be installed on the cluster.")
                    logError("Please run setup_gdal() to generate the init script for install GDAL install.")
                    logError("After the init script is generated, please restart the cluster with the init script to complete the setup.")
                    logError(s"Error: ${exception.getMessage}")
                    isEnabled = false
                    throw exception
            }
        } else {
            configureCheckpoint(expressionConfig)
        }
    }

    /**
      * Enables the GDAL environment with checkpointing.
      * - alternative to setting spark configs prior to init.
      * - can be called multiple times in a session as you want to change
      *   checkpoint location.
      * - sets [[checkpointPath]] to provided path.
      * - sets [[useCheckpoint]] to "true".
      * @param spark
      *   spark session to use.
      * @param withCheckpointPath
      *   path to set.
      */
    def enableGDALWithCheckpoint(spark: SparkSession, withCheckpointPath: String): Unit = {
        // [a] initial checks
        // - will make dirs if conditions met
        val isTestMode = spark.conf.get(MOSAIC_TEST_MODE, "false").toBoolean
        if (withCheckpointPath == null) {
            val msg = "Null checkpoint path provided."
            logError(msg)
            throw new NullPointerException(msg)
        } else if (!isTestMode && !PathUtils.isFuseLocation(withCheckpointPath)) {
            val msg = "Checkpoint path must be a (non-local) fuse location."
            logError(msg)
            throw new InvalidPathException(withCheckpointPath, msg)
        } else if (!Files.exists(Paths.get(withCheckpointPath))) {
            if (withCheckpointPath.startsWith("/Volumes/")) {
                val msg = "Volume checkpoint path doesn't exist and must be created through Databricks catalog."
                logError(msg)
                throw new FileNotFoundException(msg)
            } else {
                val dir = new File(withCheckpointPath)
                dir.mkdirs
            }
        }

        // [b] set spark configs for checkpoint
        // - will make sure the session is consistent with these settings.
        spark.conf.set(MOSAIC_RASTER_CHECKPOINT, withCheckpointPath)
        spark.conf.set(MOSAIC_RASTER_USE_CHECKPOINT, "true")

        // [c] enable gdal from configs
        enableGDAL(spark)
        logInfo(s"Checkpoint enabled for this session under $checkpointPath (overrides existing spark confs).")

        // [d] re-register expressions
        // - needed to update the MosaicConfig used.
        Try {
            MosaicContext.context().register(spark)
            logInfo("... re-registered expressions")
        }.getOrElse(
          logWarning("...Unable to re-register expressions (is MosaicContext initialized?)")
        )

    }

    /** Loads the shared objects required for GDAL. */
    private def loadSharedObjects(): Unit = {
        loadOrNOOP(usrlibsoPath)
        loadOrNOOP(usrlibso30Path)
        loadOrNOOP(usrlibso3003Path)
        loadOrNOOP(libjnisoPath)
        loadOrNOOP(libjniso30Path)
        loadOrNOOP(libjniso3003Path)
        loadOrNOOP(libogdisoPath)
    }

    /** Loads the shared object if it exists. */
    // noinspection ScalaStyle
    private def loadOrNOOP(path: String): Unit = {
        try {
            if (Files.exists(Paths.get(path))) System.load(path)
        } catch {
            case t: Throwable =>
                println(t.toString)
                println(s"Failed to load $path")
                logWarning(s"Failed to load $path", t)
        }
    }

    /** @return value of useCheckpoint. */
    def isUseCheckpoint: Boolean = this.useCheckpoint

    /** return value of checkpointPath. */
    def getCheckpointPath: String = this.checkpointPath

}
