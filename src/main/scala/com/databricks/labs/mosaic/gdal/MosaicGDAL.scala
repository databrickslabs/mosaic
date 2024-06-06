package com.databricks.labs.mosaic.gdal

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystemFactory
import com.databricks.labs.mosaic.{MOSAIC_RASTER_BLOCKSIZE_DEFAULT, MOSAIC_RASTER_CHECKPOINT, MOSAIC_RASTER_CHECKPOINT_DEFAULT, MOSAIC_RASTER_USE_CHECKPOINT, MOSAIC_RASTER_USE_CHECKPOINT_DEFAULT, MOSAIC_TEST_MODE}
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
/** GDAL environment preparation and configuration. Some functions only for driver. */
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
        gdal.SetConfigOption("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")
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

    /**
      * Enables the GDAL environment, called from driver.
      * - see mosaic_context.py as well for use.
      *
      * @param spark
      *   spark session to use.
      */
    def enableGDAL(spark: SparkSession): Unit = {
        // refresh configs in case spark had changes
        val mosaicConfig = MosaicExpressionConfig(spark)

        if (!wasEnabled(spark) && !isEnabled) {
            Try {
                isEnabled = true
                loadSharedObjects()
                configureGDAL(mosaicConfig)
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
            configureCheckpoint(mosaicConfig)
        }
    }

    /**
      * Enables the GDAL environment with checkpointing, called from driver.
      * - alternative to setting spark configs prior to init.
      * - can be called multiple times in a session as you want to change
      *   checkpoint location.
      * - sets [[checkpointPath]] to provided path.
      * - sets [[useCheckpoint]] to "true".
      * - see mosaic_context.py as well for use.
      * @param spark
      *   spark session to use.
      * @param withCheckpointPath
      *   path to set.
      */
    def enableGDALWithCheckpoint(spark: SparkSession, withCheckpointPath: String): Unit = {
        // - set spark config to enable checkpointing
        // - initial checks + update path
        // - also inits MosaicContext
        // - also enables GDAL and refreshes accessors
        spark.conf.set(MOSAIC_RASTER_USE_CHECKPOINT, "true")
        updateCheckpointPath(spark, withCheckpointPath)
        logInfo(s"Checkpoint enabled for this session under $checkpointPath (overrides existing spark confs).")
    }

    /**
      * Go back to defaults.
      * - spark conf unset for use checkpoint (off).
      * - spark conf unset for checkpoint path.
      * - see mosaic_context.py as well for use.
      *
      * @param spark
      *   spark session to use.
      */
    def resetCheckpoint(spark: SparkSession): Unit = {
        spark.conf.set(MOSAIC_RASTER_USE_CHECKPOINT, MOSAIC_RASTER_USE_CHECKPOINT_DEFAULT)
        spark.conf.set(MOSAIC_RASTER_CHECKPOINT, MOSAIC_RASTER_CHECKPOINT_DEFAULT)
        updateMosaicContext(spark)
    }

    /**
      * Update the checkpoint path.
      * - will make dirs if conditions met.
      * - will make sure the session is consistent with these settings.
      * - see mosaic_context.py as well for use.
      *
      * @param spark
      *   spark session to use.
      * @param path
      *   supported cloud object path to use.
      */
    def updateCheckpointPath(spark: SparkSession, path: String): Unit = {
        val isTestMode = spark.conf.get(MOSAIC_TEST_MODE, "false").toBoolean
        if (path == null) {
            val msg = "Null checkpoint path provided."
            logError(msg)
            throw new NullPointerException(msg)
        } else if (!isTestMode && !PathUtils.isFuseLocation(path)) {
            val msg = "Checkpoint path must be a (non-local) fuse location."
            logError(msg)
            throw new InvalidPathException(path, msg)
        } else if (!Files.exists(Paths.get(path))) {
            if (path.startsWith("/Volumes/")) {
                val msg = "Volume checkpoint path doesn't exist and must be created through Databricks catalog."
                logError(msg)
                throw new FileNotFoundException(msg)
            } else {
                val dir = new File(path)
                dir.mkdirs
            }
        }
        spark.conf.set(MOSAIC_RASTER_CHECKPOINT, path)
        updateMosaicContext(spark)
    }

    /**
      * Set spark config to disable checkpointing.
      *  - will make sure the session is consistent with these settings.
      *  - see mosaic_context.py as well for use.
      *
      * @param spark
      *   spark session to use.
      */
    def setCheckpointOff(spark: SparkSession): Unit = {
        spark.conf.set(MOSAIC_RASTER_USE_CHECKPOINT, "false")
        updateMosaicContext(spark)
    }

    /**
      * Set spark config to enable checkpointing.
      *  - will make sure the session is consistent with these settings.
      *  - see mosaic_context.py as well for use.
      *
      * @param spark
      *    Spark session to use.
      */
    def setCheckpointOn(spark: SparkSession): Unit = {
        spark.conf.set(MOSAIC_RASTER_USE_CHECKPOINT, "true")
        updateMosaicContext(spark)
    }

    private def updateMosaicContext(spark: SparkSession): Unit = {
        // - necessary to register with the latest context
        // - registers spark expressions with the new config
        // - will make sure the session is consistent with these settings
        if (!MosaicContext.checkContext) {
            val mosaicConfig = MosaicExpressionConfig(spark)
            val indexSystem = IndexSystemFactory.getIndexSystem(mosaicConfig.getIndexSystem)
            val geometryAPI =  GeometryAPI.apply(mosaicConfig.getGeometryAPI)
            MosaicContext.build(indexSystem, geometryAPI)
        }
        val mc = MosaicContext.context()
        mc.register(spark)
        enableGDAL(spark)
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

    /** @return value of checkpoint path. */
    def getCheckpointPath: String = this.checkpointPath

    /** @return default value of checkpoint path. */
    def getCheckpointPathDefault: String = MOSAIC_RASTER_CHECKPOINT_DEFAULT

}
