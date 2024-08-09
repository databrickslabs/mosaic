package com.databricks.labs.mosaic.gdal

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystemFactory
import com.databricks.labs.mosaic.core.raster.io.CleanUpManager
import com.databricks.labs.mosaic.{
    MOSAIC_CLEANUP_AGE_LIMIT_DEFAULT,
    MOSAIC_CLEANUP_AGE_LIMIT_MINUTES,
    MOSAIC_RASTER_BLOCKSIZE_DEFAULT,
    MOSAIC_RASTER_CHECKPOINT,
    MOSAIC_RASTER_CHECKPOINT_DEFAULT,
    MOSAIC_RASTER_TMP_PREFIX_DEFAULT,
    MOSAIC_RASTER_USE_CHECKPOINT,
    MOSAIC_RASTER_USE_CHECKPOINT_DEFAULT,
    MOSAIC_TEST_MODE
}
import com.databricks.labs.mosaic.functions.{ExprConfig, MosaicContext}
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

    /** update this var each time `config*` is invoked. */
    var exprConfigOpt: Option[ExprConfig] = None

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
    private val GDAL_ENABLED = "spark.mosaic.gdal.native.enabled"
    private var enabled = false
    private var checkpointDir: String = MOSAIC_RASTER_CHECKPOINT_DEFAULT
    private var useCheckpoint: Boolean = MOSAIC_RASTER_USE_CHECKPOINT_DEFAULT.toBoolean
    private var localRasterDir: String = s"$MOSAIC_RASTER_TMP_PREFIX_DEFAULT/mosaic_tmp"
    private var cleanUpAgeLimitMinutes: Int = MOSAIC_CLEANUP_AGE_LIMIT_DEFAULT.toInt
    private var manualMode: Boolean = true

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
    def configureGDAL(exprConfigOpt: Option[ExprConfig]): Unit = {
        val CPL_TMPDIR = MosaicContext.getTmpSessionDir(exprConfigOpt)
        val GDAL_PAM_PROXY_DIR = MosaicContext.getTmpSessionDir(exprConfigOpt)
        gdal.SetConfigOption("GDAL_VRT_ENABLE_PYTHON", "YES")
        gdal.SetConfigOption("GDAL_DISABLE_READDIR_ON_OPEN", "TRUE")
        gdal.SetConfigOption("CPL_TMPDIR", CPL_TMPDIR)
        gdal.SetConfigOption("GDAL_PAM_PROXY_DIR", GDAL_PAM_PROXY_DIR)
        gdal.SetConfigOption("GDAL_PAM_ENABLED", "YES")
        gdal.SetConfigOption("CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE", "NO")
        gdal.SetConfigOption("CPL_LOG", s"$CPL_TMPDIR/gdal.log")
        gdal.SetConfigOption("GDAL_CACHEMAX", "512")
        gdal.SetConfigOption("GDAL_NUM_THREADS", "ALL_CPUS")
        exprConfigOpt match {
            case Some(exprConfig) =>
                exprConfig.getGDALConf.foreach { case (k, v) => gdal.SetConfigOption(k.split("\\.").last, v) }
            case _ => ()
        }
        setBlockSize(exprConfigOpt)
        configureCheckpoint(exprConfigOpt)
        configureLocalRasterDir(exprConfigOpt)
    }

    def configureCheckpoint(exprConfigOpt: Option[ExprConfig]): Unit = {
        this.checkpointDir = Try(exprConfigOpt.get.getRasterCheckpoint)
            .getOrElse(MOSAIC_RASTER_CHECKPOINT_DEFAULT)
        this.useCheckpoint = Try(exprConfigOpt.get.isRasterUseCheckpoint)
            .getOrElse(MOSAIC_RASTER_USE_CHECKPOINT_DEFAULT.toBoolean)
    }

    def configureLocalRasterDir(exprConfigOpt: Option[ExprConfig]): Unit = {
        this.manualMode = Try(exprConfigOpt.get.isManualCleanupMode)
            .getOrElse(false)
        this.cleanUpAgeLimitMinutes = Try(exprConfigOpt.get.getCleanUpAgeLimitMinutes)
            .getOrElse(MOSAIC_CLEANUP_AGE_LIMIT_MINUTES.toInt)

        // don't allow a fuse path
        val tmpPrefix = Try(exprConfigOpt.get.getTmpPrefix).getOrElse(MOSAIC_RASTER_TMP_PREFIX_DEFAULT)
        if (PathUtils.isFusePathOrDir(tmpPrefix, uriGdalOpt = None)) {
            throw new Error(
                s"configured tmp prefix '$tmpPrefix' must be local, " +
                    s"not fuse mounts ('/dbfs/', '/Volumes/', or '/Workspace/')")
        } else {
            this.localRasterDir = s"$tmpPrefix/mosaic_tmp"
        }

        // make sure cleanup manager thread is running
        CleanUpManager.runCleanThread()
    }


    def setBlockSize(exprConfigOpt: Option[ExprConfig]): Unit = {
        val blockSize = Try(exprConfigOpt.get.getRasterBlockSize).getOrElse(0)
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
        exprConfigOpt = Option(ExprConfig(spark))

        if (!wasEnabled(spark) && !enabled) {
            Try {
                enabled = true
                loadSharedObjects()
                configureGDAL(exprConfigOpt)
                gdal.AllRegister()
                spark.conf.set(GDAL_ENABLED, "true")
            } match {
                case scala.util.Success(_)         => logInfo("GDAL environment enabled successfully.")
                case scala.util.Failure(exception) =>
                    logError("GDAL not enabled. Mosaic with GDAL requires that GDAL be installed on the cluster.")
                    logError("Please run setup_gdal() to generate the init script for install GDAL install.")
                    logError("After the init script is generated, please restart the cluster with the init script to complete the setup.")
                    logError(s"Error: ${exception.getMessage}")
                    enabled = false
                    throw exception
            }
        } else {
            configureCheckpoint(exprConfigOpt)
            configureLocalRasterDir(exprConfigOpt)
        }
    }

    /**
      * Enables the GDAL environment with checkpointing, called from driver.
      * - alternative to setting spark configs prior to init.
      * - can be called multiple times in a session as you want to change
      *   checkpoint location.
      * - sets [[checkpointDir]] to provided directory.
      * - sets [[useCheckpoint]] to "true".
      * - see mosaic_context.py as well for use.
      * @param spark
      *   spark session to use.
      * @param withCheckpointDir
      *   path to set.
      */
    def enableGDALWithCheckpoint(spark: SparkSession, withCheckpointDir: String): Unit = {
        // - set spark config to enable checkpointing
        // - initial checks + update directory
        // - also inits MosaicContext
        // - also enables GDAL and refreshes accessors
        spark.conf.set(MOSAIC_RASTER_USE_CHECKPOINT, "true")
        updateCheckpointDir(spark, withCheckpointDir)
        logInfo(s"Checkpoint enabled for this session under $checkpointDir (overrides existing spark confs).")
    }

    /**
      * Go back to defaults.
      * - spark conf unset for use checkpoint (off).
      * - spark conf unset for checkpoint directory.
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
      * @param dir
      *   supported cloud object directory to use.
      */
    def updateCheckpointDir(spark: SparkSession, dir: String): Unit = {
        val isTestMode = spark.conf.get(MOSAIC_TEST_MODE, "false").toBoolean
        if (dir == null) {
            val msg = "Null checkpoint path provided."
            logError(msg)
            throw new NullPointerException(msg)
        } else if (!isTestMode && !PathUtils.isFusePathOrDir(dir, uriGdalOpt = None)) {
            val msg = "Checkpoint path must be a (non-local) fuse location."
            logError(msg)
            throw new InvalidPathException(dir, msg)
        } else if (!Files.exists(Paths.get(dir))) {
            if (dir.startsWith("/Volumes/")) {
                val msg = "Volume checkpoint directory doesn't exist and must be created through Databricks catalog."
                logError(msg)
                throw new FileNotFoundException(msg)
            } else {
                val d = new File(dir)
                d.mkdirs
            }
        }
        spark.conf.set(MOSAIC_RASTER_CHECKPOINT, dir)
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
            val exprConfig = ExprConfig(spark)
            val indexSystem = IndexSystemFactory.getIndexSystem(exprConfig.getIndexSystem)
            val geometryAPI =  GeometryAPI.apply(exprConfig.getGeometryAPI)
            MosaicContext.build(indexSystem, geometryAPI)
            exprConfigOpt = Option(exprConfig) // <- update the class variable
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
    private def loadOrNOOP(path: String): Unit = {
        try {
            if (Files.exists(Paths.get(path))) System.load(path)
        } catch {
            case t: Throwable =>
                // scalastyle:off println
                println(t.toString)
                println(s"Failed to load $path")
                // scalastyle:on println
                logWarning(s"Failed to load $path", t)
        }
    }

    /** @return if gdal enabled. */
    def isEnabled: Boolean = this.enabled

    /** @return if manual mode for cleanup (configured). */
    def isManualMode: Boolean = this.manualMode

    /** @return if using checkpoint (configured). */
    def isUseCheckpoint: Boolean = this.useCheckpoint

    /** @return value of checkpoint directory (configured). */
    def getCheckpointDir: String = this.checkpointDir

    /** @return default value of checkpoint directory. */
    def getCheckpointDirDefault: String = MOSAIC_RASTER_CHECKPOINT_DEFAULT

    /** @return value of local directory (configured).  */
    def getLocalRasterDir: String = this.localRasterDir

    /** @return file age limit for cleanup (configured). */
    def getCleanUpAgeLimitMinutes: Int = this.cleanUpAgeLimitMinutes

    ////////////////////////////////////////////////
    // Thread-safe Accessors
    ////////////////////////////////////////////////

    /** @return if gdal enabled. */
    def isEnabledThreadSafe: Boolean = synchronized(this.enabled)

    /** @return if manual mode for cleanup (configured). */
    def isManualModeThreadSafe: Boolean = synchronized(this.manualMode)

    /** @return if using checkpoint (configured). */
    def isUseCheckpointThreadSafe: Boolean = synchronized(this.useCheckpoint)

    /** @return value of checkpoint directory (configured). */
    def getCheckpointDirThreadSafe: String = synchronized(this.checkpointDir)

    /** @return value of local directory (configured).  */
    def getLocalRasterDirThreadSafe: String = synchronized(this.localRasterDir)

    /** @return file age limit for cleanup (configured). */
    def getCleanUpAgeLimitMinutesThreadSafe: Int = synchronized(this.cleanUpAgeLimitMinutes)
}
