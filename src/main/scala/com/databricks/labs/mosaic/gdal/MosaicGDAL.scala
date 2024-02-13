package com.databricks.labs.mosaic.gdal

import com.databricks.labs.mosaic.functions.{MosaicContext, MosaicExpressionConfig}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.gdal.gdal.gdal

import java.io.{BufferedInputStream, File, PrintWriter}
import java.nio.file.{Files, Paths}
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

    // noinspection ScalaWeakerAccess
    val GDAL_ENABLED = "spark.mosaic.gdal.native.enabled"
    var isEnabled = false

    /** Returns true if GDAL is enabled. */
    def wasEnabled(spark: SparkSession): Boolean =
        spark.conf.get(GDAL_ENABLED, "false").toBoolean || sys.env.getOrElse("GDAL_ENABLED", "false").toBoolean

    /** Configures the GDAL environment. */
    def configureGDAL(mosaicConfig: MosaicExpressionConfig): Unit = {
        val CPL_TMPDIR = MosaicContext.tmpDir
        val GDAL_PAM_PROXY_DIR = MosaicContext.tmpDir
        gdal.SetConfigOption("GDAL_VRT_ENABLE_PYTHON", "YES")
        gdal.SetConfigOption("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")
        gdal.SetConfigOption("CPL_TMPDIR", CPL_TMPDIR)
        gdal.SetConfigOption("GDAL_PAM_PROXY_DIR", GDAL_PAM_PROXY_DIR)
        gdal.SetConfigOption("GDAL_PAM_ENABLED", "YES")
        gdal.SetConfigOption("CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE", "NO")
        gdal.SetConfigOption("CPL_LOG", s"$CPL_TMPDIR/gdal.log")
        mosaicConfig.getGDALConf.foreach { case (k, v) => gdal.SetConfigOption(k.split("\\.").last, v) }
    }

    /** Enables the GDAL environment. */
    def enableGDAL(spark: SparkSession): Unit = {
        if (!wasEnabled(spark) && !isEnabled) {
            Try {
                isEnabled = true
                loadSharedObjects()
                val expressionConfig = MosaicExpressionConfig(spark)
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
        }
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

    /** Reads the resource bytes. */
    private def readResourceBytes(name: String): Array[Byte] = {
        val bis = new BufferedInputStream(getClass.getResourceAsStream(name))
        try { Stream.continually(bis.read()).takeWhile(-1 !=).map(_.toByte).toArray }
        finally bis.close()
    }

    /** Reads the resource lines. */
    // noinspection SameParameterValue
    private def readResourceLines(name: String): Array[String] = {
        val bytes = readResourceBytes(name)
        val lines = new String(bytes).split("\n")
        lines
    }
}
