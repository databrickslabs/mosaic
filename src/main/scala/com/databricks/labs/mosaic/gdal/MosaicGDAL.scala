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

    /** Prepares the GDAL environment. 
        This will copy the init script as well as optionally copy shared object files into
        `toFuseDir`, e.g. `/Volumes/..`, `/Workspace/..`, `/dbfs/..`. It has an additional argument,
        called `jniSoFiles` to indicate whether to copy shared objects to the path as well.
        NOTES: 
          [1] If you are trying to setup for Volume using JVM write, you must:
              (a) be on a Shared Access cluster
              (b) have already added Mosaic JAR(s) to the Unity Catalog allowlist
              (c) have already setup the /Volumes path within a catalog and schema
          [2] The Mosaic python call `setup_gdal` has more default permissions to write to Volumes
    */
    def prepareEnvironment(spark: SparkSession, toFuseDir: String, jniSoFiles: Boolean): Unit = {
        if (!wasEnabled(spark) && !isEnabled) {
            Try {
                if (jniSoFiles){
                    // suitable for Volume based init script
                    copyInitScript(toFuseDir, "install-gdal-fuse.sh", "install-gdal-fuse.sh")
                    copyJNISharedObjects(toFuseDir)
                } else {
                    // preserves non-Volume based init script option
                    copyInitScript(toFuseDir, "install-gdal-databricks.sh", "mosaic-gdal-init.sh")
                }
            } match {
                case scala.util.Success(_)         => logInfo("GDAL environment prepared successfully.")
                case scala.util.Failure(exception) => 
                    if (toFuseDir.toString.startsWith("/Volumes")) {
                        logWarning("Note: Python `setup_gdal` has more default permissions for `/Volumes`.")
                    }
                    logWarning("GDAL environment preparation failed", exception)
            }
        }
    }
    
    /** Prepares the GDAL environment with copy `jniSoFiles=false`.
        This call is not suitable for Volume based init scripts.
     */
    def prepareEnvironment(spark: SparkSession, toFuseDir: String): Unit = {
        prepareEnvironment(spark, toFuseDir, false)
    }

    /** Configures the GDAL environment. */
    def configureGDAL(mosaicConfig: MosaicExpressionConfig): Unit = {
        val CPL_TMPDIR = MosaicContext.tmpDir
        val GDAL_PAM_PROXY_DIR = MosaicContext.tmpDir
        gdal.SetConfigOption("GDAL_VRT_ENABLE_PYTHON", "YES")
        gdal.SetConfigOption("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")
        gdal.SetConfigOption("CPL_TMPDIR", CPL_TMPDIR)
        gdal.SetConfigOption("GDAL_PAM_PROXY_DIR", GDAL_PAM_PROXY_DIR)
        gdal.SetConfigOption("GDAL_PAM_ENABLED", "NO")
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

    // noinspection ScalaStyle
    private def copyInitScript(toFuseDir: String, inScriptName: String, outScriptName: String): Unit = {
        val destPath = Paths.get(toFuseDir)
        if (!Files.exists(destPath)) Files.createDirectories(destPath)

        val scriptLines = readResourceLines(s"/scripts/$inScriptName")
        val w = new PrintWriter(new File(s"$toFuseDir/$outScriptName"))
        scriptLines
            .map { x => if (x.contains("__FUSE_DIR__")) x.replace("__FUSE_DIR__", toFuseDir) else x }
            .foreach(x => w.println(x))
        w.close()
    }

    // noinspection ScalaStyle
    private def copyJNISharedObjects(toFuseDir: String): Unit = {
        val destPath = Paths.get(toFuseDir)
        if (!Files.exists(destPath)) Files.createDirectories(destPath)

        // these are around 3MB each, 
        // should be ok to write bytes directly (vs streamed)
        Files.write(Paths.get(destPath.toString,"libgdalalljni.so"), readResourceBytes("/gdal/ubuntu/libgdalalljni.so"))
        Files.write(Paths.get(destPath.toString,"libgdalalljni.so.30"), readResourceBytes("/gdal/ubuntu/libgdalalljni.so.30"))
        Files.write(Paths.get(destPath.toString,"libgdalalljni.so.30.0.3"), readResourceBytes("/gdal/ubuntu/libgdalalljni.so.30.0.3"))
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
