package com.databricks.labs.mosaic.gdal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.gdal.gdal.gdal

import java.io.{BufferedInputStream, File, PrintWriter}
import java.nio.file.{Files, Paths}
import scala.language.postfixOps
import scala.util.Try

//noinspection DuplicatedCode
object MosaicGDAL extends Logging {

    private val usrlibsoPath = "/usr/lib/libgdal.so"
    private val usrlibso30Path = "/usr/lib/libgdal.so.30"
    private val usrlibso3003Path = "/usr/lib/libgdal.so.30.0.3"
    private val libjnisoPath = "/lib/jni/libgdalalljni.so"
    private val libjniso30Path = "/lib/jni/libgdalalljni.so.30"
    private val libogdisoPath = "/lib/ogdi/libgdal.so"

    // noinspection ScalaWeakerAccess
    val GDAL_ENABLED = "spark.mosaic.gdal.native.enabled"
    private val mosaicGDALPath = Files.createTempDirectory("mosaic-gdal")
    private val mosaicGDALAbsolutePath = mosaicGDALPath.toAbsolutePath.toString
    var isEnabled = false

    def wasEnabled(spark: SparkSession): Boolean = spark.conf.get(GDAL_ENABLED, "false").toBoolean

    def prepareEnvironment(spark: SparkSession, initScriptPath: String): Unit = {
        if (!wasEnabled(spark) && !isEnabled) {
            Try {
                copyInitScript(initScriptPath)
                copySharedObjects()
            } match {
                case scala.util.Success(_)         => logInfo("GDAL environment prepared successfully.")
                case scala.util.Failure(exception) => logWarning("GDAL environment preparation failed.", exception)
            }
        }
    }

    def configureGDAL(): Unit = {
        val tmpDirLocal = Files.createTempDirectory("mosaic-gdal-tmp").toAbsolutePath.toString
        gdal.SetConfigOption("GDAL_DISABLE_READDIR_ON_OPEN", "TRUE")
        gdal.SetConfigOption("CPL_TMPDIR", tmpDirLocal)
        gdal.SetConfigOption("GDAL_PAM_PROXY_DIR", tmpDirLocal)
        gdal.SetConfigOption("CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE", "YES")
        //gdal.SetConfigOption("GDAL_ENABLE_TIFF_SPLIT", "FALSE")
        //gdal.SetConfigOption("GTIFF_DIRECT_IO", "YES")
        //gdal.SetConfigOption("GTIFF_VIRTUAL_MEM_IO", "IF_ENOUGH_RAM")
    }

    def enableGDAL(spark: SparkSession): Unit = {
        if (!wasEnabled(spark) && !isEnabled) {
            Try {
                isEnabled = true
                //copySharedObjects()
                loadSharedObjects()
                configureGDAL()
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

    private def copySharedObjects(): Unit = {

        val libjniso = readResourceBytes(s"/gdal/ubuntu/$libjnisoPath")
        val libjniso30 = readResourceBytes(s"/gdal/ubuntu/$libjniso30Path")
        val libogdiso = readResourceBytes(s"/gdal/ubuntu/$libogdisoPath")
        val usrlibso = readResourceBytes(s"/gdal/ubuntu/$usrlibsoPath")
        val usrlibso30 = readResourceBytes(s"/gdal/ubuntu/$usrlibso30Path")
        val usrlibso3003 = readResourceBytes(s"/gdal/ubuntu/$usrlibso3003Path")

        if (!Files.exists(Paths.get("/usr/lib"))) Files.createDirectories(Paths.get("/usr/lib"))
        if (!Files.exists(Paths.get("/lib/jni"))) Files.createDirectories(Paths.get("/lib/jni"))
        if (!Files.exists(Paths.get("/lib/ogdi"))) Files.createDirectories(Paths.get("/lib/ogdi"))

        if (!Files.exists(Paths.get(usrlibsoPath))) Files.write(Paths.get(usrlibsoPath), usrlibso)
        if (!Files.exists(Paths.get(usrlibso30Path))) Files.write(Paths.get(usrlibso30Path), usrlibso30)
        if (!Files.exists(Paths.get(usrlibso3003Path))) Files.write(Paths.get(usrlibso3003Path), usrlibso3003)
        if (!Files.exists(Paths.get(libjnisoPath))) Files.write(Paths.get(libogdisoPath), libjniso)
        if (!Files.exists(Paths.get(libjniso30Path))) Files.write(Paths.get(libjniso30Path), libjniso30)
        if (!Files.exists(Paths.get(libogdisoPath))) Files.write(Paths.get(libogdisoPath), libogdiso)

    }

    // noinspection ScalaStyle
    private def copyInitScript(path: String): Unit = {
        val destPath = Paths.get(path)
        if (!Files.exists(destPath)) Files.createDirectories(destPath)

        val w = new PrintWriter(new File(s"$path/mosaic-gdal-init.sh"))
        val scriptLines = readResourceLines("/scripts/install-gdal-databricks.sh")
        scriptLines
            .map { x => if (x.contains("__DEFAULT_JNI_PATH__")) x.replace("__DEFAULT_JNI_PATH__", path) else x }
            .foreach(x => w.println(x))
        w.close()
    }

    private def loadSharedObjects(): Unit = {
        loadOrNOOP(usrlibsoPath)
        loadOrNOOP(usrlibso30Path)
        loadOrNOOP(usrlibso3003Path)
        loadOrNOOP(libjnisoPath)
        loadOrNOOP(libjniso30Path)
        loadOrNOOP(libogdisoPath)
    }

    private def loadOrNOOP(path: String): Unit = {
        try {
            //if (!Files.exists(Paths.get(path)))
                System.load(path)
        } catch {
            case t: Throwable => logWarning(s"Failed to load $path", t)
        }
    }

    private def readResourceBytes(name: String): Array[Byte] = {
        val bis = new BufferedInputStream(getClass.getResourceAsStream(name))
        try { Stream.continually(bis.read()).takeWhile(-1 !=).map(_.toByte).toArray }
        finally bis.close()
    }

    // noinspection SameParameterValue
    private def readResourceLines(name: String): Array[String] = {
        val bytes = readResourceBytes(name)
        val lines = new String(bytes).split("\n")
        lines
    }

}
