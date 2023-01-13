package com.databricks.labs.mosaic.gdal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.gdal.gdal.gdal

import java.io.BufferedInputStream
import java.nio.file.{Files, Paths}
import scala.language.postfixOps
import scala.sys.process._
import scala.util.Try

//noinspection DuplicatedCode
object MosaicGDAL extends Logging {

    // noinspection ScalaWeakerAccess
    val GDAL_ENABLED = "spark.mosaic.gdal.native.enabled"
    private val mosaicGDALPath = Files.createTempDirectory("mosaic-gdal")
    private val mosaicGDALAbsolutePath = mosaicGDALPath.toAbsolutePath.toString
    var isEnabled = false

    def wasEnabled(spark: SparkSession): Boolean = spark.conf.get(GDAL_ENABLED, "false").toBoolean

    def prepareEnvironment(spark: SparkSession, initScriptPath: String, sharedObjectsPath: String): Unit = {
        if (!wasEnabled(spark) && !isEnabled) {
            Try {
                copyInitScript(initScriptPath)
                copySharedObjects(sharedObjectsPath)
            } match {
                case scala.util.Success(_)         =>
                    logInfo("GDAL environment prepared successfully.")
                case scala.util.Failure(exception) =>
                    logError("GDAL environment preparation failed.", exception)
                    throw exception
            }
        }
    }

    def enableGDAL(spark: SparkSession): Unit = {
        if (!wasEnabled(spark) && !isEnabled) {
            Try {
                isEnabled = true
                loadSharedObjects()
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

    private def copySharedObjects(path: String): Unit = {
        val so = readResourceBytes("/gdal/ubuntu/libgdalalljni.so")
        val so30 = readResourceBytes("/gdal/ubuntu/libgdalalljni.so.30")

        val usrGDALPath = Paths.get("/usr/lib/jni/")
        if (!Files.exists(mosaicGDALPath)) Files.createDirectories(mosaicGDALPath)
        if (!Files.exists(usrGDALPath)) Files.createDirectories(usrGDALPath)
        Files.write(Paths.get(s"$mosaicGDALAbsolutePath/libgdalalljni.so"), so)
        Files.write(Paths.get(s"$mosaicGDALAbsolutePath/libgdalalljni.so.30"), so30)

        s"sudo cp $mosaicGDALAbsolutePath/libgdalalljni.so $path/libgdalalljni.so".!!
        s"sudo cp $mosaicGDALAbsolutePath/libgdalalljni.so.30 $path/libgdalalljni.so.30".!!
    }

    private def copyInitScript(path: String): Unit = {
        val destPath = Paths.get(path)
        val script = readResourceBytes("/scripts/install-gdal-databricks.sh")
        if (!Files.exists(mosaicGDALPath)) Files.createDirectories(mosaicGDALPath)
        if (!Files.exists(destPath)) Files.createDirectories(destPath)
        Files.write(Paths.get(s"$mosaicGDALAbsolutePath/mosaic-gdal-init.sh"), script)
        s"sudo cp $mosaicGDALAbsolutePath/mosaic-gdal-init.sh $path/mosaic-gdal-init.sh".!!
    }

    private def loadSharedObjects(): Unit = {
        System.load("/usr/lib/libgdal.so.30")
        if (!Files.exists(Paths.get("/usr/lib/libgdal.so"))) {
            "sudo cp /usr/lib/libgdal.so.30 /usr/lib/libgdal.so".!!
        }
        System.load("/usr/lib/libgdal.so")
        System.load("/usr/lib/libgdal.so.30.0.3")
        System.load("/usr/lib/jni/libgdalalljni.so.30")
        System.load("/usr/lib/ogdi/libgdal.so")
    }

    private def readResourceBytes(name: String): Array[Byte] = {
        val bis = new BufferedInputStream(getClass.getResourceAsStream(name))
        try Stream.continually(bis.read()).takeWhile(-1 !=).map(_.toByte).toArray
        finally bis.close()
    }

}
