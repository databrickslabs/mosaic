package com.databricks.labs.mosaic.gdal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.gdal.gdal.gdal

import java.io.BufferedInputStream
import java.nio.file.{Files, Paths}
import scala.language.postfixOps
import scala.sys.process._
import scala.util.Try

object MosaicGDAL extends Logging {

    private val GDAL_ENABLED = "spark.mosaic.gdal.native.enabled"

    private val mosaicGDALPath = Files.createTempDirectory("mosaic-gdal")
    private val mosaicGDALAbsolutePath = mosaicGDALPath.toAbsolutePath.toString

    if (!Files.exists(mosaicGDALPath)) Files.createDirectories(mosaicGDALPath)

    private var isEnabled = false

    private def wasEnabled(spark: SparkSession): Boolean = spark.conf.get(GDAL_ENABLED, "false").toBoolean

    private def readResourceFile(name: String): Array[Byte] = {
        val bis = new BufferedInputStream(getClass.getResourceAsStream(name))
        try Stream.continually(bis.read()).takeWhile(-1 !=).map(_.toByte).toArray
        finally bis.close()
    }

    def prepareEnvironment(scriptPath: String = "/FileStore/geospatial/mosaic/gdal/init_scripts/"): Unit = {
        copySharedObjects()
        copyInstallScript(scriptPath)
    }

    def enableGDAL(spark: SparkSession): Unit = {
        if (!wasEnabled(spark) && !isEnabled) {
            isEnabled = true
            loadSharedObjects()
            gdal.AllRegister()
            spark.conf.set(GDAL_ENABLED, "true")
        }
    }

    def disableGDAL(): Unit = {
        gdal.Rmdir("/vsimem/")
        Try {
            val spark = SparkSession.builder().getOrCreate()
            if (wasEnabled(spark) && isEnabled) {
                gdal.GDALDestroyDriverManager()
                // Clear the drivers.
                val n = gdal.GetDriverCount()
                for (i <- 0 until n) {
                    val driver = gdal.GetDriver(i)
                    if (driver != null) {
                        driver.Deregister()
                        driver.delete()
                    }
                }
                // Clear the virtual filesystem
                isEnabled = false
            }
        }
    }

    private def copyInstallScript(path: String = "/FileStore/geospatial/mosaic/gdal/init_scripts/"): Unit = {
        val scriptBytes = readResourceFile("/script/install-gdal-databricks.sh")
        Files.write(Paths.get(s"$mosaicGDALAbsolutePath/mosaic-gdal.sh"), scriptBytes)
        s"sudo cp $mosaicGDALAbsolutePath/mosaic-gdal.sh $path/mosaic-gdal.sh".!!
    }

    private def copySharedObjects(): Unit = {
        val so = readResourceFile("/gdal/ubuntu/libgdalalljni.so")
        val so30 = readResourceFile("/gdal/ubuntu/libgdalalljni.so.30")

        val usrGDALPath = Paths.get("/usr/lib/jni/")
        val libgdalSOPath = Paths.get("/usr/lib/libgdal.so")
        val libgdalSO30Path = Paths.get("/usr/lib/libgdal.so.30")
        if (!Files.exists(usrGDALPath)) Files.createDirectories(usrGDALPath)
        if (!Files.exists(libgdalSOPath)) "sudo cp /usr/lib/libgdal.so.30 /usr/lib/libgdal.so".!!
        if (!Files.exists(libgdalSO30Path)) "sudo cp /usr/lib/libgdal.so /usr/lib/libgdal.so.30".!!
        Files.write(Paths.get(s"$mosaicGDALAbsolutePath/libgdalalljni.so"), so)
        Files.write(Paths.get(s"$mosaicGDALAbsolutePath/libgdalalljni.so.30"), so30)

        s"sudo cp $mosaicGDALAbsolutePath/libgdalalljni.so /usr/lib/jni/libgdalalljni.so".!!
        s"sudo cp $mosaicGDALAbsolutePath/libgdalalljni.so.30 /usr/lib/jni/libgdalalljni.so.30".!!
    }

    private def loadSharedObjects(): Unit = {
        Try(System.load("/usr/lib/libgdal.so.30"))
        Try(System.load("/usr/lib/jni/libgdalalljni.so.30"))
        Try(System.load("/usr/lib/libgdal.so.30.0.3"))
        Try(System.load("/usr/lib/ogdi/libgdal.so"))
        Try(System.load("/usr/lib/libgdal.so"))
    }

}
