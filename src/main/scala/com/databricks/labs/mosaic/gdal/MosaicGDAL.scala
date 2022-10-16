package com.databricks.labs.mosaic.gdal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkException
import org.gdal.gdal.gdal

import java.io.{BufferedInputStream, IOException}
import java.nio.file.{Files, Paths}
import scala.io.{BufferedSource, Source}
import scala.language.postfixOps
import scala.sys.process._
import scala.util.Try

object MosaicGDAL extends Logging {

    val GDAL_ENABLED = "spark.mosaic.gdal.native.enabled"

    private var isEnabled = false

    private def wasEnabled(spark: SparkSession): Boolean = spark.conf.get(GDAL_ENABLED, "false").toBoolean

    def enableGDAL(spark: SparkSession): Unit = {
        if (!wasEnabled(spark) && !isEnabled) {
            isEnabled = true
            installGDAL(spark)
            copySharedObjects()
            loadSharedObjects()
            gdal.AllRegister()
            spark.conf.set(GDAL_ENABLED, "true")
            spark.sparkContext.addSparkListener(new GDALListener)
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

    def copySharedObjects(): Unit = {
        def readSOBytes(name: String): Array[Byte] = {
            val bis = new BufferedInputStream(getClass.getResourceAsStream(name))
            try Stream.continually(bis.read()).takeWhile(-1 !=).map(_.toByte).toArray
            finally bis.close()
        }

        val so = readSOBytes("/gdal/ubuntu/libgdalalljni.so")
        val so30 = readSOBytes("/gdal/ubuntu/libgdalalljni.so.30")

        val mosaicGDALPath = Files.createTempDirectory("mosaic-gdal")
        val mosaicGDALAbsolutePath = mosaicGDALPath.toAbsolutePath.toString
        val usrGDALPath = Paths.get("/usr/lib/jni/")
        val libgdalSOPath = Paths.get("/usr/lib/libgdal.so")
        if (!Files.exists(mosaicGDALPath)) Files.createDirectories(mosaicGDALPath)
        if (!Files.exists(usrGDALPath)) Files.createDirectories(usrGDALPath)
        if (!Files.exists(libgdalSOPath)) {
            "sudo cp /usr/lib/libgdal.so.30 /usr/lib/libgdal.so".!!
        }
        Files.write(Paths.get(s"$mosaicGDALAbsolutePath/libgdalalljni.so"), so)
        Files.write(Paths.get(s"$mosaicGDALAbsolutePath/libgdalalljni.so.30"), so30)

        s"sudo cp $mosaicGDALAbsolutePath/libgdalalljni.so /usr/lib/jni/libgdalalljni.so".!!
        s"sudo cp $mosaicGDALAbsolutePath/libgdalalljni.so.30 /usr/lib/jni/libgdalalljni.so.30".!!
    }

    def loadSharedObjects(): Unit = {
        System.load("/usr/lib/libgdal.so.30")
        System.load("/usr/lib/jni/libgdalalljni.so.30")
        System.load("/usr/lib/libgdal.so.30.0.3")
        System.load("/usr/lib/ogdi/libgdal.so")
        System.load("/usr/lib/libgdal.so")
    }

    def installGDAL(spark: SparkSession): Unit = installGDAL(Some(spark))

    def installGDAL(spark: Option[SparkSession]): Unit = {
        val sc = spark.map(_.sparkContext)
        val numExecutors = sc.map(_.getExecutorMemoryStatus.size - 1)
        val script = getScript
        for (cmd <- script.getLines.toList) {
            try {
                if (!cmd.startsWith("#") || cmd.nonEmpty) cmd.!!
                sc.map { sparkContext =>
                    if (!sparkContext.isLocal) {
                        sparkContext.parallelize(1 to numExecutors.get).pipe(cmd).collect
                    }
                }
            } catch {
                case e: IOException           => logError(e.getMessage)
                case e: IllegalStateException => logError(e.getMessage)
                case e: SparkException        => logError(e.getMessage)
                case e: Throwable             => logError(e.getMessage)
            } finally {
                script.close
            }
        }
    }

    def getScript: BufferedSource = {
        val scriptPath = System.getProperty("os.name").toLowerCase() match {
            case o: String if o.contains("nux") => "/scripts/install-gdal-databricks.sh"
            case _ => throw new UnsupportedOperationException("This method only supports Ubuntu Linux with `apt`.")
        }
        val script = Source.fromInputStream(getClass.getResourceAsStream(scriptPath))
        script
    }

}
