package com.databricks.labs.mosaic.test

import com.databricks.labs.mosaic.gdal.MosaicGDAL._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import java.io.BufferedInputStream
import java.nio.file.Files
import scala.language.postfixOps
import scala.sys.process._


object TestMosaicGDAL extends Logging {

    private def readResourceBytes(): Array[Byte] = {
        val bis = new BufferedInputStream(getClass.getResourceAsStream("/scripts/install-gdal-databricks.sh"))
        try {
            Stream.continually(bis.read()).takeWhile(-1 !=).map(_.toByte).toArray
        }
        finally bis.close()
    }

    def installGDAL(spark: SparkSession): Unit = {
        if (!wasEnabled(spark) && !isEnabled) installGDAL()
    }

    def installGDAL(): Unit = {
        val bytes = readResourceBytes()
        val tempPath = Files.createTempFile("gdal-ubuntu-install", ".sh")
        Files.write(tempPath, bytes)

        s"chmod +x ${tempPath.toAbsolutePath.toString}".!!
        s"sh ${tempPath.toAbsolutePath.toString}".!!
    }



}
