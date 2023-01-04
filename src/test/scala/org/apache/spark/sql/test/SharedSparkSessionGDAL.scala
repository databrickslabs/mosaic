package org.apache.spark.sql.test

import com.databricks.labs.mosaic.gdal.MosaicGDAL
import com.databricks.labs.mosaic.test.TestMosaicGDAL
import com.databricks.labs.mosaic.MOSAIC_GDAL_NATIVE
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.nio.file.Files

trait SharedSparkSessionGDAL extends SharedSparkSession {

    override def sparkConf: SparkConf = {
        super.sparkConf
            .set(MOSAIC_GDAL_NATIVE, "true")
    }

    override def createSparkSession: TestSparkSession = {
        val conf = sparkConf
        SparkSession.cleanupAnyExistingSession()
        val session = new TestSparkSession(conf)
        if (conf.get(MOSAIC_GDAL_NATIVE, "false").toBoolean) {
            TestMosaicGDAL.installGDAL(session)
            val tempPath = Files.createTempDirectory("mosaic-gdal")
            MosaicGDAL.prepareEnvironment(session, tempPath.toAbsolutePath.toString, "usr/lib/jni")
            MosaicGDAL.enableGDAL(session)
        }
        session
    }

}
