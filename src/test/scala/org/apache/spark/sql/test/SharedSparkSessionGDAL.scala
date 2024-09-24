package org.apache.spark.sql.test

import com.databricks.labs.mosaic.gdal.MosaicGDAL
import com.databricks.labs.mosaic.utils.FileUtils
import com.databricks.labs.mosaic.{MOSAIC_GDAL_NATIVE, MOSAIC_RASTER_CHECKPOINT, MOSAIC_RASTER_USE_CHECKPOINT, MOSAIC_RASTER_USE_CHECKPOINT_DEFAULT, MOSAIC_TEST_MODE}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.gdal.gdal.gdal

import scala.util.Try

trait SharedSparkSessionGDAL extends SharedSparkSession {

    override def sparkConf: SparkConf = {
        super.sparkConf
            .set(MOSAIC_GDAL_NATIVE, "true")
        super.sparkConf
            .set(MOSAIC_TEST_MODE, "true")
    }

    override def createSparkSession: TestSparkSession = {
        val conf = sparkConf
        conf.set(MOSAIC_RASTER_CHECKPOINT, FileUtils.createMosaicTempDir(prefix = "/mnt/"))
        SparkSession.cleanupAnyExistingSession()
        val session = new MosaicTestSparkSession(conf)
        session.sparkContext.setLogLevel("ERROR")
        Try {
            MosaicGDAL.enableGDAL(session)
        }
        session
    }

    override def beforeEach(): Unit = {
        super.beforeEach()
        sparkConf.set(MOSAIC_RASTER_USE_CHECKPOINT, MOSAIC_RASTER_USE_CHECKPOINT_DEFAULT)
        MosaicGDAL.enableGDAL(this.spark)
        gdal.AllRegister()
    }

}
