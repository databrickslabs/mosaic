package org.apache.spark.sql.test

import com.databricks.labs.mosaic._
import com.databricks.labs.mosaic.gdal.MosaicGDAL
import com.databricks.labs.mosaic.utils.FileUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{Args, CompositeStatus, Status}

import scala.util.Try

trait SharedSparkSessionGDAL extends SharedSparkSession {


    var checkpointingEnabled: Boolean = _
    def checkpointingStatus: String = if (checkpointingEnabled) "enabled" else "disabled"

    override protected def runTest(testName: String, args: Args): Status = {
        val statuses = for (checkpointing <- Seq(true, false)) yield {
            checkpointingEnabled = checkpointing
            spark.conf.set(MOSAIC_RASTER_USE_CHECKPOINT, checkpointing)
            spark.sparkContext.setLogLevel("INFO")
            logInfo(s"Raster checkpointing is $checkpointingStatus")
            spark.sparkContext.setLogLevel("ERROR")
            super.runTest(testName, args)
        }
        new CompositeStatus(statuses.toSet)
    }

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
    }

}
