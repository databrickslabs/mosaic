package org.apache.spark.sql.test

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.io.CleanUpManager
import com.databricks.labs.mosaic.gdal.MosaicGDAL
import com.databricks.labs.mosaic.utils.FileUtils
import com.databricks.labs.mosaic.{MOSAIC_GDAL_NATIVE, MOSAIC_MANUAL_CLEANUP_MODE, MOSAIC_RASTER_CHECKPOINT, MOSAIC_RASTER_LOCAL_AGE_LIMIT_MINUTES, MOSAIC_RASTER_TMP_PREFIX, MOSAIC_RASTER_TMP_PREFIX_DEFAULT, MOSAIC_RASTER_USE_CHECKPOINT, MOSAIC_RASTER_USE_CHECKPOINT_DEFAULT, MOSAIC_TEST_MODE}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.gdal.gdal.gdal

import scala.util.Try

trait SharedSparkSessionGDAL extends SharedSparkSession {

    private var mosaicCheckpointRootDir: String = _

    override def sparkConf: SparkConf = {
        //note: calling super.sparkConf constructs a new object
        super.sparkConf
            .set(MOSAIC_GDAL_NATIVE, "true")
            .set(MOSAIC_TEST_MODE, "true")
    }

    override def createSparkSession: TestSparkSession = {
        SparkSession.cleanupAnyExistingSession()
        val conf = sparkConf
        val session = new MosaicTestSparkSession(conf)
        session.sparkContext.setLogLevel("ERROR")
        mosaicCheckpointRootDir = FileUtils.createMosaicTempDir(prefix = getCheckpointRootDir)
        Try(MosaicGDAL.enableGDAL(session))
        session
    }

    override def beforeEach(): Unit = {
        super.beforeEach()

        val sc: SparkSession = this.spark
        sc.sparkContext.setLogLevel("ERROR")

        sc.conf.set(MOSAIC_GDAL_NATIVE, "true")
        sc.conf.set(MOSAIC_TEST_MODE, "true")
        sc.conf.set(MOSAIC_MANUAL_CLEANUP_MODE, "false")
        sc.conf.set(MOSAIC_RASTER_LOCAL_AGE_LIMIT_MINUTES, "10") // default "30"
        sc.conf.set(MOSAIC_RASTER_USE_CHECKPOINT, MOSAIC_RASTER_USE_CHECKPOINT_DEFAULT)
        sc.conf.set(MOSAIC_RASTER_CHECKPOINT, mosaicCheckpointRootDir)
        sc.conf.set(MOSAIC_RASTER_TMP_PREFIX, MOSAIC_RASTER_TMP_PREFIX_DEFAULT)
        sc.conf.set(MOSAIC_RASTER_USE_CHECKPOINT, MOSAIC_RASTER_USE_CHECKPOINT_DEFAULT)

        Try(MosaicGDAL.enableGDAL(sc))
        Try(gdal.AllRegister())
    }

    override def afterEach(): Unit = {
        super.afterEach()

        // clean up 5+ minute old checkpoint files (for testing)
        // - this specifies to remove fuse mount files which are mocked for development
        GDAL.cleanUpManualDir(ageMinutes = 5, getCheckpointRootDir, keepRoot = true, allowFuseDelete = true) match {
            case Some(msg) => info(s"cleanup mosaic tmp dir msg -> '$msg'")
            case _ => ()
        }
    }

    override def afterAll(): Unit = {
        // Hotfix for SharedSparkSession afterAll cleanup.
        // - super.afterAll stops spark
        Try(super.afterAll())

        // option: clean up configured MosaicTmpRootDir
        // - all but those in the last 5 minutes
        // - this is separate from the managed process (10 minute cleanup)
        // - this seems to affect
//        GDAL.cleanUpManualDir(ageMinutes = 5, getMosaicTmpRootDir, keepRoot = true) match {
//            case Some(msg) => info(s"cleanup mosaic tmp dir msg -> '$msg'")
//            case _ => ()
//        }
    }

    protected def getCheckpointRootDir: String = "/dbfs/checkpoint"

    protected def getMosaicCheckpointRootDir: String = mosaicCheckpointRootDir

    protected def getTempRootDir: String = MOSAIC_RASTER_TMP_PREFIX_DEFAULT

    protected def getMosaicTmpRootDir: String = s"$getTempRootDir/mosaic_tmp"
}
