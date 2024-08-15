package org.apache.spark.sql.test

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.functions.{ExprConfig, MosaicContext}
import com.databricks.labs.mosaic.gdal.MosaicGDAL
import com.databricks.labs.mosaic.test.mocks.filePath
import com.databricks.labs.mosaic.utils.{FileUtils, PathUtils}
import com.databricks.labs.mosaic.{MOSAIC_CLEANUP_AGE_LIMIT_MINUTES, MOSAIC_GDAL_NATIVE, MOSAIC_MANUAL_CLEANUP_MODE, MOSAIC_RASTER_CHECKPOINT, MOSAIC_RASTER_TMP_PREFIX, MOSAIC_RASTER_TMP_PREFIX_DEFAULT, MOSAIC_RASTER_USE_CHECKPOINT, MOSAIC_RASTER_USE_CHECKPOINT_DEFAULT, MOSAIC_TEST_MODE}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.gdal.gdal.gdal

import java.nio.file.Paths
import scala.util.Try

trait SharedSparkSessionGDAL extends SharedSparkSession {

    private var mosaicCheckpointRootDir: String = _

    private var exprConfigOpt: Option[ExprConfig] = None

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
        mosaicCheckpointRootDir = FileUtils.createMosaicTmpDir(prefix = getCheckpointRootDir)
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
        sc.conf.set(MOSAIC_CLEANUP_AGE_LIMIT_MINUTES, "5") // manual is -1 (default is 30)
        sc.conf.set(MOSAIC_RASTER_USE_CHECKPOINT, "true") // default is "false"
        sc.conf.set(MOSAIC_RASTER_CHECKPOINT, mosaicCheckpointRootDir)
        sc.conf.set(MOSAIC_RASTER_TMP_PREFIX, MOSAIC_RASTER_TMP_PREFIX_DEFAULT)

        Try(MosaicGDAL.enableGDAL(sc))
        Try(gdal.AllRegister())

        exprConfigOpt = Option(ExprConfig(sc))

        // clean-up sidecar files in modis, if any
        // - 'target-class' dir as well as project 'resources' dir
        PathUtils.cleanUpPAMFiles(
            Paths.get(filePath("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")).getParent.toString,
            uriGdalOpt = None
        )
        PathUtils.cleanUpPAMFiles("src/test/resources/modis/", uriGdalOpt = None)

        // option: clean this session's tmp dir (from any previous tests in this suite)
        // - just this session, can be more restrictive
        val sessionAge = 2
        GDAL.cleanUpManualDir(ageMinutes = sessionAge, MosaicContext.getTmpSessionDir(exprConfigOpt), keepRoot = true) match {
            case Some(msg) => info(s"cleanup local session dir (older than $sessionAge minutes) msg -> '$msg'")
            case _ => ()
        }
    }

    override def afterAll(): Unit = {
        // option: clean checkpoint files (for testing)
        // - this specifies to remove fuse mount files which are mocked for development
        val checkAge = 3
        GDAL.cleanUpManualDir(ageMinutes = checkAge, getMosaicCheckpointRootDir, keepRoot = true, allowFuseDelete = true) match {
            case Some(msg) => info(s"cleanup mosaic checkpoint dir (older than $checkAge minutes msg -> '$msg'")
            case _ => ()
        }

        // option: clean local tmp dir (from any previous tests in this suite)
        // - this is for repeat local testing on docker (before CleanUpManager 10 minute policy kicks in)
        val localAge = 3
        GDAL.cleanUpManualDir(ageMinutes = localAge, getMosaicTmpRootDir, keepRoot = true) match {
            case Some(msg) => info(s"cleanup mosaic local dir (older than $localAge minutes) msg -> '$msg'")
            case _ => ()
        }

        // Hotfix for SharedSparkSession afterAll cleanup.
        // - super.afterAll stops spark
        Try(super.afterAll())
    }

    protected def getCheckpointRootDir: String = "/dbfs/checkpoint"

    protected def getExprConfigOpt: Option[ExprConfig] = exprConfigOpt

    protected def getMosaicCheckpointRootDir: String = mosaicCheckpointRootDir

    protected def getTempRootDir: String = MOSAIC_RASTER_TMP_PREFIX_DEFAULT

    protected def getMosaicTmpRootDir: String = s"$getTempRootDir/mosaic_tmp"
}
