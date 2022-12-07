package org.apache.spark.sql.test

import com.databricks.labs.mosaic.gdal.MosaicGDAL
import com.databricks.labs.mosaic.test.{MosaicGDAL => TestMosaicGDAL}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SharedSparkSessionGDAL extends SharedSparkSession {

    override def sparkConf: SparkConf = {
        super.sparkConf
            .set("spark.mosaic.gdal.native", "true")
    }

    override def createSparkSession: TestSparkSession = {
        val conf = sparkConf
        SparkSession.cleanupAnyExistingSession()
        val session = new TestSparkSession(conf)
        if (conf.get("spark.mosaic.gdal.native", "false").toBoolean) {
            TestMosaicGDAL.installGDAL(session)
            MosaicGDAL.enableGDAL(session)
        }
        session
    }

}
