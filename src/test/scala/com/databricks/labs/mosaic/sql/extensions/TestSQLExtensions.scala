package com.databricks.labs.mosaic.sql.extensions

import com.databricks.labs.mosaic._
import com.databricks.labs.mosaic.core.geometry.api.JTS
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
import org.apache.spark.SparkConf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class TestSQLExtensions extends AnyFlatSpec with SQLExtensionsBehaviors with SparkSuite {

    "Mosaic" should "register SQL extension for all index systems and geometry APIs" in {
        var conf = new SparkConf(false)
            .set(MOSAIC_INDEX_SYSTEM, "H3")
            .set(MOSAIC_GEOMETRY_API, "JTS")
            .set(MOSAIC_RASTER_API, "GDAL")
            .set("spark.sql.extensions", "com.databricks.labs.mosaic.sql.extensions.MosaicSQL")
        var spark = withConf(conf)
        spark.sparkContext.setLogLevel("ERROR")
        it should behave like sqlRegister(MosaicContext.build(H3IndexSystem, JTS), spark)

        conf = new SparkConf(false)
            .set(MOSAIC_INDEX_SYSTEM, "BNG")
            .set(MOSAIC_GEOMETRY_API, "JTS")
            .set(MOSAIC_RASTER_API, "GDAL")
            .set("spark.sql.extensions", "com.databricks.labs.mosaic.sql.extensions.MosaicSQL")
        spark = withConf(conf)
        spark.sparkContext.setLogLevel("ERROR")
        it should behave like sqlRegister(MosaicContext.build(BNGIndexSystem, JTS), spark)

        conf = new SparkConf(false)
            .set(MOSAIC_INDEX_SYSTEM, "DummyIndex")
            .set(MOSAIC_GEOMETRY_API, "DummyAPI")
            .set(MOSAIC_RASTER_API, "GDAL")
            .set("spark.sql.extensions", "com.databricks.labs.mosaic.sql.extensions.MosaicSQL")
        spark = withConf(conf)
        spark.sparkContext.setLogLevel("ERROR")
        it should behave like {
            an[Error] should be thrownBy spark.sql("""show functions""").collect()
        }

        conf = new SparkConf(false)
            .set("spark.sql.extensions", "com.databricks.labs.mosaic.sql.extensions.MosaicSQLDefault")
        spark = withConf(conf)
        spark.sparkContext.setLogLevel("ERROR")
        it should behave like sqlRegister(MosaicContext.build(H3IndexSystem, JTS), spark)

    }

    "Mosaic" should "register GDAL extension for all index systems and geometry APIs in Linux" in {
        assume(System.getProperty("os.name") == "Linux")

        val conf = new SparkConf(loadDefaults = false)
            .set(MOSAIC_GDAL_NATIVE, "true")
            .set("spark.sql.extensions", "com.databricks.labs.mosaic.sql.extensions.MosaicGDAL")
        val spark = withConf(conf)
        spark.sparkContext.setLogLevel("ERROR")
        it should behave like mosaicGDAL(MosaicContext.build(H3IndexSystem, JTS), spark)

    }

}
