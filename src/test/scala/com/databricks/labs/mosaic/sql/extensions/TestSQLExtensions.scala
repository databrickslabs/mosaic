package com.databricks.labs.mosaic.sql.extensions

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
import org.apache.spark.SparkConf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class TestSQLExtensions extends AnyFlatSpec with SQLExtensionsBehaviors with SparkSuite {

    "Mosaic" should "register SQL extension for all index systems and geometry APIs" in {
        var conf = new SparkConf(false)
            .set("spark.databricks.labs.mosaic.index.system", "H3")
            .set("spark.databricks.labs.mosaic.geometry.api", "JTS")
            .set("spark.sql.extensions", "com.databricks.labs.mosaic.sql.extensions.MosaicSQL")
        var spark = withConf(conf)
        it should behave like sqlRegister(MosaicContext.build(H3IndexSystem, JTS), spark)

        conf = new SparkConf(false)
            .set("spark.databricks.labs.mosaic.index.system", "H3")
            .set("spark.databricks.labs.mosaic.geometry.api", "ESRI")
            .set("spark.sql.extensions", "com.databricks.labs.mosaic.sql.extensions.MosaicSQL")
        spark = withConf(conf)
        it should behave like sqlRegister(MosaicContext.build(H3IndexSystem, ESRI), spark)

        conf = new SparkConf(false)
            .set("spark.databricks.labs.mosaic.index.system", "BNG")
            .set("spark.databricks.labs.mosaic.geometry.api", "JTS")
            .set("spark.sql.extensions", "com.databricks.labs.mosaic.sql.extensions.MosaicSQL")
        spark = withConf(conf)
        it should behave like sqlRegister(MosaicContext.build(BNGIndexSystem, JTS), spark)

        conf = new SparkConf(false)
            .set("spark.databricks.labs.mosaic.index.system", "BNG")
            .set("spark.databricks.labs.mosaic.geometry.api", "ESRI")
            .set("spark.sql.extensions", "com.databricks.labs.mosaic.sql.extensions.MosaicSQL")
        spark = withConf(conf)
        it should behave like sqlRegister(MosaicContext.build(BNGIndexSystem, ESRI), spark)

        conf = new SparkConf(false)
            .set("spark.databricks.labs.mosaic.index.system", "DummyIndex")
            .set("spark.databricks.labs.mosaic.geometry.api", "DummyAPI")
            .set("spark.sql.extensions", "com.databricks.labs.mosaic.sql.extensions.MosaicSQL")
        spark = withConf(conf)
        it should behave like {
            an[Error] should be thrownBy spark.sql("""show functions""").collect()
        }

        conf = new SparkConf(false)
            .set("spark.sql.extensions", "com.databricks.labs.mosaic.sql.extensions.MosaicSQLDefault")
        spark = withConf(conf)
        it should behave like sqlRegister(MosaicContext.build(H3IndexSystem, ESRI), spark)
    }

}
