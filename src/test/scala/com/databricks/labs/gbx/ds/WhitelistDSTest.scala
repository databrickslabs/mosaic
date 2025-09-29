package com.databricks.labs.gbx.ds

import com.databricks.labs.gbx.ds.whitelist.{WhitelistBatch, WhitelistTable}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.matchers.must.Matchers.{an, be, noException, not}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.language.postfixOps

class WhitelistDSTest extends PlanTest with SilentSparkSession {

    test("Whitelist DS should load() with path provided") {
        spark.sparkContext.setLogLevel("ERROR")

        val tifPath = this.getClass.getResource("/modis/").toString

        val schema = StructType(Array(StructField("test", StringType)))

        val batch = new WhitelistBatch(schema, Map.empty)
        batch.readSchema() shouldEqual schema

        val table = new WhitelistTable(schema, Map.empty)
        table.schema() shouldEqual schema

        an[Exception] should be thrownBy {
            spark.read
                .format("com.databricks.labs.gbx.ds.whitelist.WhitelistDataSource")
                .load()
                .collect()
        }

        noException should be thrownBy {
            val wldf = spark.read
                .format("com.databricks.labs.gbx.ds.whitelist.WhitelistDataSource")
                .option("path", tifPath)
                .load()

            wldf.collect()

            wldf.schema should not be null
        }
    }

}
