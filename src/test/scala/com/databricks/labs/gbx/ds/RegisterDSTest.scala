package com.databricks.labs.gbx.ds

import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.must.Matchers.{be, noException}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.language.postfixOps

class RegisterDSTest extends PlanTest with SilentSparkSession {

    test("Register DS should register all packages on  load()") {
        noException should be thrownBy
            spark.read
                .format("register_ds")
                .option("functions", "all")
                .load() // should not throw any exception
                .collect()

        spark.sql("show functions like 'gbx_bng_*'").count() should be > 0L
        spark.sql("show functions like 'gbx_st_*'").count() should be > 0L
        spark.sql("show functions like 'gbx_rst_*'").count() should be > 0L
    }

    test("Register DS should register only bng package") {
        noException should be thrownBy
            spark.read
                .format("register_ds")
                .option("functions", "gridx.bng")
                .load() // should not throw any exception
                .collect()

        spark.sql("show functions like 'gbx_bng_*'").count() should be > 0L
    }

    test("Register DS should register only legacy package") {
        noException should be thrownBy
            spark.read
                .format("register_ds")
                .option("functions", "vectorx.jts.legacy")
                .load() // should not throw any exception
                .collect()
        spark.sql("show functions like 'gbx_st_*'").count() should be > 0L
    }

    test("Register DS should register only rasterx package") {
        noException should be thrownBy
            spark.read
                .format("register_ds")
                .option("functions", "rasterx")
                .load() // should not throw any exception
                .collect()
        spark.sql("show functions like 'gbx_rst_*'").count() should be > 0L
    }

}
