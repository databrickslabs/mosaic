package org.apache.spark.sql.test

import com.databricks.labs.mosaic.MOSAIC_TEST_MODE
import org.apache.spark.{SparkConf, SparkContext}

class MosaicTestSparkSession(sc: SparkContext) extends TestSparkSession(sc) {

    def this(sparkConf: SparkConf) = {

        this(
          new SparkContext(
            "local[8]",
            "test-sql-context",
            sparkConf
                .set("spark.sql.adaptive.enabled", "false")
                .set("spark.driver.memory", "32g")
                .set("spark.executor.memory", "32g")
                .set("spark.sql.shuffle.partitions", "8")
                .set(MOSAIC_TEST_MODE, "true")
          )
        )
    }

    def this() = {
        this(new SparkConf)
    }

}
