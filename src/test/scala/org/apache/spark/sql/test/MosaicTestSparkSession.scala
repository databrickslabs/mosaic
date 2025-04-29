package org.apache.spark.sql.test

import org.apache.spark.{SparkConf, SparkContext}

class MosaicTestSparkSession(sc: SparkContext) extends TestSparkSession(sc) {

    def this(sparkConf: SparkConf) = {

        this(
          new SparkContext(
            "local[16]",
            "test-sql-context",
            sparkConf
                .set("spark.sql.adaptive.enabled", "false")
                .set("spark.driver.memory", "32g")
                .set("spark.executor.memory", "32g")
                .set("spark.sql.testkey", "true")
                .set("spark.sql.shuffle.partitions", "32")
          )
        )
    }

    def this() = {
        this(new SparkConf)
    }

}
