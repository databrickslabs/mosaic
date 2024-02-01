package org.apache.spark.sql.test

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
                .set("spark.sql.testkey", "true")
          )
        )
    }

    def this() = {
        this(new SparkConf)
    }

}
