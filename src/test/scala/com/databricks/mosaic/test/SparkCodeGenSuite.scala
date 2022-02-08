package com.databricks.mosaic.test

trait SparkCodeGenSuite extends SparkSuite {

    override def beforeAll(): Unit = {
        super.beforeAll()
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        spark.conf.set("spark.sql.codegen.factoryMode", "CODEGEN_ONLY")
        spark.conf.set("spark.sql.codegen.fallback", "false")
    }

}
