package com.databricks.labs.mosaic.test

import com.databricks.labs.mosaic.MOSAIC_GDAL_NATIVE

trait SparkCodeGenSuite extends SparkSuite {

    override def beforeAll(): Unit = {
        super.beforeAll()
        spark.conf.set(MOSAIC_GDAL_NATIVE, "false")
        spark.conf.set("spark.sql.codegen.factoryMode", "CODEGEN_ONLY")
        spark.conf.set("spark.sql.codegen.fallback", "false")
    }

}
