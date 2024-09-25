package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.JTS
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSessionGDAL

class RST_WriteTest extends QueryTest with SharedSparkSessionGDAL with RST_WriteBehaviors {
    test("write raster tiles") {
        behaviors(H3IndexSystem, JTS)
    }

}
