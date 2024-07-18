package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.JTS
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSessionGDAL

class RST_DTMFromGeomsTest extends QueryTest with SharedSparkSessionGDAL with RST_DTMFromGeomsBehaviours {
    test("Testing RST_DTMFromGeoms with manual GDAL registration (H3, JTS).") {
            assume(System.getProperty("os.name") == "Linux")
            simpleRasterizeTest(H3IndexSystem, JTS)
    }
}
