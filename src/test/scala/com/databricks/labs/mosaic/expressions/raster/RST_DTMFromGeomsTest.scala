package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.JTS
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSessionGDAL

class RST_DTMFromGeomsTest extends QueryTest with SharedSparkSessionGDAL with RST_DTMFromGeomsBehaviours {
    test("Testing RST_DTMFromGeoms for simple triangulation with manual GDAL registration (H3, JTS).") {
            assume(System.getProperty("os.name") == "Linux")
            simpleRasterizeTest(H3IndexSystem, JTS)
    }
    test("Testing RST_DTMFromGeoms for conforming triangulation with manual GDAL registration (BNG, JTS).") {
            assume(System.getProperty("os.name") == "Linux")
            conformedTriangulationRasterizeTest(BNGIndexSystem, JTS)
    }

    registerIgnoredTest("Ignored due to resource / duration")(
        test("Testing RST_DTMFromGeoms for conforming triangulation over multiple grid regions with manual GDAL registration (BNG, JTS).") {
            assume(System.getProperty("os.name") == "Linux")
            multiRegionTriangulationRasterizeTest(BNGIndexSystem, JTS)
        }
    )
}
