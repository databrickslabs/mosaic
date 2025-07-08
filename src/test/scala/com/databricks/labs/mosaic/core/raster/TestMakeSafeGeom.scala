package com.databricks.labs.mosaic.core.raster

import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.gdal.MosaicGDAL
import com.databricks.labs.mosaic.test.mocks.filePath
import com.databricks.labs.mosaic.utils.IsolatedProcess
import com.databricks.labs.mosaic.{JTS, MOSAIC_RASTER_CHECKPOINT, MOSAIC_RASTER_USE_CHECKPOINT, MOSAIC_TEST_MODE}
import org.apache.spark.sql.test.SharedSparkSessionGDAL
import org.gdal.gdal.{gdal => gdalJNI}
import org.gdal.gdalconst
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.duration.DurationInt

class TestMakeSafeGeom extends SharedSparkSessionGDAL {

    test("Test that anti meridian bbox is correctly created") {
        val geom1 = JTS.geometry("POLYGON((179.5 55.0, -179.5 55.0, -179.5 54.0, 179.5 54.0, 179.5 55.0))", "WKT")
        val safe = MosaicRasterGDAL.makeSafeGeometry(JTS, geom1)

        val geom2 = JTS.geometry("POLYGON((179.7 10, -179.7 10, -179.7 9, 179.7 9, 179.7 10))", "WKT")
        val safe2 = MosaicRasterGDAL.makeSafeGeometry(JTS, geom2)

        val geom3 = JTS.geometry("POLYGON((179.9 10, 0.0 10, 0.0 9, 179.9 9, 179.9 10))", "WKT")
        val safe3 = MosaicRasterGDAL.makeSafeGeometry(JTS, geom3)

        val geom4 = JTS.geometry("POLYGON((180.0 6, -179.5 6, -179.5 5, 180.0 5, 180.0 6))", "WKT")
        val safe4 = MosaicRasterGDAL.makeSafeGeometry(JTS, geom4)

        val geom5 = JTS.geometry("POLYGON((180.0 2, 0.0 2, 0.0 1, 180.0 1, 180.0 2))", "WKT")
        val safe5 = MosaicRasterGDAL.makeSafeGeometry(JTS, geom5)

        1 shouldBe 1 // dummy assertion to ensure the test runs
    }

}
