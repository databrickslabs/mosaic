package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
import org.scalatest.flatspec.AnyFlatSpec

class TestConversions extends AnyFlatSpec with ConversionBehaviors with SparkSuite {

    "st_geomfromgeojson" should "convert geojson to geom" in {
        it should behave like st_geomfromgeojson(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like st_geomfromgeojson(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

}
