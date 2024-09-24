package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.JTS
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

case class ST_InterpolateElevationTest() extends QueryTest with SharedSparkSession with ST_InterpolateElevationBehaviours {
    test("Testing ST_InterpolateElevation (H3, JTS) to produce interpolated grid elevations on an unconstrained triangulation") { simpleInterpolationBehavior(H3IndexSystem, JTS)}
    test("Testing ST_InterpolateElevation (H3, JTS) to produce interpolated grid elevations on an conforming triangulation") { conformingInterpolationBehavior(H3IndexSystem, JTS)}
}
