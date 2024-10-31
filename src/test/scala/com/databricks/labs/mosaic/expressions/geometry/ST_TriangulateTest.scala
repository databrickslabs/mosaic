package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.JTS
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ST_TriangulateTest extends QueryTest with SharedSparkSession with ST_TriangulateBehaviours {
    test("Testing ST_Triangulate (H3, JTS) to produce unconstrained triangulation") { simpleTriangulateBehavior(H3IndexSystem, JTS)}
    test("Testing ST_Triangulate (H3, JTS) to produce conforming triangulation") { conformingTriangulateBehavior(H3IndexSystem, JTS)}
}
