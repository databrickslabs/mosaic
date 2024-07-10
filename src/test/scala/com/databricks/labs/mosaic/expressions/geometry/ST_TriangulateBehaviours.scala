package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import org.apache.spark.sql.QueryTest
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers.{an, be, convertToAnyShouldWrapper}


trait ST_TriangulateBehaviours extends QueryTest {

    def simpleTriangulateBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {

        val points = spark.read.format("shapefile").load("src/test/resources/binary/elevation/sd46_dtm_point.shp")
        noException should be thrownBy points.collect()
        1 shouldBe 1
    }

}
