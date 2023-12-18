package com.databricks.labs.mosaic.core.geometry.api

import com.databricks.labs.mosaic.core.geometry.point.MosaicPointJTS
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class TestGeometryAPI extends AnyFlatSpec with MockFactory {

    "GeometryAPI" should "throw an exception when serializing non existing format." in {
        val point = MosaicPointJTS.fromWKT("POINT(1 1)")
        val geometryAPI = JTS

        assertThrows[Error] {
            geometryAPI.serialize(point, "non-existent-format")
        }
    }

}
