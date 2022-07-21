package com.databricks.labs.mosaic.core.geometry.api

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.ESRI
import com.databricks.labs.mosaic.core.geometry.point.MosaicPointESRI
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalamock.scalatest.MockFactory

class TestGeometryAPI extends AnyFlatSpec with MockFactory {

    "GeometryAPI" should "throw an exception when serializing non existing format." in {
        val point = MosaicPointESRI.fromWKT("POINT(1 1)")
        val geometryAPI = ESRI

        assertThrows[Error] {
            geometryAPI.serialize(point, "non-existent-format")
        }
    }

}
