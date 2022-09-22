package com.databricks.labs.mosaic.core.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.JTS
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class TestH3IndexSystem extends AnyFlatSpec {

    "Auxiliary Methods" should "not cause any Errors." in {
        val indexRes = H3IndexSystem.pointToIndex(10, 10, 10)
        noException shouldBe thrownBy { H3IndexSystem.format(indexRes) }
        noException shouldBe thrownBy { H3IndexSystem.getResolutionStr(10) }
        noException shouldBe thrownBy { H3IndexSystem.indexToGeometry(H3IndexSystem.format(indexRes), JTS) }
    }

}
