package com.databricks.labs.mosaic.core.index

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class IndexSystemIDTest extends AnyFunSuite {

    test("IndexSystemID creation from string") {
        IndexSystemID("H3") shouldEqual H3
        IndexSystemID("BNG") shouldEqual BNG
        an[Error] should be thrownBy IndexSystemID("XYZ")
    }

    test("IndexSystemID getIndexSystem from ID") {
        IndexSystemID.getIndexSystem(H3) shouldEqual H3IndexSystem
        IndexSystemID.getIndexSystem(BNG) shouldEqual BNGIndexSystem
        an[Error] should be thrownBy IndexSystemID.getIndexSystem(null)
    }

}
