package com.databricks.labs.mosaic.core.index

import com.databricks.labs.mosaic.{BNG, H3}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class IndexSystemIDTest extends AnyFunSuite {

    test("IndexSystemID creation from string") {
        IndexSystemFactory.getIndexSystem("H3") shouldEqual H3
        IndexSystemFactory.getIndexSystem("BNG") shouldEqual BNG
        an[Error] should be thrownBy IndexSystemFactory.getIndexSystem("XYZ")
    }

    test("IndexSystemID getIndexSystem from ID") {
        IndexSystemFactory.getIndexSystem(H3.name) shouldEqual H3IndexSystem
        IndexSystemFactory.getIndexSystem(BNG.name) shouldEqual BNGIndexSystem
        an[Error] should be thrownBy IndexSystemFactory.getIndexSystem(null)
    }

}
