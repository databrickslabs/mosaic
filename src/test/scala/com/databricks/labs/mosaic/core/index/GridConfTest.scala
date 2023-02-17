package com.databricks.labs.mosaic.core.index

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class GridConfTest extends AnyFunSuite {

    test("Grid conf computed values should be correct") {

        val conf = GridConf(0, 100, 0, 100, 2, 100, 100)

        conf.spanX shouldBe 100
        conf.spanY shouldBe 100
        conf.bitsPerResolution shouldBe 2
        conf.maxResolution shouldBe 20

    }


    test("Grid conf computed values should be correct for non centered grid") {

        val conf = GridConf(-10, 100, -1, 101, 10, 110, 102)

        conf.spanX shouldBe 110
        conf.spanY shouldBe 102
        conf.bitsPerResolution shouldBe 7
        conf.maxResolution shouldBe 8

    }

}
