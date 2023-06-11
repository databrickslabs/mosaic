package com.databricks.labs.mosaic.core.expressions.raster

import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.{a, convertToAnyShouldWrapper}

class RasterTest extends AnyFunSuite with MockFactory {

  test("package object should implement buildMapString and buildMapDouble") {
    val map1 = Map("a" -> "b", "c" -> "d")
    val map2 = Map("a" -> 1.0, "c" -> 2.0)

    com.databricks.labs.mosaic.core.expressions.raster.buildMapString(map1) shouldBe a[ArrayBasedMapData]
    com.databricks.labs.mosaic.core.expressions.raster.buildMapDouble(map2) shouldBe a[ArrayBasedMapData]
  }

}
