package com.databricks.labs.mosaic.core

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.ESRI
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import org.scalatest.funsuite.AnyFunSuite

class TestMosaic extends AnyFunSuite {

  test("mosaicFill should not return duplicates with H3") {
    // This tests the fix for issue #243 https://github.com/databrickslabs/mosaic/issues/243
    val geom = ESRI.geometry("POLYGON ((4.42 51.78, 4.38 51.78, 4.39 51.83, 4.40 51.83, 4.41 51.8303, 4.417 51.8295, 4.42 51.83, 4.44 51.81, 4.42 51.78))", "WKT")
    val result = Mosaic.mosaicFill(geom, 7, true, H3IndexSystem, ESRI)

    assert(result.length == 10)
    assert(result.map(x => x.index).distinct.length == 10)
  }
}
