package com.databricks.labs.mosaic.core.index

import org.scalatest.funsuite.AnyFunSuite

class IndexSystemFactoryTest extends AnyFunSuite {

  test("Get index system by name") {

    IndexSystemFactory.getIndexSystem("BNG")
    IndexSystemFactory.getIndexSystem("H3")
    IndexSystemFactory.getIndexSystem("CUSTOM(1,2,3,4,5,6,7)")
    IndexSystemFactory.getIndexSystem("CUSTOM(-1,-2,-3,-4,5,6,7)")
    IndexSystemFactory.getIndexSystem("CUSTOM(10,20,30,40,50,60,70)")

  }


  test("Get index system by name throws exception if not supported") {
    assertThrows[Error] {
      IndexSystemFactory.getIndexSystem("Oops!")
    }
  }
}
