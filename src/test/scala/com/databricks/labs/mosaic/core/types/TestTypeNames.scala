package com.databricks.labs.mosaic.core.types

import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec

class TestTypeNames extends AnyFlatSpec {

    "TypeNames" should "be like expected" in {
        assert(HexType.simpleString == "HEX")
        assert(JSONType.simpleString == "GEOJSON")
        assert(InternalGeometryType.simpleString == "COORDS")
        assert(ChipType(LongType).simpleString == "CHIP")
        assert(ChipType(IntegerType).simpleString == "CHIP")
        assert(ChipType(StringType).simpleString == "CHIP")
        assert(MosaicType(LongType).simpleString == "MOSAIC")
        assert(MosaicType(IntegerType).simpleString == "MOSAIC")
        assert(MosaicType(StringType).simpleString == "MOSAIC")
    }
}
