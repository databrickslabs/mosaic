package com.databricks.labs.mosaic.core.types

import org.scalatest.flatspec.AnyFlatSpec

class TestTypeNames extends AnyFlatSpec {

    "TypeNames" should "be like expected" in {
        assert(HexType.simpleString == "HEX")
        assert(JSONType.simpleString == "GEOJSON")
        assert(InternalGeometryType.simpleString == "COORDS")
        assert(ChipType.simpleString == "CHIP")
        assert(MosaicType.simpleString == "MOSAIC")
        assert(KryoType.simpleString == "KRYO")
    }
}
