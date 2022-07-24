package com.databricks.labs.mosaic.core.geometry

import com.databricks.labs.mosaic.core.types.{HexType, InternalGeometryType, JSONType}
import org.apache.spark.sql.types.{BinaryType, DoubleType, StringType}
import org.scalatest.flatspec.AnyFlatSpec

class TestGeometryFormat extends AnyFlatSpec {

    "GeometryFormat" should "Map the column types to default formats" in {
        assert(GeometryFormat.getDefaultFormat(BinaryType) == "WKB")
        assert(GeometryFormat.getDefaultFormat(StringType) == "WKT")
        assert(GeometryFormat.getDefaultFormat(HexType) == "HEX")
        assert(GeometryFormat.getDefaultFormat(JSONType) == "JSONOBJECT")
        assert(GeometryFormat.getDefaultFormat(InternalGeometryType) == "COORDS")
    }

    it should "throw an exception for unknown types" in {
        assertThrows[Error](GeometryFormat.getDefaultFormat(DoubleType))
    }

}
