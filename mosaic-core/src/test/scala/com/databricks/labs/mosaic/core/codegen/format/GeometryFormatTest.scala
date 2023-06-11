package com.databricks.labs.mosaic.core.codegen.format

import com.databricks.labs.mosaic.core.types.{GeoJSONType, HexType}
import org.apache.spark.sql.types.{BinaryType, CalendarIntervalType, StringType}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class GeometryFormatTest extends AnyFunSuite with MockFactory {

    test("GeometryFormat should handle valid and invalid data types") {
        noException should be thrownBy GeometryFormat.getDefaultFormat(BinaryType)
        noException should be thrownBy GeometryFormat.getDefaultFormat(StringType)
        noException should be thrownBy GeometryFormat.getDefaultFormat(HexType)
        noException should be thrownBy GeometryFormat.getDefaultFormat(GeoJSONType)
        an[Error] should be thrownBy GeometryFormat.getDefaultFormat(CalendarIntervalType)
    }

}
