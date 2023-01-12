package com.databricks.labs.mosaic.codegen

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class ConvertToCodegenMockTest extends AnyFunSuite with MockFactory {

    test("ConvertTo Expression from GEOJSON to Unsupported format should throw an exception") {
        val ctx = stub[CodegenContext]
        val api = stub[GeometryAPI]
        api.name _ when () returns "ESRI"

        assertThrows[Error] {
            ConvertToCodeGen writeGeometryCode (ctx, "", "unsupported", api)
        }
    }

}
