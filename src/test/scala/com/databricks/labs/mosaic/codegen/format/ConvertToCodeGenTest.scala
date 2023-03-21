package com.databricks.labs.mosaic.codegen.format

import com.databricks.labs.mosaic.core.geometry.api.{ESRI, JTS}
import com.databricks.labs.mosaic.core.types.{HexType, InternalGeometryType, JSONType}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.{an, be, noException}

class ConvertToCodeGenTest extends AnyFlatSpec {

    "ConvertToCodeGen" should "fail to do codegen " in {
        val ctx = new CodegenContext
        val eval = "geom"
        def nullSafeEval: (CodegenContext, ExprCode, String => String) => ExprCode =
            (_: CodegenContext, code: ExprCode, f: String => String) => {
                f(eval)
                code
            }
        an[Error] should be thrownBy {
            ConvertToCodeGen.doCodeGen(ctx, ExprCode.forNullValue(StringType), nullSafeEval, StringType, "double", JTS)
        }
    }

    "ConvertToCodeGen" should "fail for not supported data type" in {
        val ctx = new CodegenContext
        val eval = "geom"
        an[Error] should be thrownBy ConvertToCodeGen.readGeometryCode(ctx, eval, IntegerType, JTS)
        an[Error] should be thrownBy ConvertToCodeGen.writeGeometryCode(ctx, eval, IntegerType, JTS)
    }


    "ConvertToCodeGen" should "generate read code for all formats" in {
        val ctx = new CodegenContext
        val eval = "geom"
        noException should be thrownBy ConvertToCodeGen.readGeometryCode(ctx, eval, BinaryType, JTS)
        noException should be thrownBy ConvertToCodeGen.readGeometryCode(ctx, eval, StringType, JTS)
        noException should be thrownBy ConvertToCodeGen.readGeometryCode(ctx, eval, HexType, JTS)
        noException should be thrownBy ConvertToCodeGen.readGeometryCode(ctx, eval, JSONType, JTS)
        noException should be thrownBy ConvertToCodeGen.readGeometryCode(ctx, eval, InternalGeometryType, JTS)
        noException should be thrownBy ConvertToCodeGen.readGeometryCode(ctx, eval, BinaryType, ESRI)
        noException should be thrownBy ConvertToCodeGen.readGeometryCode(ctx, eval, StringType, ESRI)
        noException should be thrownBy ConvertToCodeGen.readGeometryCode(ctx, eval, HexType, ESRI)
        noException should be thrownBy ConvertToCodeGen.readGeometryCode(ctx, eval, JSONType, ESRI)
        noException should be thrownBy ConvertToCodeGen.readGeometryCode(ctx, eval, InternalGeometryType, ESRI)
    }

    "ConvertToCodeGen" should "generate write code for all formats" in {
        val ctx = new CodegenContext
        val eval = "geom"
        noException should be thrownBy ConvertToCodeGen.writeGeometryCode(ctx, eval, BinaryType, JTS)
        noException should be thrownBy ConvertToCodeGen.writeGeometryCode(ctx, eval, StringType, JTS)
        noException should be thrownBy ConvertToCodeGen.writeGeometryCode(ctx, eval, HexType, JTS)
        noException should be thrownBy ConvertToCodeGen.writeGeometryCode(ctx, eval, JSONType, JTS)
        noException should be thrownBy ConvertToCodeGen.writeGeometryCode(ctx, eval, InternalGeometryType, JTS)
        noException should be thrownBy ConvertToCodeGen.writeGeometryCode(ctx, eval, BinaryType, ESRI)
        noException should be thrownBy ConvertToCodeGen.writeGeometryCode(ctx, eval, StringType, ESRI)
        noException should be thrownBy ConvertToCodeGen.writeGeometryCode(ctx, eval, HexType, ESRI)
        noException should be thrownBy ConvertToCodeGen.writeGeometryCode(ctx, eval, JSONType, ESRI)
        noException should be thrownBy ConvertToCodeGen.writeGeometryCode(ctx, eval, InternalGeometryType, ESRI)
    }

}
