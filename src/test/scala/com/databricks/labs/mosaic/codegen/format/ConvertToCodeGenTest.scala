package com.databricks.labs.mosaic.codegen.format

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{Illegal, JTS}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.an

class ConvertToCodeGenTest extends AnyFlatSpec {

    "ConvertToCodeGen" should "fail to do codegen " in {
        val ctx = new CodegenContext
        val eval = "geom"
        def nullSafeEval: (CodegenContext, ExprCode, String => String) => ExprCode =
            (_: CodegenContext, code: ExprCode, f: String => String) => {
                f(eval)
                code
            }
        an[IllegalArgumentException] should be thrownBy {
            ConvertToCodeGen.doCodeGen(ctx, ExprCode.forNullValue(StringType), nullSafeEval, StringType, BinaryType, Illegal)
        }
    }

    "ConvertToCodeGen" should "fail for invalid geometry API" in {
        val ctx = new CodegenContext
        val eval = "geom"
        an[IllegalArgumentException] should be thrownBy ConvertToCodeGen.readGeometryCode(ctx, eval, BinaryType, Illegal)
        an[IllegalArgumentException] should be thrownBy ConvertToCodeGen.writeGeometryCode(ctx, eval, BinaryType, Illegal)
    }

    "ConvertToCodeGen" should "fail for not supported data type" in {
        val ctx = new CodegenContext
        val eval = "geom"
        an[IllegalArgumentException] should be thrownBy ConvertToCodeGen.readGeometryCode(ctx, eval, IntegerType, JTS)
        an[IllegalArgumentException] should be thrownBy ConvertToCodeGen.writeGeometryCode(ctx, eval, IntegerType, JTS)
    }

}
