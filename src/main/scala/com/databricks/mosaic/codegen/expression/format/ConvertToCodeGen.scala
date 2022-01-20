package com.databricks.mosaic.codegen.expression.format

import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types._

import com.databricks.mosaic.codegen.geometry.{MosaicGeometryIOCodeGenJTS, MosaicGeometryIOCodeGenOGC}
import com.databricks.mosaic.core.geometry.api.GeometryAPI
import com.databricks.mosaic.core.geometry.api.GeometryAPI.{JTS, OGC}
import com.databricks.mosaic.core.types._

object ConvertToCodeGen {

    // noinspection DuplicatedCode
    def doCodeGenOGC(
        ctx: CodegenContext,
        ev: ExprCode,
        nullSafeCodeGen: (CodegenContext, ExprCode, String => String) => ExprCode,
        inputDataType: DataType,
        outputDataType: DataType,
        geometryAPI: GeometryAPI
    ): ExprCode = {
        val geometryCodeGen = geometryAPI.name match {
            case n if n == OGC.name => MosaicGeometryIOCodeGenOGC
            case n if n == JTS.name => MosaicGeometryIOCodeGenJTS
        }
        nullSafeCodeGen(
          ctx,
          ev,
          eval => {
              val (inCode, geomInRef) = inputDataType match {
                  case BinaryType           => geometryCodeGen.fromWKB(ctx, eval, geometryAPI)
                  case StringType           => geometryCodeGen.fromWKT(ctx, eval, geometryAPI)
                  case HexType              => geometryCodeGen.fromHex(ctx, eval, geometryAPI)
                  case JSONType             => geometryCodeGen.fromJSON(ctx, eval, geometryAPI)
                  case InternalGeometryType => geometryCodeGen.fromInternal(ctx, eval, geometryAPI)
                  case KryoType             => ???
              }

              val (outCode, geomOutRef) = outputDataType match {
                  case BinaryType           => geometryCodeGen.toWKB(ctx, geomInRef, geometryAPI)
                  case StringType           => geometryCodeGen.toWKT(ctx, geomInRef, geometryAPI)
                  case HexType              => geometryCodeGen.toHEX(ctx, geomInRef, geometryAPI)
                  case JSONType             => geometryCodeGen.toJSON(ctx, geomInRef, geometryAPI)
                  case InternalGeometryType => geometryCodeGen.toInternal(ctx, geomInRef, geometryAPI)
                  case KryoType             => ???
              }
              geometryAPI.name match {
                  case n if n == OGC.name => s"""
                                                |$inCode
                                                |$outCode
                                                |${ev.value} = $geomOutRef;
                                                |""".stripMargin
                  case n if n == JTS.name => s"""
                                                |try {
                                                |$inCode
                                                |$outCode
                                                |${ev.value} = $geomOutRef;
                                                |} catch (Exception e) {
                                                | throw e;
                                                |}
                                                |""".stripMargin
              }
          }
        )
    }

}
