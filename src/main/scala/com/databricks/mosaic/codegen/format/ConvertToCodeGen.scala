package com.databricks.mosaic.codegen.format

import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types._

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
        nullSafeCodeGen(
          ctx,
          ev,
          eval => {
              val (inCode, geomInRef) = readGeometryCode(ctx, eval, inputDataType, geometryAPI)
              val (outCode, geomOutRef) = writeGeometryCode(ctx, geomInRef, outputDataType, geometryAPI)

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

    // noinspection DuplicatedCode
    def readGeometryCode(ctx: CodegenContext, eval: String, inputDataType: DataType, geometryAPI: GeometryAPI): (String, String) = {
        val geometryCodeGen = geometryAPI.name match {
            case n if n == OGC.name => MosaicGeometryIOCodeGenOGC
            case n if n == JTS.name => MosaicGeometryIOCodeGenJTS
        }
        // noinspection ScalaStyle
        inputDataType match {
            case BinaryType           => geometryCodeGen.fromWKB(ctx, eval, geometryAPI)
            case StringType           => geometryCodeGen.fromWKT(ctx, eval, geometryAPI)
            case HexType              => geometryCodeGen.fromHex(ctx, eval, geometryAPI)
            case JSONType             => geometryCodeGen.fromJSON(ctx, eval, geometryAPI)
            case InternalGeometryType => geometryCodeGen.fromInternal(ctx, eval, geometryAPI)
            case KryoType             => // noinspection NotImplementedCode
                ???
        }
    }

    // noinspection DuplicatedCode
    def writeGeometryCode(ctx: CodegenContext, eval: String, outputDataType: DataType, geometryAPI: GeometryAPI): (String, String) = {
        val geometryCodeGen = geometryAPI.name match {
            case n if n == OGC.name => MosaicGeometryIOCodeGenOGC
            case n if n == JTS.name => MosaicGeometryIOCodeGenJTS
        }
        // noinspection ScalaStyle
        outputDataType match {
            case BinaryType           => geometryCodeGen.toWKB(ctx, eval, geometryAPI)
            case StringType           => geometryCodeGen.toWKT(ctx, eval, geometryAPI)
            case HexType              => geometryCodeGen.toHEX(ctx, eval, geometryAPI)
            case JSONType             => geometryCodeGen.toJSON(ctx, eval, geometryAPI)
            case InternalGeometryType => geometryCodeGen.toInternal(ctx, eval, geometryAPI)
            case KryoType             => // noinspection NotImplementedCode
                ???
        }
    }

}
