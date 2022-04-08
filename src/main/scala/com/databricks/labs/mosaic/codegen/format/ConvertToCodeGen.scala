package com.databricks.labs.mosaic.codegen.format

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{JTS, ESRI}
import com.databricks.labs.mosaic.core.types._

import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types._

object ConvertToCodeGen {

    // noinspection DuplicatedCode
    def doCodeGenESRI(
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
                  case n if n == ESRI.name => s"""
                                                 |$inCode
                                                 |$outCode
                                                 |${ev.value} = $geomOutRef;
                                                 |""".stripMargin
                  case n if n == JTS.name  => s"""
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
            case n if n == ESRI.name => MosaicGeometryIOCodeGenESRI
            case n if n == JTS.name  => MosaicGeometryIOCodeGenJTS
        }
        // noinspection ScalaStyle
        inputDataType match {
            case BinaryType           => geometryCodeGen.fromWKB(ctx, eval, geometryAPI)
            case StringType           => geometryCodeGen.fromWKT(ctx, eval, geometryAPI)
            case HexType              => geometryCodeGen.fromHex(ctx, eval, geometryAPI)
            case JSONType             => geometryCodeGen.fromJSON(ctx, eval, geometryAPI)
            case InternalGeometryType => geometryCodeGen.fromInternal(ctx, eval, geometryAPI)
            case KryoType             => throw new NotImplementedError("KryoType is not Supported yet.")
        }
    }

    // noinspection DuplicatedCode
    def writeGeometryCode(ctx: CodegenContext, eval: String, outputDataType: DataType, geometryAPI: GeometryAPI): (String, String) = {
        val geometryCodeGen = geometryAPI.name match {
            case n if n == ESRI.name => MosaicGeometryIOCodeGenESRI
            case n if n == JTS.name  => MosaicGeometryIOCodeGenJTS
        }
        // noinspection ScalaStyle
        outputDataType match {
            case BinaryType           => geometryCodeGen.toWKB(ctx, eval, geometryAPI)
            case StringType           => geometryCodeGen.toWKT(ctx, eval, geometryAPI)
            case HexType              => geometryCodeGen.toHEX(ctx, eval, geometryAPI)
            case JSONType             => geometryCodeGen.toJSON(ctx, eval, geometryAPI)
            case InternalGeometryType => geometryCodeGen.toInternal(ctx, eval, geometryAPI)
            case KryoType             => throw new NotImplementedError("KryoType is not Supported yet.")
        }
    }

}
