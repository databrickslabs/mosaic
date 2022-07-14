package com.databricks.labs.mosaic.codegen.format

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.types._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types._

import scala.util.{Success, Try}

object ConvertToCodeGen {

    // noinspection DuplicatedCode
    def doCodeGen(
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
              if (inputDataType.simpleString == outputDataType.simpleString) {
                  s"""
                     |${ev.value} = $eval;
                     |""".stripMargin
              } else {
                  val tryIO = Try {
                      val (inCode, geomInRef) = readGeometryCode(ctx, eval, inputDataType, geometryAPI)
                      val (outCode, geomOutRef) = writeGeometryCode(ctx, geomInRef, outputDataType, geometryAPI)
                      ((inCode, geomInRef), (outCode, geomOutRef))
                  }
                  (tryIO, geometryAPI) match {
                      case (
                            Success(((inCode, _), (outCode, geomOutRef))),
                            ESRI
                          ) => s"""
                                  |$inCode
                                  |$outCode
                                  |${ev.value} = $geomOutRef;
                                  |""".stripMargin
                      case (
                            Success(((inCode, _), (outCode, geomOutRef))),
                            JTS
                          ) => s"""
                                  |try {
                                  |$inCode
                                  |$outCode
                                  |${ev.value} = $geomOutRef;
                                  |} catch (Exception e) {
                                  | throw e;
                                  |}
                                  |""".stripMargin
                      case _ => throw new IllegalArgumentException(s"Geometry API unsupported: ${geometryAPI.name}.")
                  }
              }
          }
        )
    }

    // noinspection DuplicatedCode
    def readGeometryCode(ctx: CodegenContext, eval: String, inputDataType: DataType, geometryAPI: GeometryAPI): (String, String) = {
        val geometryCodeGen = geometryAPI.ioCodeGen
        inputDataType match {
            case BinaryType           => geometryCodeGen.fromWKB(ctx, eval, geometryAPI)
            case StringType           => geometryCodeGen.fromWKT(ctx, eval, geometryAPI)
            case HexType              => geometryCodeGen.fromHex(ctx, eval, geometryAPI)
            case JSONType             => geometryCodeGen.fromJSON(ctx, eval, geometryAPI)
            case InternalGeometryType => geometryCodeGen.fromInternal(ctx, eval, geometryAPI)
            case _                    => throw new IllegalArgumentException(s"Geometry API unsupported: ${inputDataType.typeName}.")
        }
    }

    // noinspection DuplicatedCode
    def writeGeometryCode(ctx: CodegenContext, eval: String, outputDataType: DataType, geometryAPI: GeometryAPI): (String, String) = {
        val geometryCodeGen = geometryAPI.ioCodeGen
        outputDataType match {
            case BinaryType           => geometryCodeGen.toWKB(ctx, eval, geometryAPI)
            case StringType           => geometryCodeGen.toWKT(ctx, eval, geometryAPI)
            case HexType              => geometryCodeGen.toHEX(ctx, eval, geometryAPI)
            case JSONType             => geometryCodeGen.toJSON(ctx, eval, geometryAPI)
            case InternalGeometryType => geometryCodeGen.toInternal(ctx, eval, geometryAPI)
            case _                    => throw new IllegalArgumentException(s"Data type unsupported: ${outputDataType.typeName}.")
        }
    }

}
