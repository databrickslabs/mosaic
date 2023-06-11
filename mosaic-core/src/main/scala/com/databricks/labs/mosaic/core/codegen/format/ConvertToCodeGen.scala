package com.databricks.labs.mosaic.core.codegen.format

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.types._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types._

/**
 * This class is used to generate CodeGen for converting between different geometry formats.
 */
object ConvertToCodeGen {

  /**
   * This method generates code to construct a geometry from the input format.
   * Then the method generates code to write the geometry to the output format.
   * There is currently no support for conversion without constructing a geometry.
   *
   * @param ctx                CodegenContext used for code generation.
   * @param ev                 ExprCode that will store the reference to the output.
   * @param eval               Reference to the input.
   * @param inputDataType      DataType of the input.
   * @param outputDataTypeName Name of the output DataType.
   * @param geometryAPI        GeometryAPI used to manipulate the geometry.
   * @return Code to construct the geometry from the input format and write the geometry to the output format.
   */
  def fromEval(
                ctx: CodegenContext,
                ev: ExprCode,
                eval: String,
                inputDataType: DataType,
                outputDataTypeName: String,
                geometryAPI: GeometryAPI
              ): String = {
    if (inputDataType.simpleString == outputDataTypeName) {
      s"""
         |${ev.value} = $eval;
         |""".stripMargin
    } else {
      val (inCode, geomInRef) = readGeometryCode(ctx, eval, inputDataType, geometryAPI)
      val (outCode, geomOutRef) = writeGeometryCode(ctx, geomInRef, outputDataTypeName, geometryAPI)
      geometryAPI.codeGenTryWrap(
        s"""
           |$inCode
           |$outCode
           |${ev.value} = $geomOutRef;
           |""".stripMargin)
    }
  }

  /**
   * This method executes the actual code generation.
   * We need this nesting to allow for testing through scalamock.
   *
   * @param ctx                CodegenContext used for code generation.
   * @param ev                 ExprCode that will store the reference to the output.
   * @param nullSafeCodeGen    Code to generate the output.
   * @param inputDataType      DataType of the input.
   * @param outputDataTypeName Name of the output DataType.
   * @param geometryAPI        GeometryAPI used to manipulate the geometry.
   * @return Code to construct the geometry from the input format and write the geometry to the output format.
   */
  def doCodeGen(
                 ctx: CodegenContext,
                 ev: ExprCode,
                 nullSafeCodeGen: (CodegenContext, ExprCode, String => String) => ExprCode,
                 inputDataType: DataType,
                 outputDataTypeName: String,
                 geometryAPI: GeometryAPI
               ): ExprCode = {
    nullSafeCodeGen(
      ctx,
      ev,
      eval => fromEval(ctx, ev, eval, inputDataType, outputDataTypeName, geometryAPI)
    )
  }

  /**
   * This method generates code to read the geometry from the input format.
   *
   * @param ctx           CodegenContext used for code generation.
   * @param eval          Reference to the input.
   * @param inputDataType DataType of the input.
   * @param geometryAPI   GeometryAPI used to manipulate the geometry.
   * @return Code to construct the geometry from the input format.
   */
  def readGeometryCode(ctx: CodegenContext, eval: String, inputDataType: DataType, geometryAPI: GeometryAPI): (String, String) = {
    val geometryCodeGen = geometryAPI.ioCodeGen
    inputDataType match {
      case BinaryType => geometryCodeGen.fromWKB(ctx, eval, geometryAPI)
      case StringType => geometryCodeGen.fromWKT(ctx, eval, geometryAPI)
      case HexType => geometryCodeGen.fromHex(ctx, eval, geometryAPI)
      case GeoJSONType => geometryCodeGen.fromGeoJSON(ctx, eval, geometryAPI)
      case _ => throw new Error(s"Geometry API unsupported: ${inputDataType.typeName}.")
    }
  }

  /**
   * This method generates code to write the geometry to the output format.
   *
   * @param ctx            CodegenContext used for code generation.
   * @param eval           Reference to the input.
   * @param outputDataType DataType of the output.
   * @param geometryAPI    GeometryAPI used to manipulate the geometry.
   * @return Code to write the geometry to the output format.
   */
  def writeGeometryCode(ctx: CodegenContext, eval: String, outputDataType: DataType, geometryAPI: GeometryAPI): (String, String) = {
    val outDataFormat = GeometryFormat.getDefaultFormat(outputDataType)
    writeGeometryCode(ctx, eval, outDataFormat, geometryAPI)
  }

  /**
   * This method generates code to write the geometry to the output format.
   *
   * @param ctx                  CodegenContext used for code generation.
   * @param eval                 Reference to the input.
   * @param outputDataFormatName Name of the output format.
   * @param geometryAPI          GeometryAPI used to manipulate the geometry.
   * @return Code to write the geometry to the output format.
   */
  def writeGeometryCode(ctx: CodegenContext, eval: String, outputDataFormatName: String, geometryAPI: GeometryAPI): (String, String) = {
    val geometryCodeGen = geometryAPI.ioCodeGen

    outputDataFormatName match {
      case "WKB" => geometryCodeGen.toWKB(ctx, eval, geometryAPI)
      case "WKT" => geometryCodeGen.toWKT(ctx, eval, geometryAPI)
      case "HEX" => geometryCodeGen.toHEX(ctx, eval, geometryAPI)
      case "GEOJSON" => geometryCodeGen.toGeoJSON(ctx, eval, geometryAPI)
      case _ => throw new Error(s"Data type unsupported: $outputDataFormatName.")
    }
  }

}
