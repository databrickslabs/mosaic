package com.databricks.mosaic.codegen.expression.format

import com.databricks.mosaic.core.types.{HexType, JSONType}
import com.esri.core.geometry.ogc.OGCGeometry
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}

import java.nio.ByteBuffer

object ConvertToCodeGen {

  //noinspection DuplicatedCode
  private def doCodeGenOGC(
                            ctx: CodegenContext,
                            ev: ExprCode,
                            nullSafeCodeGen: (CodegenContext, ExprCode, String => String) => ExprCode,
                            inputDataType: DataType,
                            outputDataType: DataType
                          ): ExprCode = {
    val inputType = CodeGenerator.javaType(inputDataType)
    val ogcGeom = classOf[OGCGeometry].getName
    val byteBuffer = classOf[ByteBuffer]
    val outputType = CodeGenerator.javaType(outputDataType)
    val inputVar = ctx.freshName("inputVar")
    val inputGeom = ctx.freshName("inputGeom")
    val outputVar = ctx.freshName("outputVar")
    nullSafeCodeGen(ctx, ev, eval => {
      s"""
         |/*mosaic_codegen*/
         |$inputType $inputVar = ($inputType)($eval);
         |${
        inputDataType match {
          case BinaryType =>
            s"""$ogcGeom $inputGeom = $ogcGeom.fromBinary($byteBuffer.wrap($inputVar));""".stripMargin
          case StringType =>
            s"""$ogcGeom $inputGeom = $ogcGeom.fromText($inputVar);""".stripMargin
          case HexType =>
            val tmpHolder = ctx.freshName("tmpHolder")
            s"""
               |String $tmpHolder = ${CodeGenerator.getValue(inputVar, StringType, "0")};
               |$ogcGeom $inputGeom = $ogcGeom.fromText($tmpHolder);
               |""".stripMargin
          case JSONType =>
            val tmpHolder = ctx.freshName("tmpHolder")
            s"""
               |String $tmpHolder = ${CodeGenerator.getValue(inputVar, StringType, "0")};
               |$ogcGeom $inputGeom = $ogcGeom.fromText($tmpHolder);
               |""".stripMargin
        }
      }
         |$outputType $outputVar = ${
        outputDataType match {
          case BinaryType =>
        }
      }
         |""".stripMargin
    })
  }

  private def doCodeGenOGC(
                            ctx: CodegenContext,
                            ev: ExprCode,
                            nullSafeCodeGen: (CodegenContext, ExprCode, String => String) => ExprCode
                          ): ExprCode = {
    nullSafeCodeGen(ctx, ev, eval => {
      s"""
         |
         |""".stripMargin
    })
  }

}
