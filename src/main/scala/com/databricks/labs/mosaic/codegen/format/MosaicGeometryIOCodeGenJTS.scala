package com.databricks.labs.mosaic.codegen.format

import com.databricks.labs.mosaic.core.geometry.MosaicGeometryJTS
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.types.InternalGeometryType
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io._
import org.locationtech.jts.io.geojson.{GeoJsonReader, GeoJsonWriter}

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.types.{BinaryType, StringType}

object MosaicGeometryIOCodeGenJTS extends GeometryIOCodeGen {

    override def fromWKT(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        val inputGeom = ctx.freshName("inputGeom")
        val jtsGeom = classOf[Geometry].getName
        val wktReader = classOf[WKTReader].getName
        (s"""$jtsGeom $inputGeom = new $wktReader().read($eval.toString());""", inputGeom)
    }

    override def fromWKB(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        val inputGeom = ctx.freshName("inputGeom")
        val binaryJavaType = CodeGenerator.javaType(BinaryType)
        val jtsGeom = classOf[Geometry].getName
        val wkbReader = classOf[WKBReader].getName
        (s"""$jtsGeom $inputGeom = new $wkbReader().read(($binaryJavaType)($eval));""", inputGeom)
    }

    override def fromJSON(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        val inputGeom = ctx.freshName("inputGeom")
        val stringJavaType = CodeGenerator.javaType(StringType)
        val tmpHolder = ctx.freshName("tmpHolder")
        val jtsGeom = classOf[Geometry].getName
        val jsonReader = classOf[GeoJsonReader].getName
        (
          s"""
             |$stringJavaType $tmpHolder = ${CodeGenerator.getValue(eval, StringType, "0")};
             |$jtsGeom $inputGeom = new $jsonReader().read($tmpHolder.toString());
             |$tmpHolder = null;
             |""".stripMargin,
          inputGeom
        )
    }

    // noinspection DuplicatedCode
    override def fromHex(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        val inputGeom = ctx.freshName("inputGeom")
        val stringJavaType = CodeGenerator.javaType(StringType)
        val tmpHolder = ctx.freshName("tmpHolder")
        val bytes = ctx.freshName("bytes")
        val wkbReader = classOf[WKBReader].getName
        val jtsGeom = classOf[Geometry].getName
        (
          s"""
             |$stringJavaType $tmpHolder = ${CodeGenerator.getValue(eval, StringType, "0")};
             |byte[] $bytes = $wkbReader.hexToBytes($tmpHolder.toString());
             |$jtsGeom $inputGeom = new $wkbReader().read($bytes);
             |$bytes = null;
             |$tmpHolder = null;
             |""".stripMargin,
          inputGeom
        )
    }

    // noinspection DuplicatedCode
    override def fromInternal(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        val geometryClass = classOf[Geometry].getName
        val geometry = ctx.freshName("geometry")
        val mosaicGeometryClass = classOf[MosaicGeometryJTS].getName

        (
          s"""
             |$geometryClass $geometry = (($mosaicGeometryClass)$mosaicGeometryClass.fromInternal($eval)).getGeom();
             |""".stripMargin,
          geometry
        )
    }

    override def toWKT(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        val outputGeom = ctx.freshName("outputGeom")
        val javaStringType = CodeGenerator.javaType(StringType)
        val wktWriterClass = classOf[WKTWriter].getName
        (
          s"""
             |$javaStringType $outputGeom = $javaStringType.fromString(new $wktWriterClass().write($eval));
             |""".stripMargin,
          outputGeom
        )
    }

    override def toWKB(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        val outputGeom = ctx.freshName("outputGeom")
        val javaBinaryType = CodeGenerator.javaType(BinaryType)
        val wkbWriterClass = classOf[WKBWriter].getName
        (
          s"""
             |$javaBinaryType $outputGeom = new $wkbWriterClass().write($eval);
             |""".stripMargin,
          outputGeom
        )
    }

    // noinspection DuplicatedCode
    override def toHEX(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        val outputGeom = ctx.freshName("outputGeom")
        val binaryJavaType = CodeGenerator.javaType(BinaryType)
        val stringJavaType = CodeGenerator.javaType(StringType)
        val tmpHolder = ctx.freshName("tmpHolder")
        val values = ctx.freshName("values")
        val bytes = ctx.freshName("bytes")
        val wktWriterClass = classOf[WKBWriter].getName
        val rowClass = classOf[GenericInternalRow].getName
        (
          s"""
             |$binaryJavaType $bytes = new $wktWriterClass().write($eval);
             |$stringJavaType $tmpHolder = $stringJavaType.fromString($wktWriterClass.toHex($bytes));
             |Object[] $values = new Object[1];
             |$values[0] = $tmpHolder;
             |InternalRow $outputGeom = new $rowClass($values);
             |$values = null;
             |$bytes = null;
             |$tmpHolder = null;
             |""".stripMargin,
          outputGeom
        )
    }

    // noinspection DuplicatedCode
    override def toJSON(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        val outputGeom = ctx.freshName("outputGeom")
        val stringJavaType = CodeGenerator.javaType(StringType)
        val tmpHolder = ctx.freshName("tmpHolder")
        val values = ctx.freshName("values")
        val rowClass = classOf[GenericInternalRow].getName
        val geoJsonWriterClass = classOf[GeoJsonWriter].getName
        (
          s"""
             |$stringJavaType $tmpHolder = $stringJavaType.fromString(new $geoJsonWriterClass().write($eval));
             |Object[] $values = new Object[1];
             |$values[0] = $tmpHolder;
             |InternalRow $outputGeom = new $rowClass($values);
             |$values = null;
             |$tmpHolder = null;
             |""".stripMargin,
          outputGeom
        )
    }

    override def toGeoJSON(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        val outputGeom = ctx.freshName("outputGeom")
        val javaStringType = CodeGenerator.javaType(StringType)
        val geoJsonWriterClass = classOf[GeoJsonWriter].getName
        (
          s"""
             |$javaStringType $outputGeom = $javaStringType.fromString(new $geoJsonWriterClass().write($eval));
             |""".stripMargin,
          outputGeom
        )
    }

    override def toInternal(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        val outputGeom = ctx.freshName("outputGeom")
        val mosaicGeometryClass = classOf[MosaicGeometryJTS].getName
        val internalGeometryJavaType = CodeGenerator.javaType(InternalGeometryType)
        (
          s"""
             |$internalGeometryJavaType $outputGeom = (InternalRow)($mosaicGeometryClass.apply($eval).toInternal().serialize());
             |""".stripMargin,
          outputGeom
        )
    }

}
