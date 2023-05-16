package com.databricks.labs.mosaic.codegen.format

import java.nio.ByteBuffer

import com.databricks.labs.mosaic.core.geometry.{MosaicGeometry, MosaicGeometryESRI}
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.types.InternalGeometryType
import com.esri.core.geometry.ogc.OGCGeometry
import com.esri.core.geometry.SpatialReference
import org.locationtech.jts.io.{WKBReader, WKBWriter}

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.types.{BinaryType, StringType}

object MosaicGeometryIOCodeGenESRI extends GeometryIOCodeGen {

    override def fromWKT(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        // Technically, fromEWKT can have an implementation which is only a subset of implementation of
        // fromWKT but it's not really necessary and both can use the same implementation so long as
        // it works for both.
        fromEWKT(ctx, eval, geometryAPI)
    }

    override def fromWKB(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        val inputGeom = ctx.freshName("inputGeom")
        val binaryJavaType = CodeGenerator.javaType(BinaryType)
        val ogcGeom = classOf[OGCGeometry].getName
        val byteBuffer = classOf[ByteBuffer].getName
        (s"""$ogcGeom $inputGeom = $ogcGeom.fromBinary($byteBuffer.wrap(($binaryJavaType)($eval)));""", inputGeom)
    }

    override def fromJSON(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        val inputGeom = ctx.freshName("inputGeom")
        val stringJavaType = CodeGenerator.javaType(StringType)
        val tmpHolder = ctx.freshName("tmpHolder")
        val ogcGeom = classOf[OGCGeometry].getName
        (
          s"""
             |$stringJavaType $tmpHolder = ${CodeGenerator.getValue(eval, StringType, "0")};
             |$ogcGeom $inputGeom = $ogcGeom.fromGeoJson($tmpHolder.toString());
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
        val ogcGeom = classOf[OGCGeometry].getName
        val byteBuffer = classOf[ByteBuffer].getName
        (
          s"""
             |$stringJavaType $tmpHolder = ${CodeGenerator.getValue(eval, StringType, "0")};
             |byte[] $bytes = $wkbReader.hexToBytes($tmpHolder.toString());
             |$ogcGeom $inputGeom = $ogcGeom.fromBinary($byteBuffer.wrap($bytes));
             |$bytes = null;
             |$tmpHolder = null;
             |""".stripMargin,
          inputGeom
        )
    }

    override def fromInternal(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        val ogcGeom = classOf[OGCGeometry].getName
        val geometry = ctx.freshName("geometry")
        val mosaicGeometryClass = classOf[MosaicGeometryESRI].getName

        (
          s"""
             |$ogcGeom $geometry = $mosaicGeometryClass.fromInternal($eval).getGeom();
             |""".stripMargin,
          geometry
        )
    }

    override def fromEWKT(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        val inputGeom = ctx.freshName("inputGeom")
        val geom = ctx.freshName("geom")
        val parts = ctx.freshName("parts")
        val srid = ctx.freshName("srid")
        val ogcGeom = classOf[OGCGeometry].getName
        val sptRef = classOf[SpatialReference].getName
        (
            s"""
               |$ogcGeom $inputGeom;
               |String $geom = $eval.toString();
               |if ($geom.startsWith("SRID=")) {
               |    String[] $parts = $geom.split(";", 0);
               |    String $srid = $parts[0].split("=", 0)[1];
               |    $inputGeom = $ogcGeom.fromText($parts[1]);
               |    $inputGeom.setSpatialReference($sptRef.create(Integer.parseInt($srid)));
               |} else {
               |    $inputGeom = $ogcGeom.fromText($geom);
               |}
               |""".stripMargin,
            inputGeom
        )

//        val inputGeom = ctx.freshName("inputGeom")
//        val parts = ctx.freshName("parts")
//        val srid = ctx.freshName("srid")
//        val ogcGeom = classOf[OGCGeometry].getName
//        val sptRef = classOf[SpatialReference].getName
//        (
//            s"""
//               |String[] $parts = $eval.toString().split(";", 0);
//               |String $srid = $parts[0].split("=", 0)[1];
//               |$inputGeom = $ogcGeom.fromText($parts[1]);
//               |$inputGeom.setSpatialReference($sptRef.create($srid))
//               |""".stripMargin,
//            inputGeom
//        )
    }

    override def toWKT(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        val outputGeom = ctx.freshName("outputGeom")
        val javaStringType = CodeGenerator.javaType(StringType)
        (s"""$javaStringType $outputGeom = $javaStringType.fromString($eval.asText());""", outputGeom)
    }

    override def toWKB(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        val outputGeom = ctx.freshName("outputGeom")
        val javaBinaryType = CodeGenerator.javaType(BinaryType)
        (s"""$javaBinaryType $outputGeom = $eval.asBinary().array();""", outputGeom)
    }

    // noinspection DuplicatedCode
    override def toHEX(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        val outputGeom = ctx.freshName("outputGeom")
        val binaryJavaType = CodeGenerator.javaType(BinaryType)
        val stringJavaType = CodeGenerator.javaType(StringType)
        val tmpHolder = ctx.freshName("tmpHolder")
        val values = ctx.freshName("values")
        val bytes = ctx.freshName("bytes")
        val wkbWriter = classOf[WKBWriter].getName
        val rowClass = classOf[GenericInternalRow].getName
        (
          s"""
             |$binaryJavaType $bytes = $eval.asBinary().array();
             |$stringJavaType $tmpHolder = $stringJavaType.fromString($wkbWriter.toHex($bytes));
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
    override def toGeoJSON(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        val outputGeom = ctx.freshName("outputGeom")
        val javaStringType = CodeGenerator.javaType(StringType)
        (s"""$javaStringType $outputGeom = $javaStringType.fromString($eval.asGeoJson());""", outputGeom)
    }

    // noinspection DuplicatedCode
    override def toJSON(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        val outputGeom = ctx.freshName("outputGeom")
        val stringJavaType = CodeGenerator.javaType(StringType)
        val tmpHolder = ctx.freshName("tmpHolder")
        val values = ctx.freshName("values")
        val rowClass = classOf[GenericInternalRow].getName
        (
          s"""
             |$stringJavaType $tmpHolder = $stringJavaType.fromString($eval.asGeoJson());
             |Object[] $values = new Object[1];
             |$values[0] = $tmpHolder;
             |InternalRow $outputGeom = new $rowClass($values);
             |$values = null;
             |$tmpHolder = null;
             |""".stripMargin,
          outputGeom
        )
    }

    override def toInternal(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        val outputGeom = ctx.freshName("outputGeom")
        val mosaicGeometryClass = classOf[MosaicGeometryESRI].getName
        val internalGeometryJavaType = CodeGenerator.javaType(InternalGeometryType)
        (
          s"""
             |$internalGeometryJavaType $outputGeom = (InternalRow)($mosaicGeometryClass.apply($eval).toInternal().serialize());
             |""".stripMargin,
          outputGeom
        )
    }

    override def toEWKT(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        val outputGeom = ctx.freshName("outputGeom")
        val srid = ctx.freshName("grid")
        val javaStringType = CodeGenerator.javaType(StringType)
        (
            s"""
               |int $srid = 0;
               |if ($eval.esriSR != null) $srid = $eval.getEsriSpatialReference().getID();
               |$javaStringType $outputGeom = $javaStringType.fromString("SRID=" + Integer.toString($srid) + ";" + $eval.asText());
               |""".stripMargin,
            outputGeom
        )
    }

}
