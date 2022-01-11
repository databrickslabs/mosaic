package com.databricks.mosaic.codegen.geometry

import java.nio.ByteBuffer

import com.esri.core.geometry._
import com.esri.core.geometry.ogc._
import org.locationtech.jts.io.WKBReader

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.types._

import com.databricks.mosaic.core.geometry.MosaicGeometryOGC
import com.databricks.mosaic.core.geometry.api.GeometryAPI
import com.databricks.mosaic.core.types.{BoundaryType, HolesType}
import com.databricks.mosaic.core.types.model.GeometryTypeEnum._

object MosaicGeometryIOCodeGenOGC extends GeometryIOCodeGen {

    override def fromWKT(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        val inputGeom = ctx.freshName("inputGeom")
        val stringJavaType = CodeGenerator.javaType(StringType)
        val ogcGeom = classOf[OGCGeometry].getName
        (s"""$ogcGeom $inputGeom = $ogcGeom.fromText(($stringJavaType)($eval));""", inputGeom)
    }

    override def fromWKB(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        val inputGeom = ctx.freshName("inputGeom")
        val binaryJavaType = CodeGenerator.javaType(BinaryType)
        val ogcGeom = classOf[OGCGeometry].getName
        val byteBuffer = classOf[ByteBuffer]
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
             |$ogcGeom $inputGeom = $ogcGeom.fromGeoJson($tmpHolder);
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
        (
          s"""
             |$stringJavaType $tmpHolder = ${CodeGenerator.getValue(eval, StringType, "0")};
             |byte[] $bytes = $wkbReader.hexToBytes($tmpHolder);
             |$ogcGeom $inputGeom = $ogcGeom.fromBinary($bytes);
             |$bytes = null;
             |$tmpHolder = null;
             |""".stripMargin,
          inputGeom
        )
    }

    override def fromInternal(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {

        val intJavaType = CodeGenerator.javaType(IntegerType)
        val boundaryJavaType = CodeGenerator.javaType(BoundaryType)
        val holesJavaType = CodeGenerator.javaType(HolesType)

        val spatialReference = classOf[SpatialReference].getName
        val ogcGeom = classOf[OGCGeometry].getName
        val ogcPoint = classOf[OGCPoint].getName
        val ogcMultiPoint = classOf[OGCMultiPoint].getName
        val ogcLineString = classOf[OGCLineString].getName
        val ogcMultiLineString = classOf[OGCMultiLineString].getName
        val ogcPolygon = classOf[OGCPolygon].getName
        val ogcMultiPolygon = classOf[OGCMultiPolygon].getName
        val esriPoint = classOf[Point].getName
        val esriMultiPoint = classOf[MultiPoint].getName
        val esriPolyline = classOf[Polyline].getName
        val esriPolygon = classOf[Polygon].getName

        val geomTypeId = ctx.freshName("geomTypeId")
        val geometry = ctx.freshName("geometry")
        val boundaries = ctx.freshName("boundaries")
        val localBoundaries = ctx.freshName("localBoundaries")
        val holes = ctx.freshName("holes")
        val localHoles = ctx.freshName("localHoles")
        val coords = ctx.freshName("coords")
        val spatialReferenceInstance = ctx.freshName("spatialReferenceInstance")
        val point = ctx.freshName("point")
        val multiPoint = ctx.freshName("multiPoint")
        val polyline = ctx.freshName("polyline")
        val polygon = ctx.freshName("polygon")

        val constructPointFuncName = ctx.freshName("constructPointFunc")
        val constructPolylineFuncName = ctx.freshName("constructPolylineFunc")
        val constructPolygonFuncName = ctx.freshName("constructPolygonFunc")

        val spatialReferenceId = MosaicGeometryOGC.spatialReference.getID

        // spatial reference should be global immutable state
        // for each different SR create a different instance do not update
//    val countTerm = ctx.addMutableState(CodeGenerator.JAVA_LONG, "count")
//    val partitionMaskTerm = "partitionMask"
//    ctx.addImmutableStateIfNotExists(CodeGenerator.JAVA_LONG, partitionMaskTerm)
//    ctx.addPartitionInitializationStatement(s"$countTerm = 0L;")
//    ctx.addPartitionInitializationStatement(s"$partitionMaskTerm = ((long) partitionIndex) << 33;")

        val constructPointFunc = ctx.addNewFunction(
          constructPointFuncName,
          s"""
             |private $esriPoint $constructPointFuncName(double[] $coords) {
             |  $esriPoint $point;
             |  if ($coords.length == 2) {
             |    $point = new $esriPoint($coords[0], $coords[1]);
             |  } else {
             |    $point = new $esriPoint($coords[0], $coords[1], $coords[2]);
             |  }
             |  return $point;
             |}
             |""".stripMargin
        )

        val constructPolylineFunc = ctx.addNewFunction(
          constructPolylineFuncName,
          s"""
             |private $esriPolyline $constructPolylineFuncName(double[][][] $localBoundaries) {
             |  $esriPolyline $polyline = new $esriPolyline();
             |  for(int j=0; j<$localBoundaries.length; j++) {
             |    $polyline.startPath($localBoundaries[j][0][0], $localBoundaries[j][0][1]);
             |    for(int i=1; i<$localBoundaries[j].length; i++) {
             |      $polyline.lineTo($localBoundaries[j][i][0], $localBoundaries[j][i][1]);
             |    }
             |  }
             |  return $polyline;
             |}
             |""".stripMargin
        ) // 2d vs 3d fix

        // simplify via local functions
        val constructPolygonFunc = ctx.addNewFunction(
          constructPolygonFuncName,
          s"""
             |private $esriPolygon $constructPolygonFuncName(double[][][] $localBoundaries, double[][][][] $localHoles) {
             |  $esriPolygon $polygon = new $esriPolygon();
             |  for(int j=0; j<$localBoundaries.length; j++) {
             |    if($localBoundaries[j][0].length == 2) {
             |      $polygon.startPath($localBoundaries[j][0][0], $localBoundaries[j][0][1]);
             |    } else {
             |      $polygon.startPath($localBoundaries[j][0][0], $localBoundaries[j][0][1], $localBoundaries[j][0][2]);
             |    }
             |    for(int i=1; i<$localBoundaries[j].length; i++) {
             |      if(
             |        i < $localBoundaries[j].length - 1 || (
             |          $localBoundaries[j][0][0]!=$localBoundaries[j][i][0] &&
             |          $localBoundaries[j][0][0]!=$localBoundaries[j][i][0])
             |        )
             |      ) {
             |        if($localBoundaries[j][0].length == 2) {
             |          $polygon.lineTo($localBoundaries[j][i][0], $localBoundaries[j][i][1]);
             |        } else {
             |          $polygon.lineTo($localBoundaries[j][i][0], $localBoundaries[j][i][1], $localBoundaries[j][i][2]);
             |        }
             |      }
             |    }
             |  }
             |  for(int j=0; j<$localHoles.length; j++) {
             |    if($localHoles[j][0].length == 2) {
             |      $polygon.startPath($localHoles[j][0][0], $localHoles[j][0][1]);
             |    } else {
             |      $polygon.startPath($localHoles[j][0][0], $localHoles[j][0][1], $localHoles[j][0][2]);
             |    }
             |    for(int i=1; i<$localHoles[j].length; i++) {
             |      if(
             |        i < $localHoles[j].length - 1 || (
             |          $polygon[j][0][0]!=$localHoles[j][i][0] &&
             |          $polygon[j][0][0]!=$localHoles[j][i][0])
             |        )
             |      ) {
             |        if($localHoles[j][0].length == 2) {
             |          $polygon.lineTo($localHoles[j][i][0], $localHoles[j][i][1]);
             |        } else {
             |          $polygon.lineTo($localHoles[j][i][0], $localHoles[j][i][1], $localHoles[j][i][2]);
             |        }
             |      }
             |    }
             |  }
             |  return $polygon;
             |}
             |""".stripMargin
        )

        (
          s"""
             |$intJavaType $geomTypeId = ${CodeGenerator.getValue(eval, IntegerType, "0")};
             |$boundaryJavaType $boundaries = ${CodeGenerator.getValue(eval, BoundaryType, "1")};
             |$holesJavaType $holes = ${CodeGenerator.getValue(eval, HolesType, "2")};
             |$ogcGeom $geometry;
             |$spatialReference $spatialReferenceInstance = $spatialReference.create($spatialReferenceId);
             |switch ($geomTypeId) {
             |  case ${POINT.id}: {
             |    $geometry = new $ogcPoint($constructPointFunc($boundaries[0][0]), $spatialReferenceInstance);
             |    break;
             |  }
             |  case ${MULTIPOINT.id}: {
             |    $esriMultiPoint $multiPoint = new $esriMultiPoint();
             |    for(int i=0; i<$boundaries[0].length; i++) {
             |      $multiPoint.add($constructPointFunc($boundaries[0][i]));
             |    }
             |    $geometry = new $ogcMultiPoint($multiPoint, $spatialReferenceInstance);
             |    break;
             |  }
             |  case ${LINESTRING.id}: {
             |    $esriPolyline $polyline = $constructPolylineFunc($boundaries);
             |    $geometry = new $ogcLineString($polyline, 0, $spatialReferenceInstance);
             |    break;
             |  }
             |  case ${MULTILINESTRING.id}: {
             |    $geometry = new $ogcMultiLineString($constructPolylineFunc($boundaries), $spatialReferenceInstance);
             |    break;
             |  }
             |  case ${POLYGON.id}: {
             |    $geometry = new $ogcPolygon($constructPolygonFunc($boundaries, $holes), $spatialReferenceInstance);
             |    break;
             |  }
             |  case ${MULTIPOLYGON.id}: {
             |    $geometry = new $ogcMultiPolygon($constructPolygonFunc($boundaries, $holes), $spatialReferenceInstance);
             |    break;
             |  }
             |}
             |$boundaries null;
             |$holes = null;
             |""".stripMargin,
          geometry
        )

        // test if I can use Mosaic classes in codegen
    }

    override def toWKT(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        val inputGeom = ctx.freshName("inputGeom")
        val ogcGeometry = classOf[OGCGeometry].getName
        val javaStringType = CodeGenerator.javaType(StringType)
        (s"""$javaStringType """, inputGeom)
    }

    override def toWKB(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = ???

    override def toJSON(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = ???

    override def toHEX(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = ???

    override def toInternal(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = ???

}
