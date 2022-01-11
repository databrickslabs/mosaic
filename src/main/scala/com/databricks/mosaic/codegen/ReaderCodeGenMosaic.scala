package com.databricks.mosaic.codegen

import com.esri.core.geometry.{MultiPoint => EsriMultiPoint, Point => EsriPoint}
import com.esri.core.geometry.ogc.{OGCGeometry, OGCPoint}
import org.locationtech.jts.geom.{Geometry => JTSGeometry}
import org.locationtech.jts.io.WKBReader

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.types.{IntegerType, StringType}

import com.databricks.mosaic.core.geometry.api.GeometryAPI
import com.databricks.mosaic.core.types.{BoundaryType, HolesType}
import com.databricks.mosaic.core.types.model.GeometryTypeEnum._

trait ReaderCodeGenMosaic {

    def fromInternal(
        ctx: CodegenContext,
        eval: String,
        geometryAPI: GeometryAPI
    ): (String, String) = {
        val inputGeom = ctx.freshName("inputGeom")
        val stringJavaType = CodeGenerator.javaType(StringType)
        val intJavaType = CodeGenerator.javaType(IntegerType)
        val boundaryJavaType = CodeGenerator.javaType(BoundaryType)
        val holesJavaType = CodeGenerator.javaType(HolesType)
        val geomTypeId = ctx.freshName("geomTypeId")
        val tmpHolder = ctx.freshName("tmpHolder")
        val bytes = ctx.freshName("bytes")
        val boundaries = ctx.freshName("boundaries")
        val holes = ctx.freshName("holes")
        val wkbReader = classOf[WKBReader].getName
        geometryAPI.name match {
            case "OGC" =>
                val ogcGeom = classOf[OGCGeometry].getName
                val ogcPoint = classOf[OGCPoint].getName
                val esriPoint = classOf[EsriPoint].getName
                val esriMultiPoint = classOf[EsriMultiPoint].getName
                val geometry = ctx.freshName("geometry")
                (
                  s"""
                     |$intJavaType $geomTypeId = ${CodeGenerator.getValue(eval, IntegerType, "0")};
                     |$boundaryJavaType $boundaries = ${CodeGenerator.getValue(eval, BoundaryType, "1")};
                     |$holesJavaType $holes = ${CodeGenerator.getValue(eval, HolesType, "2")};
                     |switch ($geomTypeId) {
                     |  case ${POINT.id}: {
                     |    $ogcGeom $geometry;
                     |    if ($boundaries[0][0].length == 2) {
                     |      $geometry = new $ogcPoint(new $esriPoint($boundaries[0][0][0], $boundaries[0][0][1]));
                     |    } else {
                     |      $geometry = new $ogcPoint(new $esriPoint($boundaries[0][0][0], $boundaries[0][0][1], $boundaries[0][0][2]));
                     |    }
                     |    break;
                     |  }
                     |  case ${MULTIPOINT.id}: {
                     |
                     |    break;
                     |  }
                     |  case ${LINESTRING.id}: {
                     |    break;
                     |  }
                     |  case ${MULTILINESTRING.id}: {
                     |    break;
                     |  }
                     |  case ${POLYGON.id}: {
                     |    break;
                     |  }
                     |  case ${MULTIPOLYGON.id}: {
                     |    break;
                     |  }
                     |}
                     |$stringJavaType $tmpHolder = ${CodeGenerator.getValue(eval, StringType, "0")};
                     |byte[] $bytes = $wkbReader.hexToBytes($tmpHolder);
                     |$ogcGeom $inputGeom = $ogcGeom.fromBinary($bytes);
                     |$bytes = null;
                     |$tmpHolder = null;
                     |""".stripMargin,
                  inputGeom
                )
            case "JTS" =>
                val jtsGeom = classOf[JTSGeometry].getName
                (
                  s"""
                     |$stringJavaType $tmpHolder = ${CodeGenerator.getValue(eval, StringType, "0")};
                     |byte[] $bytes = $wkbReader.hexToBytes($tmpHolder);
                     |$jtsGeom $inputGeom = new $wkbReader().read($eval);
                     |$bytes = null;
                     |$tmpHolder = null;
                     |""".stripMargin,
                  inputGeom
                )
        }
    }

}
