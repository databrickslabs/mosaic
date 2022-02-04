package com.databricks.mosaic.codegen.geometry

import com.esri.core.geometry.ogc.OGCGeometry

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.DataType

import com.databricks.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.mosaic.core.geometry.MosaicGeometryOGC
import com.databricks.mosaic.core.geometry.api.GeometryAPI

object GeometryTransformationsCodeGenOGC {

    def rotate(ctx: CodegenContext, geomEval: String, angleEval: String, dataType: DataType, geometryAPI: GeometryAPI): (String, String) = {
        val (inCode, geomInRef) = ConvertToCodeGen.readGeometryCode(ctx, geomEval, dataType, geometryAPI)
        val tmpGeom = ctx.freshName("tmpGeom")
        val (outCode, geomOutRef) = ConvertToCodeGen.writeGeometryCode(ctx, tmpGeom, dataType, geometryAPI)
        val ogcGeometryClass = classOf[OGCGeometry].getName
        val mosaicGeometryOGCClass = classOf[MosaicGeometryOGC].getName
        (
          s"""
             |$inCode
             |$ogcGeometryClass $tmpGeom = (($mosaicGeometryOGCClass)$mosaicGeometryOGCClass.apply($geomInRef).rotate($angleEval)).getGeom();
             |$outCode
             |""".stripMargin,
          geomOutRef
        )
    }

    def scale(
        ctx: CodegenContext,
        geomEval: String,
        xDist: String,
        yDist: String,
        dataType: DataType,
        geometryAPI: GeometryAPI
    ): (String, String) = {
        val (inCode, geomInRef) = ConvertToCodeGen.readGeometryCode(ctx, geomEval, dataType, geometryAPI)
        val tmpGeom = ctx.freshName("tmpGeom")
        val (outCode, geomOutRef) = ConvertToCodeGen.writeGeometryCode(ctx, tmpGeom, dataType, geometryAPI)
        val ogcGeometryClass = classOf[OGCGeometry].getName
        val mosaicGeometryOGCClass = classOf[MosaicGeometryOGC].getName
        (
          s"""
             |$inCode
             |$ogcGeometryClass $tmpGeom = (($mosaicGeometryOGCClass)$mosaicGeometryOGCClass.apply($geomInRef).scale($xDist, $yDist)).getGeom();
             |$outCode
             |""".stripMargin,
          geomOutRef
        )
    }

    def translate(
        ctx: CodegenContext,
        geomEval: String,
        xDist: String,
        yDist: String,
        dataType: DataType,
        geometryAPI: GeometryAPI
    ): (String, String) = {
        val (inCode, geomInRef) = ConvertToCodeGen.readGeometryCode(ctx, geomEval, dataType, geometryAPI)
        val tmpGeom = ctx.freshName("tmpGeom")
        val (outCode, geomOutRef) = ConvertToCodeGen.writeGeometryCode(ctx, tmpGeom, dataType, geometryAPI)
        val ogcGeometryClass = classOf[OGCGeometry].getName
        val mosaicGeometryOGCClass = classOf[MosaicGeometryOGC].getName
        (
          s"""
             |$inCode
             |$ogcGeometryClass $tmpGeom = (($mosaicGeometryOGCClass)$mosaicGeometryOGCClass.apply($geomInRef).translate($xDist, $yDist)).getGeom();
             |$outCode
             |""".stripMargin,
          geomOutRef
        )
    }

}
