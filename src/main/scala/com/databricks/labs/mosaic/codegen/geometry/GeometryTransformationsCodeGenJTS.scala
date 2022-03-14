package com.databricks.labs.mosaic.codegen.geometry

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.MosaicGeometryJTS
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import org.locationtech.jts.geom.Geometry

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.DataType

object GeometryTransformationsCodeGenJTS {

    def rotate(ctx: CodegenContext, geomEval: String, angleEval: String, dataType: DataType, geometryAPI: GeometryAPI): (String, String) = {
        val (inCode, geomInRef) = ConvertToCodeGen.readGeometryCode(ctx, geomEval, dataType, geometryAPI)
        val tmpGeom = ctx.freshName("tmpGeom")
        val (outCode, geomOutRef) = ConvertToCodeGen.writeGeometryCode(ctx, tmpGeom, dataType, geometryAPI)
        val jtsGeometryClass = classOf[Geometry].getName
        val mosaicGeometryJTSClass = classOf[MosaicGeometryJTS].getName
        (
          s"""
             |$inCode
             |$jtsGeometryClass $tmpGeom = (($mosaicGeometryJTSClass)$mosaicGeometryJTSClass.apply($geomInRef).rotate($angleEval)).getGeom();
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
        val jtsGeometryClass = classOf[Geometry].getName
        val mosaicGeometryJTSClass = classOf[MosaicGeometryJTS].getName
        (
          s"""
             |$inCode
             |$jtsGeometryClass $tmpGeom = (($mosaicGeometryJTSClass)$mosaicGeometryJTSClass.apply($geomInRef).scale($xDist, $yDist)).getGeom();
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
        val jtsGeometryClass = classOf[Geometry].getName
        val mosaicGeometryJTSClass = classOf[MosaicGeometryJTS].getName
        (
          s"""
             |$inCode
             |$jtsGeometryClass $tmpGeom = (($mosaicGeometryJTSClass)$mosaicGeometryJTSClass.apply($geomInRef).translate($xDist, $yDist)).getGeom();
             |$outCode
             |""".stripMargin,
          geomOutRef
        )
    }

}
