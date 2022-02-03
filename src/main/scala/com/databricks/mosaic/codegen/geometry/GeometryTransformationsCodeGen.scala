package com.databricks.mosaic.codegen.geometry

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.DataType

import com.databricks.mosaic.core.geometry.api.GeometryAPI

object GeometryTransformationsCodeGen {

    def rotate(ctx: CodegenContext, geomEval: String, angleEval: String, dataType: DataType, geometryAPI: GeometryAPI): (String, String) = {
        geometryAPI.name match {
            case "OGC" => GeometryTransformationsCodeGenOGC.rotate(ctx, geomEval, angleEval, dataType, geometryAPI)
            case "JTS" => GeometryTransformationsCodeGenJTS.rotate(ctx, geomEval, angleEval, dataType, geometryAPI)
        }
    }

    def scale(ctx: CodegenContext, geomEval: String, xDist: String, yDist: String, dataType: DataType, geometryAPI: GeometryAPI): (String, String) = {
        geometryAPI.name match {
            case "OGC" => GeometryTransformationsCodeGenOGC.scale(ctx, geomEval, xDist, yDist, dataType, geometryAPI)
            case "JTS" => GeometryTransformationsCodeGenJTS.scale(ctx, geomEval, xDist, yDist, dataType, geometryAPI)
        }
    }

    def translate(ctx: CodegenContext, geomEval: String, xDist: String, yDist: String, dataType: DataType, geometryAPI: GeometryAPI): (String, String) = {
        geometryAPI.name match {
            case "OGC" => GeometryTransformationsCodeGenOGC.translate(ctx, geomEval, xDist, yDist, dataType, geometryAPI)
            case "JTS" => GeometryTransformationsCodeGenJTS.translate(ctx, geomEval, xDist, yDist, dataType, geometryAPI)
        }
    }
}
