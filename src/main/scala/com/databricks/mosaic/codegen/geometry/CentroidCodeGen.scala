package com.databricks.mosaic.codegen.geometry

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext

import com.databricks.mosaic.core.geometry.api.GeometryAPI

case class CentroidCodeGen(
    geometryAPI: GeometryAPI
) {

    def centroid(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI, nDim: Int): (String, String) = {
        geometryAPI.name match {
            case "OGC" => CentroidCodeGenOGC.centroid(ctx, eval)
            case "JTS" => CentroidCodeGenJTS.centroid(ctx, eval, nDim)
        }
    }

}
