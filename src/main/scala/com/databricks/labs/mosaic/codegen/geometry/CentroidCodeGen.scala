package com.databricks.labs.mosaic.codegen.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext

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
