package com.databricks.mosaic.codegen.geometry

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext

import com.databricks.mosaic.core.geometry.api.GeometryAPI

trait GeometryIOCodeGen {

    def fromWKT(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String)

    def fromWKB(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String)

    def fromJSON(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String)

    def fromHex(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String)

    def fromInternal(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String)

    def toWKT(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String)

    def toWKB(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String)

    def toJSON(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String)

    def toHEX(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String)

    def toInternal(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String)

}
