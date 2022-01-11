package com.databricks.mosaic.codegen.geometry

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext

import com.databricks.mosaic.core.geometry.api.GeometryAPI

case class MosaicGeometryIOCodeGen(
    geometryAPI: GeometryAPI
) extends GeometryIOCodeGen {

    override def fromWKT(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        geometryAPI.name match {
            case "OGC" => MosaicGeometryIOCodeGenOGC.fromWKT(ctx, eval, geometryAPI)
            case "JTS" => MosaicGeometryIOCodeGenJTS.fromWKT(ctx, eval, geometryAPI)
        }
    }

    override def fromWKB(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        geometryAPI.name match {
            case "OGC" => MosaicGeometryIOCodeGenOGC.fromWKB(ctx, eval, geometryAPI)
            case "JTS" => MosaicGeometryIOCodeGenJTS.fromWKB(ctx, eval, geometryAPI)
        }
    }

    override def fromJSON(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        geometryAPI.name match {
            case "OGC" => MosaicGeometryIOCodeGenOGC.fromJSON(ctx, eval, geometryAPI)
            case "JTS" => MosaicGeometryIOCodeGenJTS.fromJSON(ctx, eval, geometryAPI)
        }
    }

    override def fromHex(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        geometryAPI.name match {
            case "OGC" => MosaicGeometryIOCodeGenOGC.fromHex(ctx, eval, geometryAPI)
            case "JTS" => MosaicGeometryIOCodeGenJTS.fromHex(ctx, eval, geometryAPI)
        }
    }

    override def fromInternal(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        geometryAPI.name match {
            case "OGC" => MosaicGeometryIOCodeGenOGC.fromInternal(ctx, eval, geometryAPI)
            case "JTS" => MosaicGeometryIOCodeGenJTS.fromInternal(ctx, eval, geometryAPI)
        }
    }

    override def toWKT(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        geometryAPI.name match {
            case "OGC" => MosaicGeometryIOCodeGenOGC.toWKT(ctx, eval, geometryAPI)
            case "JTS" => MosaicGeometryIOCodeGenJTS.toWKT(ctx, eval, geometryAPI)
        }
    }

    override def toWKB(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        geometryAPI.name match {
            case "OGC" => MosaicGeometryIOCodeGenOGC.toWKB(ctx, eval, geometryAPI)
            case "JTS" => MosaicGeometryIOCodeGenJTS.toWKB(ctx, eval, geometryAPI)
        }
    }

    override def toJSON(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        geometryAPI.name match {
            case "OGC" => MosaicGeometryIOCodeGenOGC.toJSON(ctx, eval, geometryAPI)
            case "JTS" => MosaicGeometryIOCodeGenJTS.toJSON(ctx, eval, geometryAPI)
        }
    }

    override def toHEX(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        geometryAPI.name match {
            case "OGC" => MosaicGeometryIOCodeGenOGC.toHEX(ctx, eval, geometryAPI)
            case "JTS" => MosaicGeometryIOCodeGenJTS.toHEX(ctx, eval, geometryAPI)
        }
    }

    override def toInternal(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        geometryAPI.name match {
            case "OGC" => MosaicGeometryIOCodeGenOGC.toInternal(ctx, eval, geometryAPI)
            case "JTS" => MosaicGeometryIOCodeGenJTS.toInternal(ctx, eval, geometryAPI)
        }
    }

}
