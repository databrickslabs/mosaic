package com.databricks.labs.mosaic.codegen.format

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext

case class MosaicGeometryIOCodeGen(
    geometryAPI: GeometryAPI
) extends GeometryIOCodeGen {

    override def fromWKT(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        geometryAPI.name match {
            case "ESRI" => MosaicGeometryIOCodeGenESRI.fromWKT(ctx, eval, geometryAPI)
            case "JTS" => MosaicGeometryIOCodeGenJTS.fromWKT(ctx, eval, geometryAPI)
        }
    }

    override def fromWKB(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        geometryAPI.name match {
            case "ESRI" => MosaicGeometryIOCodeGenESRI.fromWKB(ctx, eval, geometryAPI)
            case "JTS" => MosaicGeometryIOCodeGenJTS.fromWKB(ctx, eval, geometryAPI)
        }
    }

    override def fromJSON(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        geometryAPI.name match {
            case "ESRI" => MosaicGeometryIOCodeGenESRI.fromJSON(ctx, eval, geometryAPI)
            case "JTS" => MosaicGeometryIOCodeGenJTS.fromJSON(ctx, eval, geometryAPI)
        }
    }

    override def fromHex(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        geometryAPI.name match {
            case "ESRI" => MosaicGeometryIOCodeGenESRI.fromHex(ctx, eval, geometryAPI)
            case "JTS" => MosaicGeometryIOCodeGenJTS.fromHex(ctx, eval, geometryAPI)
        }
    }

    override def fromInternal(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        geometryAPI.name match {
            case "ESRI" => MosaicGeometryIOCodeGenESRI.fromInternal(ctx, eval, geometryAPI)
            case "JTS" => MosaicGeometryIOCodeGenJTS.fromInternal(ctx, eval, geometryAPI)
        }
    }

    override def toWKT(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        geometryAPI.name match {
            case "ESRI" => MosaicGeometryIOCodeGenESRI.toWKT(ctx, eval, geometryAPI)
            case "JTS" => MosaicGeometryIOCodeGenJTS.toWKT(ctx, eval, geometryAPI)
        }
    }

    override def toWKB(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        geometryAPI.name match {
            case "ESRI" => MosaicGeometryIOCodeGenESRI.toWKB(ctx, eval, geometryAPI)
            case "JTS" => MosaicGeometryIOCodeGenJTS.toWKB(ctx, eval, geometryAPI)
        }
    }

    override def toJSON(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        geometryAPI.name match {
            case "ESRI" => MosaicGeometryIOCodeGenESRI.toJSON(ctx, eval, geometryAPI)
            case "JTS" => MosaicGeometryIOCodeGenJTS.toJSON(ctx, eval, geometryAPI)
        }
    }

    override def toHEX(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        geometryAPI.name match {
            case "ESRI" => MosaicGeometryIOCodeGenESRI.toHEX(ctx, eval, geometryAPI)
            case "JTS" => MosaicGeometryIOCodeGenJTS.toHEX(ctx, eval, geometryAPI)
        }
    }

    override def toInternal(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
        geometryAPI.name match {
            case "ESRI" => MosaicGeometryIOCodeGenESRI.toInternal(ctx, eval, geometryAPI)
            case "JTS" => MosaicGeometryIOCodeGenJTS.toInternal(ctx, eval, geometryAPI)
        }
    }

}
