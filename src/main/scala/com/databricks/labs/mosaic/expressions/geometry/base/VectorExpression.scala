package com.databricks.labs.mosaic.expressions.geometry.base

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.crs.CRSBoundsProvider
import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemFactory}
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.DataType

/**
  * Base class for all vector expressions. It provides the boilerplate for
  * creating a function builder for a given expression. It minimises amount of
  * code needed to create a new expression.
  */
trait VectorExpression {

    def getIndexSystem(expressionConfig: MosaicExpressionConfig): IndexSystem =
        IndexSystemFactory.getIndexSystem(expressionConfig.getIndexSystem)
    def getGeometryAPI(expressionConfig: MosaicExpressionConfig): GeometryAPI = GeometryAPI(expressionConfig.getGeometryAPI)

    def geometryAPI: GeometryAPI

    def mosaicGeomClass: String = geometryAPI.mosaicGeometryClass
    def geomClass: String = geometryAPI.geometryClass
    def CRSBoundsProviderClass: String = classOf[CRSBoundsProvider].getName
    def geometryAPIClass: String = classOf[GeometryAPI].getName

    /**
      * Generic serialisation method for the expression result. It serialises
      * the geometry if the expression returns a geometry. It passes the result
      * through if the expression returns a non-geometry.
      * @param result
      *   The result of the expression.
      * @param returnsGeometry
      *   Whether the expression returns a geometry.
      * @param dataType
      *   The data type of the result.
      * @return
      *   The serialised result.
      */
    def serialise(result: Any, returnsGeometry: Boolean, dataType: DataType): Any = {
        if (returnsGeometry) {
            geometryAPI.serialize(result.asInstanceOf[MosaicGeometry], dataType)
        } else {
            result
        }
    }

    /**
      * Generic serialisation codegen method for the expression. It provide
      * serialisation codegen for the geometry if the expression returns a
      * geometry. It yields empty codegen if the expression returns a
      * non-geometry.
      * @param resultRef
      *   The result of the expression.
      * @param returnsGeometry
      *   Whether the expression returns a geometry.
      * @param dataType
      *   The data type of the result.
      * @param ctx
      *   The codegen context.
      * @return
      *   The serialised result.
      */
    def serialiseCodegen(resultRef: String, returnsGeometry: Boolean, dataType: DataType, ctx: CodegenContext): (String, String) = {
        if (returnsGeometry) {
            val baseGeometryRef = ctx.freshName("baseGeometry")
            val (code, outputRef) = ConvertToCodeGen.writeGeometryCode(ctx, baseGeometryRef, dataType, geometryAPI)
            (
              s"""
                 |$geomClass $baseGeometryRef = $resultRef.getGeom();
                 |$code
                 |""".stripMargin,
              outputRef
            )
        } else {
            ("", resultRef) // noop code
        }
    }

    /**
      * Simplifies the creation of a geometry reference for the expression
      * codegen
      * @param geometryRef
      *   The geometry variable reference.
      * @return
      *   The mosaic geometry instance in codegen.
      */
    def mosaicGeometryRef(geometryRef: String): String = {
        s"${geometryAPI.mosaicGeometryClass}.apply($geometryRef)"
    }

}
