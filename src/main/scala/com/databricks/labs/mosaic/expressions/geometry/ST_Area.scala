package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.api._
import com.databricks.labs.mosaic.expressions.core.MosaicUnaryExpression
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DoubleType

@ExpressionDescription(
  usage = "_FUNC_(geometry) - Returns the area of the geometry.",
  arguments = """
    Arguments:
        * geometry - a geometry
    """,
  examples = """
    Examples:
        > SELECT _FUNC_(ST_Point(1.0, 1.0));
         0.0
        > SELECT _FUNC_(ST_PolygonFromEnvelope(0.0, 0.0, 1.0, 1.0));
         1.0
    """,
  group = "geometry_funcs",
  since = "0.1.0"
)
case class ST_Area(
    inputGeom: Expression,
    indexSystemName: String,
    geometryAPIName: String
) extends MosaicUnaryExpression[ST_Area](inputGeom, DoubleType, Some(indexSystemName), Some(geometryAPIName)) {

    /**
      * ST_Area expression returns area covered by the
      * [[org.locationtech.jts.geom.Geometry]] instance extracted from inputGeom
      * expression.
      */

    override def nullSafeEval(input1: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geom = geometryAPI.geometry(input1, inputGeom.dataType)
        geom.getArea
    }

    override def copyImpl(child: Expression): MosaicUnaryExpression[ST_Area] = copy(inputGeom = child)

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
        nullSafeCodeGen(
          ctx,
          ev,
          eval => {
              val geometryAPI = GeometryAPI.apply(geometryAPIName)
              val (inCode, geomInRef) = ConvertToCodeGen.readGeometryCode(ctx, eval, inputGeom.dataType, geometryAPI)
              val areaStatement = geometryAPI.geometryAreaCode
              geometryAPI.codeGenTryWrap(s"""
                                            |$inCode
                                            |${ev.value} = $geomInRef.$areaStatement;
                                            |""".stripMargin)
          }
        )

}
