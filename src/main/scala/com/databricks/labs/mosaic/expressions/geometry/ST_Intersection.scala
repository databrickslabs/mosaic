package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.{ESRI, JTS}
import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.{GeometryPredicateExpression, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.base.GenericExpressionFactory.getBaseBuilder
import com.esri.core.geometry._
import com.esri.core.geometry.ogc.OGCGeometry
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.locationtech.jts.geom.{Geometry => JTSGeometry}

case class ST_Intersection(leftGeom: Expression, rightGeom: Expression)
    extends GeometryPredicateExpression[ST_Intersection](leftGeom, rightGeom)
      with NullIntolerant {

    override def geomTransform(leftGeom: MosaicGeometry, rightGeom: MosaicGeometry): Any = {
        leftGeom.intersection(rightGeom)
    }

    override def geomTransformCodeGen(ctx: CodegenContext, leftGeomRef: String, rightGeomRef: String): (String, String) = {
        val resultGeomRef = ctx.freshName("resultGeom")
        val resultCode = geometryAPI match {
            case JTS  =>
                val jtsGeometryClass = classOf[JTSGeometry].getName
                s"$jtsGeometryClass $resultGeomRef = $leftGeomRef.intersection($rightGeomRef);"
            case ESRI =>
                val operatorIntersectionClass = classOf[OperatorIntersection].getName
                val operatorFactoryLocalClass = classOf[OperatorFactoryLocal].getName
                val esriOperatorClass = classOf[Operator].getName
                val geometryCursorClass = classOf[GeometryCursor].getName
                val ogcGeometryClass = classOf[OGCGeometry].getName
                val operatorName = ctx.freshName("operator")
                val cursorName = ctx.freshName("cursor")
                s"""
                   |$operatorIntersectionClass $operatorName = ($operatorIntersectionClass) $operatorFactoryLocalClass.getInstance().getOperator(
                   |    $esriOperatorClass.Type.Intersection
                   |);
                   |$geometryCursorClass $cursorName = $operatorName.execute(
                   |    $leftGeomRef.getEsriGeometryCursor(), $rightGeomRef.getEsriGeometryCursor(),
                   |    $leftGeomRef.getEsriSpatialReference(), null, -1
                   |);
                   |$ogcGeometryClass $resultGeomRef = $ogcGeometryClass.createFromEsriCursor(
                   |    $cursorName, $leftGeomRef.getEsriSpatialReference(), true
                   |);
                   |""".stripMargin

        }
        (resultCode, resultGeomRef)
    }

}

object ST_Intersection extends WithExpressionInfo {

    override def name: String = "st_intersection"

    override def builder: FunctionBuilder = getBaseBuilder[ST_Intersection](2)

}
