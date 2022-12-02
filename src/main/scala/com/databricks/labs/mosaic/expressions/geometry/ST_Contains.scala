package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.{GeometryBooleanPredicateExpression, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.base.GenericExpressionFactory.getBaseBuilder
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext

case class ST_Contains(leftGeom: Expression, rightGeom: Expression)
    extends GeometryBooleanPredicateExpression[ST_Contains](leftGeom, rightGeom)
      with NullIntolerant {

    override def geomTransform(leftGeom: MosaicGeometry, rightGeom: MosaicGeometry): Boolean = {
        leftGeom.contains(rightGeom)
    }

    override def geomTransformCodeGen(ctx: CodegenContext, leftGeomRef: String, rightGeomRef: String): (String, String) = {
        val resultRef = ctx.freshName("result")
        val resultCode = s"boolean $resultRef = $leftGeomRef.contains($rightGeomRef);"
        (resultCode, resultRef)
    }

}

object ST_Contains extends WithExpressionInfo {

    override def name: String = "st_contains"

    override def builder: FunctionBuilder = getBaseBuilder[ST_Contains](2)

}
