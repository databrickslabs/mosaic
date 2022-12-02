package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.{GeometryAttributeExpression, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.base.GenericExpressionFactory.getBaseBuilder
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.DoubleType

case class ST_Area(inputGeom: Expression)
    extends GeometryAttributeExpression[ST_Area](inputGeom, outputType = DoubleType)
      with NullIntolerant {

    override def geomTransform(geometry: MosaicGeometry): Any = geometry.getArea

    override def geomTransformCodeGen(ctx: CodegenContext, geomRef: String): (String, String) = {
        val areaCode = geometryAPI.geometryAreaCode
        val resultRef = ctx.freshName("result")
        val code = s"double $resultRef = $geomRef.$areaCode;"
        (code, resultRef)
    }

}

object ST_Area extends WithExpressionInfo {

    override def name: String = "st_area"

    override def builder: FunctionBuilder = getBaseBuilder[ST_Area](1)

}
