package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.{GeometryTransformUnaryExpression, WithExpressionInfo}
import org.apache.spark.sql.adapters.Column
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.DataType

case class ST_Buffer(inputGeom: Expression, radius: Expression)
    extends GeometryTransformUnaryExpression[ST_Buffer](inputGeom, radius)
      with NullIntolerant {

    override def left: Expression = inputGeom

    override def right: Expression = radius

    override def dataType: DataType = inputGeom.dataType

    override def geomTransform(geometry: MosaicGeometry, param1: Any): Any = {
        geometry.buffer(param1.asInstanceOf[Double])
    }

    override def geomTransformCodeGen(ctx: CodegenContext, geomRef: String, param1Ref: String): (String, String) = {
        val resultRef = ctx.freshName("result")
        val polygonClass = geometryAPI.geometryClass
        val resultCode = s"$polygonClass $resultRef = $geomRef.buffer($param1Ref);"
        (resultCode, resultRef)
    }

}

object ST_Buffer extends WithExpressionInfo {

    override def name: String = "st_buffer"

    override def builder: FunctionBuilder = (children: Seq[Expression]) => ST_Buffer(children(0), Column(children(1)).cast("double").expr)

}
