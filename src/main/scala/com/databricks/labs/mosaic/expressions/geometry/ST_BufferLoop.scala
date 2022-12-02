package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.{GeometryTransformBinaryExpression, WithExpressionInfo}
import org.apache.spark.sql.adapters.Column
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext

case class ST_BufferLoop(inputGeom: Expression, innerRadius: Expression, outerRadius: Expression)
    extends GeometryTransformBinaryExpression[ST_BufferLoop](inputGeom, innerRadius, outerRadius)
      with NullIntolerant {

    override def geomTransform(geometry: MosaicGeometry, param1: Any, param2: Any): Any = {
        val innerRadiusVal = param1.asInstanceOf[Double]
        val outerRadiusVal = param2.asInstanceOf[Double]
        geometry.buffer(outerRadiusVal).difference(geometry.buffer(innerRadiusVal))
    }

    override def geomTransformCodeGen(ctx: CodegenContext, geomRef: String, param1Ref: String, param2Ref: String): (String, String) = {
        val resultRef = ctx.freshName("result")
        val polygonClass = geometryAPI.geometryClass
        val resultCode = s"$polygonClass $resultRef = $geomRef.buffer($param2Ref).difference($geomRef.buffer($param1Ref));"
        (resultCode, resultRef)
    }

}

object ST_BufferLoop extends WithExpressionInfo {

    override def name: String = "st_buffer_loop"

    override def builder: Seq[Expression] => Expression =
        (children: Seq[Expression]) =>
            ST_BufferLoop(children(0), Column(children(1)).cast("double").expr, Column(children(2)).cast("double").expr)

}
