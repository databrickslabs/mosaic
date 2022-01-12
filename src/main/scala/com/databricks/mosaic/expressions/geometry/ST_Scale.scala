package com.databricks.mosaic.expressions.geometry

import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, NullIntolerant, TernaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType

import com.databricks.mosaic.core.geometry.api.GeometryAPI

@ExpressionDescription(
  usage = "_FUNC_(expr1, xd, yd) - Returns a new geometry scaled using xd for x axis and yd for y axis.",
  examples = """
    Examples:
      > SELECT _FUNC_(a, xd, yd);
       POLYGON ((...))
  """,
  since = "1.0"
)
case class ST_Scale(inputGeom: Expression, xd: Expression, yd: Expression, geometryAPIName: String)
    extends TernaryExpression
      with NullIntolerant
      with CodegenFallback {

    /**
      * ST_Scale expression returns are covered by the
      * [[org.locationtech.jts.geom.Geometry]] instance extracted from inputGeom
      * expression.
      */

    override def children: Seq[Expression] = Seq(inputGeom, xd, yd)

    // noinspection DuplicatedCode
    override def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geom = geometryAPI.geometry(input1, dataType)
        val xDist = input2.asInstanceOf[Double]
        val yDist = input3.asInstanceOf[Double]
        val output = geom.scale(xDist, yDist)
        geometryAPI.serialize(output, dataType)
    }

    /** Output Data Type */
    override def dataType: DataType = inputGeom.dataType

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(3).map(_.asInstanceOf[Expression])
        val res = ST_Scale(asArray(0), asArray(1), asArray(2), geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

}
