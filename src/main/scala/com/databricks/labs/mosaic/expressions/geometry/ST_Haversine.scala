package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, QuaternaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{DataType, DoubleType}

case class ST_Haversine(lat1: Expression, lon1: Expression, lat2: Expression, lon2: Expression)
    extends QuaternaryExpression
      with CodegenFallback {

    override def first: Expression = lat1
    override def second: Expression = lon1
    override def third: Expression = lat2
    override def fourth: Expression = lon2

    override def dataType: DataType = DoubleType

    override def nullSafeEval(lat1: Any, lon1: Any, lat2: Any, lon2: Any): Any = {
        val c = math.Pi / 180

        val th1 = c * lat1.asInstanceOf[Double]
        val th2 = c * lat2.asInstanceOf[Double]
        val dph = c * (lon1.asInstanceOf[Double] - lon2.asInstanceOf[Double])

        val dz = math.sin(th1) - math.sin(th2)
        val dx = math.cos(dph) * math.cos(th1) - math.cos(th2)
        val dy = math.sin(dph) * math.cos(th1)

        val rad_distance = math.asin(math.sqrt(dx * dx + dy * dy + dz * dz) / 2) * 2
        val earth_radius = 6371.0088

        rad_distance * earth_radius
    }

    override def prettyName: String = "ST_HAVERSINE"

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(4).map(_.asInstanceOf[Expression])
        val res = ST_Haversine(asArray(0), asArray(1), asArray(2), asArray(3))
        res.copyTagsFrom(this)
        res
    }

    override def withNewChildrenInternal(
        newFirst: Expression,
        newSecond: Expression,
        newThird: Expression,
        newFourth: Expression
    ): Expression = {
        copy(newFirst, newSecond, newThird, newFourth)
    }

}

object ST_Haversine extends WithExpressionInfo {

    override def name: String = "st_haversine"

    override def usage: String = "_FUNC_(expr1, expr2, expr3, expr4) - Returns the haversine distance between lat/lon pairs in km."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a, b, c, d);
          |        {0.00463}
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_Haversine](4, expressionConfig)
    }

}
