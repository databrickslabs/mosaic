package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionInfo, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

case class GridDistance(cellId: Expression, cellId2: Expression, indexSystem: IndexSystem, geometryAPIName: String)
    extends BinaryExpression
      with NullIntolerant
      with CodegenFallback {

    val geometryAPI: GeometryAPI = GeometryAPI(geometryAPIName)

    override def right: Expression = cellId2

    override def left: Expression = cellId

    /** Expression output DataType. */
    override def dataType: DataType = LongType

    override def toString: String = s"grid_distance($cellId, $cellId2)"

    /** Overridden to ensure [[Expression.sql]] is properly formatted. */
    override def prettyName: String = "grid_distance"

    /**
      * Generates a set of indices corresponding to kring call over the input
      * cell id.
      *
      * @param input1
      *   Any instance containing the cell id.
      * @param input2
      *   Any instance containing the k.
      * @return
      *   A set of indices.
      */
    // noinspection DuplicatedCode
    override def nullSafeEval(input1: Any, input2: Any): Any = {
        val cellId = indexSystem.formatCellId(input1, LongType).asInstanceOf[Long]
        val cellId2 = indexSystem.formatCellId(input2, LongType).asInstanceOf[Long]
        indexSystem.distance(cellId, cellId2)
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
        val res = GridDistance(asArray(0), asArray(1), indexSystem, geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
        makeCopy(Array[AnyRef](newLeft, newRight))

}

object GridDistance {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[GridDistance].getCanonicalName,
          db.orNull,
          "grid_distance",
          "_FUNC_(cellId, cellId2) - Returns k distance for given cells.",
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a, b);
            |        10
            |  """.stripMargin,
          "",
          "collection_funcs",
          "1.0",
          "",
          "built-in"
        )

}
