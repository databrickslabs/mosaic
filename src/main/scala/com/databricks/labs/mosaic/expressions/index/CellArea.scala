package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import org.apache.spark.sql.catalyst.expressions.{
    ExpectsInputTypes,
    Expression,
    ExpressionDescription,
    ExpressionInfo,
    NullIntolerant,
    UnaryExpression
}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

@ExpressionDescription(
  usage = "_FUNC_(cellId) - Returns the area in km^2 for a given cell.",
  examples = """
    Examples:
      > SELECT _FUNC_(a);
       [1.057031]
  """,
  since = "1.0"
)
case class CellArea(cellId: Expression, indexSystem: IndexSystem, geometryAPIName: String)
    extends UnaryExpression
      with ExpectsInputTypes
      with NullIntolerant
      with CodegenFallback {

    val geometryAPI: GeometryAPI = GeometryAPI(geometryAPIName)

    // noinspection DuplicatedCode
    override def inputTypes: Seq[DataType] =
        child.dataType match {
            case LongType   => Seq(LongType)
            case StringType => Seq(StringType)
            case _          => throw new Error(s"Not supported data type: (${child.dataType}).")
        }

    override def child: Expression = cellId

    /** Expression output DataType. */
    override def dataType: DataType = DoubleType

    override def toString: String = s"grid_cellarea($cellId)"

    /** Overridden to ensure [[Expression.sql]] is properly formatted. */
    override def prettyName: String = "GRID_CELLAREA"

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
    override def nullSafeEval(input: Any): Any = {
        val cellId = indexSystem.formatCellId(input, LongType).asInstanceOf[Long]
        val res = indexSystem.area(cellId)
        res
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(1).map(_.asInstanceOf[Expression])
        val res = CellArea(asArray(0), indexSystem, geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override protected def withNewChildInternal(newChild: Expression): Expression = copy(cellId = newChild)

}

object CellArea {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[CellArea].getCanonicalName,
          db.orNull,
          "grid_cellarea",
          "_FUNC_(cellId) - Returns area of a given cell in km^2.",
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a);
            |        [1.057031]
            |  """.stripMargin,
          "",
          "collection_funcs",
          "1.0",
          "",
          "built-in"
        )

}
