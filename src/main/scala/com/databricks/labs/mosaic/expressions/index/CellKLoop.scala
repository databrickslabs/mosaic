package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemFactory}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, ExpectsInputTypes, Expression, ExpressionDescription, ExpressionInfo, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

@ExpressionDescription(
  usage = "_FUNC_(cellId, k) - Returns k loop (hollow ring) for a given cell." +
      "The k loop is the set of cells that are within the k ring set of cells " +
      "but are not within the k-1 ring set of cells.",
  examples = """
    Examples:
      > SELECT _FUNC_(a, b);
       [622236721348804607, 622236721274716159, ...]
  """,
  since = "1.0"
)
case class CellKLoop(cellId: Expression, k: Expression, indexSystem: IndexSystem, geometryAPIName: String)
    extends BinaryExpression
      with ExpectsInputTypes
      with NullIntolerant
      with CodegenFallback {
  
    val geometryAPI: GeometryAPI = GeometryAPI(geometryAPIName)

    // noinspection DuplicatedCode
    override def inputTypes: Seq[DataType] =
        (left.dataType, right.dataType) match {
            case (LongType, IntegerType)   => Seq(LongType, IntegerType)
            case (StringType, IntegerType) => Seq(StringType, IntegerType)
            case _                         => throw new Error(s"Not supported data type: (${left.dataType}, ${right.dataType}).")
        }

    override def right: Expression = k

    override def left: Expression = cellId

    /** Expression output DataType. */
    override def dataType: DataType = ArrayType(indexSystem.getCellIdDataType)

    override def toString: String = s"grid_cellkloop($cellId, $k)"

    /** Overridden to ensure [[Expression.sql]] is properly formatted. */
    override def prettyName: String = "grid_cellkloop"

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
        val indices = indexSystem.kLoop(cellId, input2.asInstanceOf[Int])
        val serialized = ArrayData.toArrayData(indices.map(indexSystem.serializeCellId))
        serialized
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
        val res = CellKLoop(asArray(0), asArray(1), indexSystem, geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
        copy(cellId = newLeft, k = newRight)

}

object CellKLoop {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[CellKLoop].getCanonicalName,
          db.orNull,
          "grid_cellkloop",
          "_FUNC_(cellId, k) - Returns k loop (hollow ring) for a given cell.",
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a, b);
            |        [622236721348804607, 622236721274716159, ...]
            |  """.stripMargin,
          "",
          "collection_funcs",
          "1.0",
          "",
          "built-in"
        )

}
