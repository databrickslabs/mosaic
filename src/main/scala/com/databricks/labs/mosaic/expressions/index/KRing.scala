package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.index.IndexSystemID
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, ExpectsInputTypes, Expression, ExpressionDescription, ExpressionInfo, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

@ExpressionDescription(
  usage = "_FUNC_(cellId, k) - Returns k ring for a given cell.",
  examples = """
    Examples:
      > SELECT _FUNC_(a, b);
       [622236721348804607, 622236721274716159, ...]
  """,
  since = "1.0"
)
case class KRing(cellId: Expression, k: Expression, indexSystemName: String, geometryAPIName: String)
    extends BinaryExpression
      with ExpectsInputTypes
      with NullIntolerant
      with CodegenFallback {

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
    override def dataType: DataType = ArrayType(LongType)

    override def toString: String = s"polyfill($cellId, $k)"

    /** Overridden to ensure [[Expression.sql]] is properly formatted. */
    override def prettyName: String = "kring"

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
        val indexSystem = IndexSystemID.getIndexSystem(IndexSystemID(indexSystemName))
        val indices = indexSystem.kRing(input1.asInstanceOf[Long], input2.asInstanceOf[Int])
        val serialized = ArrayData.toArrayData(indices.toArray)
        serialized
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
        val res = Polyfill(asArray(0), asArray(1), indexSystemName, geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
        copy(cellId = newLeft, k = newRight)

}

object KRing {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
            classOf[KRing].getCanonicalName,
            db.orNull,
            "kring",
            "_FUNC_(cellId, k) - Returns k ring for a given cell.",
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
