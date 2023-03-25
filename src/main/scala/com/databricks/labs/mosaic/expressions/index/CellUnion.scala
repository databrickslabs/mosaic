package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.types.ChipType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
    BinaryExpression,
    ExpectsInputTypes,
    Expression,
    ExpressionDescription,
    ExpressionInfo,
    NullIntolerant
}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

@ExpressionDescription(
  usage = "_FUNC_(leftChip, rightChip) - Returns the chip describing the topological union of the input chips",
  examples = """
    Examples:
      > SELECT _FUNC_(a, b);
       {is_core: false, index_id: 590418571381702655, wkb: ...}
  """,
  since = "1.0"
)
case class CellUnion(leftChip: Expression, rightChip: Expression, indexSystem: IndexSystem, geometryAPIName: String)
    extends BinaryExpression
      with ExpectsInputTypes
      with NullIntolerant
      with CodegenFallback {

    val geometryAPI: GeometryAPI = GeometryAPI(geometryAPIName)

    // noinspection DuplicatedCode
    override def inputTypes: Seq[DataType] = Seq(ChipType(LongType), ChipType(LongType))

    override def right: Expression = rightChip

    override def left: Expression = leftChip

    /** Expression output DataType. */
    override def dataType: DataType = ChipType(LongType)

    override def toString: String = s"grid_cell_union($leftChip, $rightChip)"

    /** Overridden to ensure [[Expression.sql]] is properly formatted. */
    override def prettyName: String = "grid_cell_union"

    /**
      * Calculates the chip describing the topological union of the input chips.
      *
      * @param input1
      *   Any instance containing the left chip.
      * @param input2
      *   Any instance containing the right chip.
      * @return
      *   A chip.
      */
    // noinspection DuplicatedCode
    override def nullSafeEval(input1: Any, input2: Any): Any = {
        val chip1 = input1.asInstanceOf[InternalRow]
        val chip2 = input2.asInstanceOf[InternalRow]

        val index_id = chip1.getLong(1)
        require(chip2.getLong(1) == index_id, "Can only union chips with the same grid cell id")

        if (chip1.getBoolean(0)) {
            chip1
        } else if (chip2.getBoolean(0)) {
            chip2
        } else {
            val leftGeom: MosaicGeometry = geometryAPI.geometry(chip1.getBinary(2), "WKB")
            val rightGeom: MosaicGeometry = geometryAPI.geometry(chip2.getBinary(2), "WKB")
            val union = leftGeom.union(rightGeom).toWKB
            InternalRow(false, index_id, union)
        }
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
        val res = CellUnion(asArray(0), asArray(1), indexSystem, geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
        copy(leftChip = newLeft, rightChip = newRight)

}

object CellUnion {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[CellUnion].getCanonicalName,
          db.orNull,
          "grid_cell_union",
          "_FUNC_(leftChip, rightChip) - Returns union between chips.",
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a, b);
            |        {is_core: false, index_id: 590418571381702655, wkb: ...}
            |  """.stripMargin,
          "",
          "collection_funcs",
          "1.0",
          "",
          "built-in"
        )

}
