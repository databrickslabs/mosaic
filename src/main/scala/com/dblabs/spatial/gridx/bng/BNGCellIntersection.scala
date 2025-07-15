package com.dblabs.spatial.gridx.bng

import com.databricks.labs.mosaic.core.types.ChipType
import com.dblabs.spatial.expressions._
import com.dblabs.spatial.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.Geometry

case class BNGCellIntersection(
    leftChip: Expression,
    rightChip: Expression
) extends InvokedExpression
    with WithNewChildren {

    override def children: Seq[Expression] = Seq(leftChip, rightChip)
    override def dataType: DataType = ChipType(LongType)
    override def nullable: Boolean = true
    override def prettyName: String = "bng_cell_intersection"
    override def replacement: Expression = invoke(BNGCellIntersection)

}

object BNGCellIntersection extends WithExpressionInfo {

    def eval(chip1: InternalRow, chip2: InternalRow): Any = {
        val index_id = chip1.getLong(1)
        require(chip2.getLong(1) == index_id, "can only intersect chips based on the same grid cell")

        if (chip2.getBoolean(0)) {
            chip1
        } else if (chip1.getBoolean(0)) {
            chip2
        } else {
            val leftGeom: Geometry = JTS.fromWKB(chip1.getBinary(2))
            val rightGeom: Geometry = JTS.fromWKB(chip2.getBinary(2))
            val intersection = leftGeom.intersection(rightGeom)
            val intersectionWKB = JTS.toWKB(intersection)
            InternalRow(false, index_id, intersectionWKB)
        }
    }

    override def name: String = "bng_cell_intersection"

    /**
     * Returns the expression builder (parser for spark SQL).
     *
     * @return
     * An expression builder.
     */
    override def builder(expressionConfig: ExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[BNGCellIntersection](2, expressionConfig)
    }
}
