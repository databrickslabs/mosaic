package com.dblabs.spatial.gridx.bng.agg

import com.dblabs.spatial.expressions.WithExpressionInfo
import com.dblabs.spatial.gridx.grid.BNG
import com.dblabs.spatial.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types._

final case class BNG_CellIntersectionAgg(
    inputChip: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[IntersectionAcc]
      with UnaryLike[Expression] {

    override lazy val deterministic: Boolean = true
    override val child: Expression = inputChip
    override val nullable: Boolean = false
    override val dataType: DataType = BNG.cellType(LongType)
    override def withNewMutableAggBufferOffset(n: Int): ImperativeAggregate = copy(mutableAggBufferOffset = n)
    override def withNewInputAggBufferOffset(n: Int): ImperativeAggregate = copy(inputAggBufferOffset = n)
    override def prettyName: String = "bng_cell_intersection_agg"
    override protected def withNewChildInternal(newChild: Expression): BNG_CellIntersectionAgg = copy(inputChip = newChild)

    override def createAggregationBuffer(): IntersectionAcc = IntersectionAcc.empty
    override def serialize(buf: IntersectionAcc): Array[Byte] = buf.serialize
    override def deserialize(bytes: Array[Byte]): IntersectionAcc = IntersectionAcc.deserialize(bytes)

    override def update(buf: IntersectionAcc, input: InternalRow): IntersectionAcc = {
        val v = child.eval(input).asInstanceOf[InternalRow] // (isCore:Boolean, cellID:Long, wkb:Binary)
        val isCore = v.getBoolean(0)
        val idx = v.getLong(1)
        val wkb = v.getBinary(2)
        buf.update(isCore, idx, wkb)
    }

    override def merge(a: IntersectionAcc, b: IntersectionAcc): IntersectionAcc = a.merge(b)

    override def eval(buf: IntersectionAcc): Any = {
        require(buf.initialized, "empty aggregation buffer")
        if (buf.boundaryWkb eq null) {
            // all chips were core → whole cell
            InternalRow(true, buf.cellID, JTS.toWKB(BNG.cellIdToGeometry(buf.cellID)))
        } else {
            InternalRow(false, buf.cellID, buf.boundaryWkb)
        }
    }

}

object BNG_CellIntersectionAgg extends WithExpressionInfo {

    override def name: String = "bng_cell_intersection_agg"
    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_CellIntersectionAgg(c.head)

}
