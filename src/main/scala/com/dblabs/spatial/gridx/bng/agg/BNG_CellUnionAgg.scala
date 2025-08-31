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

final case class BNG_CellUnionAgg(
    inputChip: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[UnionAcc]
      with UnaryLike[Expression] {

    override lazy val deterministic = true
    override val child: Expression = inputChip
    override val nullable = false
    override val dataType: DataType = BNG.cellType(LongType)
    override def withNewMutableAggBufferOffset(n: Int): ImperativeAggregate = copy(mutableAggBufferOffset = n)
    override def withNewInputAggBufferOffset(n: Int): ImperativeAggregate = copy(inputAggBufferOffset = n)
    override def prettyName: String = "bng_cell_union_agg"
    override protected def withNewChildInternal(newChild: Expression): BNG_CellUnionAgg = copy(inputChip = newChild)

    override def createAggregationBuffer(): UnionAcc = UnionAcc.empty
    override def serialize(b: UnionAcc): Array[Byte] = b.serialize
    override def deserialize(bytes: Array[Byte]): UnionAcc = UnionAcc.deserialize(bytes)

    override def update(b: UnionAcc, in: InternalRow): UnionAcc = {
        val r = child.eval(in).asInstanceOf[InternalRow] // (isCore:Boolean, id:Long, wkb:Binary)
        b.update(r.getBoolean(0), r.getLong(1), r.getBinary(2))
    }

    override def merge(a: UnionAcc, c: UnionAcc): UnionAcc = a.merge(c)

    override def eval(b: UnionAcc): Any = {
        require(b.initialized, "empty aggregation buffer")
        if (b.hasCore) InternalRow(true, b.cellID, JTS.toWKB(BNG.cellIdToGeometry(b.cellID)))
        else InternalRow(false, b.cellID, if (b.unionWkb eq null) JTS.toWKB(JTS.emptyPolygon) else b.unionWkb)
    }

}

object BNG_CellUnionAgg extends WithExpressionInfo {

    override def name: String = "bng_cell_union_agg"
    override def builder(): FunctionBuilder = c => new BNG_CellUnionAgg(c.head)

}
