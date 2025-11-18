package com.databricks.labs.gbx.gridx.bng.agg

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

final case class BNG_CellUnionAgg(
    inputChip: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[UnionAcc]
      with UnaryLike[Expression] {

    private def idType = inputChip.dataType.asInstanceOf[StructType].fields(0).dataType
    override lazy val deterministic = true
    override val child: Expression = inputChip
    override val nullable = false
    override val dataType: DataType = BNG.cellType(idType)
    override def withNewMutableAggBufferOffset(n: Int): ImperativeAggregate = copy(mutableAggBufferOffset = n)
    override def withNewInputAggBufferOffset(n: Int): ImperativeAggregate = copy(inputAggBufferOffset = n)
    override def prettyName: String = BNG_CellUnionAgg.name
    override protected def withNewChildInternal(newChild: Expression): BNG_CellUnionAgg = copy(inputChip = newChild)

    override def createAggregationBuffer(): UnionAcc = UnionAcc.empty
    override def serialize(b: UnionAcc): Array[Byte] = b.serialize
    override def deserialize(bytes: Array[Byte]): UnionAcc = UnionAcc.deserialize(bytes)

    override def update(b: UnionAcc, in: InternalRow): UnionAcc = {
        val r = child.eval(in).asInstanceOf[InternalRow] //
        val cellId = idType match {
            case StringType => BNG.parse(r.getString(0))
            case LongType   => r.getLong(0)
        }
        b.update(cellId, r.getBoolean(1), r.getBinary(2))
    }

    override def merge(a: UnionAcc, c: UnionAcc): UnionAcc = a.merge(c)

    override def eval(b: UnionAcc): Any = {
        require(b.initialized, "empty aggregation buffer")
        val id = idType match {
            case StringType => UTF8String.fromString(BNG.format(b.cellID))
            case LongType   => b.cellID
        }
        if (b.hasCore) InternalRow(id, true, JTS.toWKB(BNG.cellIdToGeometry(b.cellID)))
        else InternalRow(id, false, if (b.unionWkb eq null) JTS.toWKB(JTS.emptyPolygon) else b.unionWkb)
    }

}

object BNG_CellUnionAgg extends WithExpressionInfo {

    override def name: String = "gbx_bng_cellunion_agg"
    override def builder(): FunctionBuilder = c => new BNG_CellUnionAgg(c.head)

    //TODO: ADD EXPRESSION INFO
    override def usageArgs: String = ""

    override def description: String = ""

    override def extendedUsageArgs: String = ""

    override def examples: String = ""

}
