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

final case class BNG_CellIntersectionAgg(
    inputChip: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[IntersectionAcc]
      with UnaryLike[Expression] {

    private def idType = inputChip.dataType.asInstanceOf[StructType].fields(0).dataType
    override lazy val deterministic: Boolean = true
    override val child: Expression = inputChip
    override val nullable: Boolean = false
    override val dataType: DataType = BNG.cellType(idType)
    override def withNewMutableAggBufferOffset(n: Int): ImperativeAggregate = copy(mutableAggBufferOffset = n)
    override def withNewInputAggBufferOffset(n: Int): ImperativeAggregate = copy(inputAggBufferOffset = n)
    override def prettyName: String = BNG_CellIntersectionAgg.name
    override protected def withNewChildInternal(newChild: Expression): BNG_CellIntersectionAgg = copy(inputChip = newChild)

    override def createAggregationBuffer(): IntersectionAcc = IntersectionAcc.empty
    override def serialize(buf: IntersectionAcc): Array[Byte] = buf.serialize
    override def deserialize(bytes: Array[Byte]): IntersectionAcc = IntersectionAcc.deserialize(bytes)

    override def update(buf: IntersectionAcc, input: InternalRow): IntersectionAcc = {
        val v = child.eval(input).asInstanceOf[InternalRow]
        val id = idType match {
            case StringType => BNG.parse(v.getString(0))
            case LongType   => v.getLong(0)
        }
        val isCore = v.getBoolean(1)
        val wkb = v.getBinary(2)
        buf.update(id, isCore, wkb)
    }

    override def merge(a: IntersectionAcc, b: IntersectionAcc): IntersectionAcc = a.merge(b)

    override def eval(buf: IntersectionAcc): Any = {
        require(buf.initialized, "empty aggregation buffer")
        val id = idType match {
            case StringType => UTF8String.fromString(BNG.format(buf.cellID))
            case LongType   => buf.cellID
        }
        if (buf.boundaryWkb eq null) {
            // all chips were core → whole cell
            InternalRow(id, true, JTS.toWKB(BNG.cellIdToGeometry(buf.cellID)))
        } else {
            InternalRow(id, false, buf.boundaryWkb)
        }
    }

}

object BNG_CellIntersectionAgg extends WithExpressionInfo {

    override def name: String = "gbx_bng_cellintersection_agg"
    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_CellIntersectionAgg(c.head)

    //TODO: ADD EXPRESSION INFO
    override def usageArgs: String = ""

    override def description: String = ""

    override def extendedUsageArgs: String = ""

    override def examples: String = ""

}
