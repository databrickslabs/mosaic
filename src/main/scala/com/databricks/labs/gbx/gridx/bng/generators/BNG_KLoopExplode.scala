package com.databricks.labs.gbx.gridx.bng.generators

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.gridx.grid.BNG
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class BNG_KLoopExplode(
    cellId: Expression,
    k: Expression
) extends CollectionGenerator
      with Serializable
      with CodegenFallback {

    override def position: Boolean = false
    override def inline: Boolean = false
    override def children: Seq[Expression] = Seq(cellId, k)

    override def eval(input: InternalRow): IterableOnce[InternalRow] = {
        val cellIdValue = cellId.eval(input)
        val kValue = k.eval(input)

        if (cellIdValue == null || kValue == null) return Iterator.empty[InternalRow]

        cellIdValue match {
            case s: UTF8String =>
                val cid = BNG.parse(s.toString)
                BNG.kLoop(cid, kValue.asInstanceOf[Int])
                    .map(cid => InternalRow.fromSeq(Seq(UTF8String.fromString(BNG.format(cid)))))
            case l: Long       => BNG
                    .kLoop(l, kValue.asInstanceOf[Int])
                    .map(cid => InternalRow.fromSeq(Seq(cid)))
            case _             => throw new IllegalArgumentException(s"Unsupported cellId type: ${cellIdValue.getClass.getName}")
        }
    }

    override def elementSchema: StructType = StructType(Seq(StructField("cellId", cellId.dataType)))

    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

object BNG_KLoopExplode extends WithExpressionInfo {

    override def name: String = "gbx_bng_kloopexplode"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_KLoopExplode(c(0), c(1))

    //TODO: ADD EXPRESSION INFO
    override def usageArgs: String = ""

    override def description: String = ""

    override def extendedUsageArgs: String = ""

    override def examples: String = ""

}
