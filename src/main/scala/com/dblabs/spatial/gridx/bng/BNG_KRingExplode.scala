package com.dblabs.spatial.gridx.bng

import com.dblabs.spatial.expressions.{ExpressionConfig, GenericExpressionFactory, WithExpressionInfo}
import com.dblabs.spatial.gridx.grid.BNG
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, ExpressionInfo}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class BNG_KRingExplode(
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
                BNG.kRing(cid, kValue.asInstanceOf[Int])
                    .map(cid => InternalRow.fromSeq(Seq(UTF8String.fromString(BNG.format(cid)))))
            case l: Long       => BNG
                    .kRing(l, kValue.asInstanceOf[Int])
                    .map(cid => InternalRow.fromSeq(Seq(cid)))
            case _             => throw new IllegalArgumentException(s"Unsupported cellId type: ${cellIdValue.getClass.getName}")
        }
    }

    override def elementSchema: StructType = StructType(Seq(StructField("cellId", cellId.dataType)))

    override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(newChildren(0), newChildren(1))

}

object BNG_KRingExplode extends WithExpressionInfo {

    override def name: String = "bng_kringexplode"

    override def builder(expressionConfig: ExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[BNG_KRingExplode](2, expressionConfig)
    }

}
