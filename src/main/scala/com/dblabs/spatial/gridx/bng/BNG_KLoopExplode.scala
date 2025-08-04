package com.dblabs.spatial.gridx.bng

import com.dblabs.spatial.expressions.{ExpressionConfig, GenericExpressionFactory, WithExpressionInfo}
import com.dblabs.spatial.gridx.grid.BNG
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

// TODO: this needs some refactoring to be more consistent with the rest of the codebase
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
        if (cellIdValue == null || kValue == null) {
            Seq.empty
        } else {
            val cid = cellIdValue match {
                case s: UTF8String => BNG.parse(s.toString)
                case l: Long => l
                case _ => throw new IllegalArgumentException(s"Unsupported cellId type: ${cellIdValue.getClass.getName}")
            }
            BNG.kLoop(cid, kValue.asInstanceOf[Int]).map {
                cellId => cellIdValue match {
                    case _: UTF8String => InternalRow.fromSeq(Seq(UTF8String.fromString(BNG.format(cellId))))
                    case _: Long => InternalRow.fromSeq(Seq(cellId))
                    case _ => throw new IllegalArgumentException(s"Unsupported cellId type: ${cellIdValue.getClass.getName}")
                }
            }
        }
    }

    override def elementSchema: StructType = StructType(Seq(StructField("cellId", cellId.dataType)))

    override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(newChildren(0), newChildren(1))

}

object BNG_KLoopExplode extends WithExpressionInfo {

    override def name: String = "bng_kloopexplode"

    override def builder(expressionConfig: ExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[BNG_KLoopExplode](2, expressionConfig)
    }
}
