package com.dblabs.spatial.gridx.bng

import com.dblabs.spatial.gridx.grid.BNG
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, ExpressionInfo}
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

    override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
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

object BNG_KLoopExplode {

    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[BNG_KLoopExplode].getCanonicalName,
          db.orNull,
          "bng_kloopexplode",
          """
            |    _FUNC_(cell_id, resolution)) - Generates the cell based k loop (hollow ring) cell IDs set for the input
            |    cell ID and the input k value.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a, b);
            |        622236721274716159
            |        622236721274716160
            |        622236721274716161
            |        ...
            |
            |  """.stripMargin,
          "",
          "generator_funcs",
          "1.0",
          "",
          "built-in"
        )
}
