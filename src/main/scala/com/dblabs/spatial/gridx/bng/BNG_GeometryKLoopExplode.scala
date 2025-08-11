package com.dblabs.spatial.gridx.bng

import com.dblabs.spatial.gridx.grid.BNG
import com.dblabs.spatial.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, ExpressionInfo}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class BNG_GeometryKLoopExplode(
    geom: Expression,
    resolution: Expression,
    k: Expression
) extends CollectionGenerator
      with Serializable
      with CodegenFallback {

    override def position: Boolean = false
    override def inline: Boolean = false
    override def children: Seq[Expression] = Seq(geom, resolution, k)

    // noinspection DuplicatedCode
    override def checkInputDataTypes(): TypeCheckResult = {
        if (!Seq(BinaryType, StringType).contains(geom.dataType)) {
            TypeCheckResult.TypeCheckFailure("Unsupported geom type.")
        } else if (!Seq(IntegerType, StringType).contains(resolution.dataType)) {
            TypeCheckResult.TypeCheckFailure("Unsupported resolution type.")
        } else if (!Seq(IntegerType).contains(k.dataType)) {
            TypeCheckResult.TypeCheckFailure("Unsupported k type.")
        } else {
            TypeCheckResult.TypeCheckSuccess
        }
    }

    // noinspection DuplicatedCode
    override def eval(input: InternalRow): IterableOnce[InternalRow] = {
        val geometryRaw = geom.eval(input)
        val resolutionRaw = resolution.eval(input)
        val kRaw = k.eval(input)
        if (geometryRaw == null || resolutionRaw == null || kRaw == null) {
            Seq.empty
        } else {
            val geometryVal = geom.dataType match {
                case StringType => JTS.fromWKT(geometryRaw.asInstanceOf[UTF8String].toString)
                case BinaryType => JTS.fromWKB(geometryRaw.asInstanceOf[Array[Byte]])
            }
            val resolutionVal = resolution.dataType match {
                case StringType  => BNG.resolutionMap(resolutionRaw.asInstanceOf[UTF8String].toString)
                case IntegerType => resolutionRaw.asInstanceOf[Int]
            }
            val kVal = kRaw.asInstanceOf[Int]

            val kLoop = BNG.geometryKLoop(geometryVal, resolutionVal, kVal)

            kLoop.map(c => InternalRow.fromSeq(Seq(UTF8String.fromString(BNG.format(c)))))
        }
    }

    override def elementSchema: StructType = StructType(Seq(StructField("cellId", StringType)))

    override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
        copy(newChildren(0), newChildren(1), newChildren(2))

}

object BNG_GeometryKLoopExplode {

    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[BNG_GeometryKLoopExplode].getCanonicalName,
          db.orNull,
          "grid_cellkloopexplode",
          """
            |    _FUNC_(cell_id, resolution)) - Generates the geometry based k loop (hollow ring) cell IDs set for the input
            |    geometry and the input k value.
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
