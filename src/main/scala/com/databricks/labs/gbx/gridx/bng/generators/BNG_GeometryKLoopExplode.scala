package com.databricks.labs.gbx.gridx.bng.generators

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression}
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

    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2))

}

object BNG_GeometryKLoopExplode extends WithExpressionInfo {

    override def name: String = "gbx_bng_geometrykloopexplode"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_GeometryKLoopExplode(c(0), c(1), c(2))

    //TODO: ADD EXPRESSION INFO
    override def usageArgs: String = ""

    override def description: String = ""

    override def extendedUsageArgs: String = ""

    override def examples: String = ""

}
