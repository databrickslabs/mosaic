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
import org.locationtech.jts.geom.Geometry

case class BNG_TessellateExplode(
    geom: Expression,
    resolution: Expression,
    keepCoreGeom: Expression
) extends CollectionGenerator
      with Serializable
      with CodegenFallback {

    override def position: Boolean = false
    override def inline: Boolean = false
    override def children: Seq[Expression] = Seq(geom, resolution, keepCoreGeom)
    override def elementSchema: StructType = StructType(Seq(StructField("cellId", StringType)))
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2))

    /**
      * Type-wise differences in evaluation are only present on the input data
      * conversion to a [[Geometry]]. The rest of the evaluation is agnostic to
      * the input data type. The evaluation generates a set of core indices that
      * are fully contained by the input [[Geometry]] and a set of border
      * indices that are partially contained by the input [[Geometry]].
      *
      * @param input
      *   Struct containing a geometry and a resolution.
      * @return
      *   A set of serialized chips. This set will be used to generate new rows
      *   of data.
      */
    override def eval(input: InternalRow): IterableOnce[InternalRow] = {
        val geomRaw = geom.eval(input)
        val resolutionRaw = resolution.eval(input)
        val keepGeomRaw = keepCoreGeom.eval(input)
        if (geomRaw == null || resolutionRaw == null || keepGeomRaw == null) {
            return Seq.empty
        }
        val geometryVal = geom.dataType match {
            case StringType => JTS.fromWKT(geomRaw.asInstanceOf[UTF8String].toString)
            case BinaryType => JTS.fromWKB(geomRaw.asInstanceOf[Array[Byte]])
        }
        val resolutionVal = resolution.dataType match {
            case StringType  => BNG.resolutionMap(resolutionRaw.asInstanceOf[UTF8String].toString)
            case IntegerType => resolutionRaw.asInstanceOf[Int]
        }
        val keepCoreGeomVal = keepGeomRaw.asInstanceOf[Boolean]

        BNG
            .tessellate(geometryVal, resolutionVal, keepCoreGeomVal)
            .map(c => {
                val cellId = UTF8String.fromString(BNG.format(c._1))
                val g = geom.dataType match {
                    case StringType if keepCoreGeomVal => UTF8String.fromString(JTS.toWKT(c._3))
                    case BinaryType if keepCoreGeomVal => JTS.toWKB(c._3)
                    case _                             => null
                }
                InternalRow.fromSeq(Seq(cellId, c._2, g))
            })

    }

}

object BNG_TessellateExplode extends WithExpressionInfo {

    override def name: String = "gbx_bng_tessellateexplode"
    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_TessellateExplode(c(0), c(1), c(2))

    //TODO: ADD EXPRESSION INFO
    override def usageArgs: String = ""

    override def description: String = ""

    override def extendedUsageArgs: String = ""

    override def examples: String = ""

}
