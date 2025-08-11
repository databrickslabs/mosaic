package com.dblabs.spatial.gridx.bng

import com.databricks.labs.mosaic.core.types._
import com.dblabs.spatial.gridx.grid.BNG
import com.dblabs.spatial.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, ExpressionInfo}
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

    override def checkInputDataTypes(): TypeCheckResult = {
        if (!Seq(BinaryType, StringType).contains(geom.dataType)) {
            TypeCheckResult.TypeCheckFailure("Unsupported geom type.")
        } else if (!Seq(IntegerType, StringType).contains(resolution.dataType)) {
            TypeCheckResult.TypeCheckFailure("Unsupported resolution type.")
        } else if (keepCoreGeom.dataType != BooleanType) {
            TypeCheckResult.TypeCheckFailure("Unsupported flag type type.")
        } else {
            TypeCheckResult.TypeCheckSuccess
        }
    }

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
      *   A set of serialized
      *   [[com.databricks.labs.mosaic.core.types.model.MosaicChip]]. This set
      *   will be used to generate new rows of data.
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

        BNG.tessellate(geometryVal, resolutionVal, keepCoreGeomVal)
            .map(c => InternalRow.fromSeq(Seq((c._1, BNG.format(c._2), if (keepCoreGeomVal) c._3 else null))))

    }

    override def elementSchema: StructType = {
        StructType(Array(StructField("index", ChipType(StringType))))
    }

    override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
        copy(newChildren(0), newChildren(1), newChildren(2))

}

object BNG_TessellateExplode {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[BNG_TessellateExplode].getCanonicalName,
          db.orNull,
          "grid_tessellateexplode",
          """
            |    _FUNC_(struct(geometry, resolution, keepCoreGeom)) - Generates the mosaic chips for the input
            |    geometry at a given resolution. Geometry and resolution are provided via struct wrapper to ensure
            |    UnaryExpression API is respected.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a, b, c);
            |        {index_id, is_border, chip_geom}
            |        {index_id, is_border, chip_geom}
            |        ...
            |        {index_id, is_border, chip_geom}
            |  """.stripMargin,
          "",
          "generator_funcs",
          "1.0",
          "",
          "built-in"
        )

}
