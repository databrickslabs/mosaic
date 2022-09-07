package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.Mosaic
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemID}
import com.databricks.labs.mosaic.core.types._
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.{LINESTRING, MULTILINESTRING}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, ExpressionInfo, Literal}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.Geometry

import scala.collection.TraversableOnce

case class MosaicExplode(
    geom: Expression,
    resolution: Expression,
    keepCoreGeom: Expression,
    idAsLong: Expression,
    indexSystemName: String,
    geometryAPIName: String
) extends CollectionGenerator
      with Serializable
      with CodegenFallback {

    lazy val indexSystem: IndexSystem = IndexSystemID.getIndexSystem(IndexSystemID(indexSystemName))

    lazy val geometryAPI: GeometryAPI = GeometryAPI(geometryAPIName)

    override def position: Boolean = false

    override def inline: Boolean = false

    override def children: Seq[Expression] = Seq(geom, resolution, keepCoreGeom, idAsLong)

    /**
      * [[MosaicExplode]] expression can only be called on supported data types.
      * The supported data types are [[BinaryType]] for WKB encoding,
      * [[StringType]] for WKT encoding, [[HexType]] ([[StringType]] wrapper)
      * for HEX encoding and [[InternalGeometryType]] for primitive types
      * encoding via [[ArrayType]].
      *
      * @return
      *   An instance of [[TypeCheckResult]] indicating success or a failure.
      */
    override def checkInputDataTypes(): TypeCheckResult = {
        if (!Seq(BinaryType, StringType, HexType, InternalGeometryType).contains(geom.dataType)) {
            TypeCheckResult.TypeCheckFailure("Unsupported geom type.")
        } else if (!Seq(IntegerType, StringType).contains(resolution.dataType)) {
            TypeCheckResult.TypeCheckFailure("Unsupported resolution type.")
        } else if (!Seq(BooleanType).contains(keepCoreGeom.dataType)) {
            TypeCheckResult.TypeCheckFailure("Unsupported boolean flag.")
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
    override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
        val geomRaw = geom.eval(input)
        val resolutionVal = indexSystem.getResolution(resolution.eval(input))
        val geometryVal = geometryAPI.geometry(geomRaw, geom.dataType)
        val keepCoreGeomVal = keepCoreGeom.eval(input).asInstanceOf[Boolean]
        val idAsLongVal = idAsLong.asInstanceOf[Literal].value.asInstanceOf[Boolean]

        val chips = GeometryTypeEnum.fromString(geometryVal.getGeometryType) match {
            case LINESTRING      => Mosaic.lineFill(geometryVal, resolutionVal, indexSystem, geometryAPI)
            case MULTILINESTRING => Mosaic.lineFill(geometryVal, resolutionVal, indexSystem, geometryAPI)
            case _               => Mosaic.mosaicFill(geometryVal, resolutionVal, keepCoreGeomVal, indexSystem, geometryAPI)
        }

        val formatted = if (!idAsLongVal) chips.map(_.toStringID(indexSystem)) else chips
        formatted.map(row => InternalRow.fromSeq(Seq(row.serialize)))
    }

    override def elementSchema: StructType = {
        val chipType = if (idAsLong.asInstanceOf[Literal].value.asInstanceOf[Boolean]) ChipType(LongType) else ChipType(StringType)
        StructType(Array(StructField("index", chipType)))
    }

    override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
        copy(newChildren(0), newChildren(1), newChildren(2), newChildren(3))

}

object MosaicExplode {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[IndexGeometry].getCanonicalName,
          db.orNull,
          "grid_tassellateexplode",
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
