package com.databricks.mosaic.expressions.index

import com.databricks.mosaic.core.Mosaic
import com.databricks.mosaic.core.geometry.api.GeometryAPI
import com.databricks.mosaic.core.index.IndexSystemID
import com.databricks.mosaic.core.types.{ChipType, HexType, InternalGeometryType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, ExpressionDescription, UnaryExpression}
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.Geometry

import scala.collection.TraversableOnce


@ExpressionDescription(
  usage = "_FUNC_(struct(geometry, resolution)) - Generates the h3 mosaic chips for the input geometry" +
    "at a given resolution. Geometry and resolution are provided via struct wrapper to ensure UnaryExpression" +
    "API is respected.",
  examples =
    """
    Examples:
      > SELECT _FUNC_(a, b);
        {index_id, is_border, chip_geom}
        {index_id, is_border, chip_geom}
        ...
        {index_id, is_border, chip_geom}
  """,
  since = "1.0")
case class MosaicExplode(pair: Expression, indexSystemName: String, geometryAPIName: String)
  extends UnaryExpression with CollectionGenerator with Serializable with CodegenFallback {

  override val inline: Boolean = false

  /**
   * [[MosaicExplode]] expression can only be called on supported data types.
   * The supported data types are [[BinaryType]] for WKB encoding, [[StringType]]
   * for WKT encoding, [[HexType]] ([[StringType]] wrapper) for HEX encoding
   * and [[InternalGeometryType]] for primitive types encoding via [[ArrayType]].
   *
   * @return An instance of [[TypeCheckResult]] indicating success or a failure.
   */
  override def checkInputDataTypes(): TypeCheckResult = {
    val fields = child.dataType.asInstanceOf[StructType].fields
    val geomType = fields.head
    val resolutionType = fields(1)

    (geomType.dataType, resolutionType.dataType) match {
      case (BinaryType, IntegerType) => TypeCheckResult.TypeCheckSuccess
      case (StringType, IntegerType) => TypeCheckResult.TypeCheckSuccess
      case (HexType, IntegerType) => TypeCheckResult.TypeCheckSuccess
      case (InternalGeometryType, IntegerType) => TypeCheckResult.TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(
          s"Input to h3 mosaic explode should be (geometry, resolution) pair. " +
            s"Geometry type can be WKB, WKT, Hex or Coords. Provided type was: ${child.dataType.catalogString}"
        )
    }
  }

  /**
   * [[MosaicExplode]] is a generator expression. All generator
   * expressions require the element schema to be provided.
   * Chip type is fixed for [[MosaicExplode]], all border chip
   * geometries will be generated as [[BinaryType]] columns
   * encoded as WKBs.
   *
   * @see [[CollectionGenerator]] for the API of generator expressions.
   *      [[ChipType]] for output type definition.
   * @return The schema of the child element. Has to be provided as
   *         a [[StructType]].
   */
  override def elementSchema: StructType = {
    val fields = child.dataType.asInstanceOf[StructType].fields
    val geomType = fields.head
    val resolutionType = fields(1)

    (geomType.dataType, resolutionType.dataType) match {
      case (BinaryType, IntegerType) => ChipType.asInstanceOf[StructType]
      case (StringType, IntegerType) => ChipType.asInstanceOf[StructType]
      case (HexType, IntegerType) => ChipType.asInstanceOf[StructType]
      case (InternalGeometryType, IntegerType) => ChipType.asInstanceOf[StructType]
      case _ => throw new Error(
        s"Input to h3 mosaic explode should be (geometry, resolution) pair. " +
          s"Geometry type can be WKB, WKT, Hex or Coords. Provided type was: ${child.dataType.catalogString}"
      )
    }
  }

  /**
   * Type-wise differences in evaluation are only present on the input
   * data conversion to a [[Geometry]]. The rest of the evaluation
   * is agnostic to the input data type. The evaluation generates
   * a set of core indices that are fully contained by the input
   * [[Geometry]] and a set of border indices that are partially
   * contained by the input [[Geometry]].
   *
   * @param input Struct containing a geometry and a resolution.
   * @return A set of serialized [[com.databricks.mosaic.core.types.model.MosaicChip]].
   *         This set will be used to generate new rows of data.
   */
  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    val inputData = child.eval(input).asInstanceOf[InternalRow]
    val geomType = child.dataType.asInstanceOf[StructType].fields.head.dataType

    val indexSystem = IndexSystemID.getIndexSystem(IndexSystemID(indexSystemName))
    val geometryAPI = GeometryAPI(geometryAPIName)
    val geometry = geometryAPI.geometry(inputData, geomType)

    val resolution = inputData.getInt(1)

    val chips = Mosaic.mosaicFill(geometry, resolution, indexSystem, geometryAPI)
    chips.map(_.serialize)
  }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val arg1 = newArgs.head.asInstanceOf[Expression]
    val res = MosaicExplode(arg1, indexSystemName, geometryAPIName)
    res.copyTagsFrom(this)
    res
  }

  override def collectionType: DataType = child.dataType

  override def child: Expression = pair

  override def position: Boolean = false
}

