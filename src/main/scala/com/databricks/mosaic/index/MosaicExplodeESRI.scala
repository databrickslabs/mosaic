package com.databricks.mosaic.index

import com.databricks.mosaic.core.H3IndexSystem.h3
import com.databricks.mosaic.core.IndexSystemID
import com.databricks.mosaic.types.{ChipType, HexType, InternalGeometryType}
import com.esri.core.geometry.ogc.{OGCGeometry, OGCPoint}
import com.esri.core.geometry._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, ExpressionDescription, UnaryExpression}
import org.apache.spark.sql.types._

import java.nio.ByteBuffer
import scala.collection.JavaConverters._
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
case class MosaicExplodeESRI(pair: Expression, indexSystemName: String)
  extends UnaryExpression with CollectionGenerator with Serializable with CodegenFallback {

  override val inline: Boolean = false

  /**
   * [[MosaicExplodeESRI]] expression can only be called on supported data types.
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
   * [[MosaicExplodeESRI]] is a generator expression. All generator
   * expressions require the element schema to be provided.
   * Chip type is fixed for [[MosaicExplodeESRI]], all border chip
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

  private def getOCGGeometry(inputData: InternalRow) = {
    val bytes = inputData.getBinary(0)
    val spatialRef = SpatialReference.create(4326)

    val geometry = OperatorImportFromWkb.local().execute(
      WkbImportFlags.wkbImportDefaults,
      Geometry.Type.Polygon,
      ByteBuffer.wrap(bytes),
      null // no progress tracking
    ).asInstanceOf[Polygon]

    val ocgGeom = OGCGeometry.createFromEsriGeometry(geometry, spatialRef)
    ocgGeom
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
   * @return A set of serialized [[com.databricks.mosaic.types.model.MosaicChip]].
   *         This set will be used to generate new rows of data.
   */
  //noinspection ZeroIndexToHead
  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    val spatialRef = SpatialReference.create(4326)

    val inputData = child.eval(input).asInstanceOf[InternalRow]

    val indexSystem = IndexSystemID.getIndexSystem(IndexSystemID(indexSystemName))

    val ocgGeom = getOCGGeometry(inputData)
    val resolution = inputData.getInt(1)

    val centroid = ocgGeom.centroid().asInstanceOf[OGCPoint]

    val centroidIndex = h3.geoToH3(centroid.Y(), centroid.X(), resolution)

    val boundary = h3.h3ToGeoBoundary(centroidIndex)
      .asScala //.map(cur => new Coordinate(cur.lng, cur.lat)).toList
      .map(cur => new OGCPoint(new Point(cur.lat, cur.lng), spatialRef))

    // Hexagons have only 3 diameters.
    // Computing them manually and selecting the maximum.
    val radius = Seq(
      boundary(0).distance(boundary(3)),
      boundary(1).distance(boundary(4)),
      boundary(2).distance(boundary(5))
    ).max/2

    // do not modify the radius
    val coreGeometry = ocgGeom.buffer(-radius).makeSimple()
    // add 1% to the radius to ensure union of carved and border geometries does not have holes inside the original geometry areas
    val borderGeometry = if(coreGeometry.isEmpty) {
      ocgGeom.buffer(radius*1.01)
    } else {
      ocgGeom.boundary().buffer(radius*1.01)
    }


    val coreIndices = indexSystem.polyfill(coreGeometry, resolution)
    val borderIndices = indexSystem.polyfill(borderGeometry, resolution)

    val coreChips = indexSystem.getCoreChipsESRI(coreIndices)
    val borderChips = indexSystem.getBorderChips(ocgGeom, borderIndices)

    val chips = coreChips ++ borderChips

    chips.map(_.serialize)
  }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val arg1 = newArgs.head.asInstanceOf[Expression]
    val res = MosaicExplodeESRI(arg1, indexSystemName)
    res.copyTagsFrom(this)
    res
  }

  override def collectionType: DataType = child.dataType

  override def child: Expression = pair

  override def position: Boolean = false
}

