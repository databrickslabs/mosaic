package com.databricks.mosaic.expressions.geometry

import com.databricks.mosaic.expressions.format.Conversions
import com.databricks.mosaic.types.{HexType, InternalGeometryType}
import com.databricks.mosaic.types.model.InternalGeometry
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, ExpressionDescription, UnaryExpression}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.{WKBWriter, WKTWriter}

import scala.collection.TraversableOnce

@ExpressionDescription(
  usage = "_FUNC_(geometry) - The geometry instance can contain both Polygons and MultiPolygons." +
    "The flattened representation will only contain Polygons." +
    "MultiPolygon rows will be exploded into Polygon rows.",
  examples =
    """
    Examples:
      > SELECT _FUNC_(a);
        Polygon ((...))
        Polygon ((...))
        ...
        Polygon ((...))
  """,
  since = "1.0")
case class FlattenPolygons(pair: Expression, geometryAPIName: String)
  extends UnaryExpression with CollectionGenerator with CodegenFallback {

  /** Fixed definitions. */
  override val inline: Boolean = false
  override def collectionType: DataType = child.dataType
  override def child: Expression = pair
  override def position: Boolean = false

  /** @see [[FlattenPolygons()]] companion object for implementations. */
  override def checkInputDataTypes(): TypeCheckResult = FlattenPolygons.checkInputDataTypesImpl(child)
  override def elementSchema: StructType = FlattenPolygons.elementSchemaImpl(child)
  override def eval(input: InternalRow): TraversableOnce[InternalRow] = FlattenPolygons.evalImpl(input, child)
  override def makeCopy(newArgs: Array[AnyRef]): Expression = FlattenPolygons.makeCopyImpl(newArgs, geometryAPIName, this)
}

object FlattenPolygons {

  /**
   * Flattens an input into a collection of outputs.
   * Each output instance should be wrapped into an [[InternalRow]] wrapper.
   * For the generator expression [[evalImpl()]] call requires that
   * input is evaluated before the evaluation of this expression can occur.
   * @param input An instance of a row before the child expression has
   *              been evaluated.
   * @return  A collection of [[InternalRow]] instances. This collection
   *          has to implement [[TraversableOnce]] API.
   */
  def evalImpl(input: InternalRow, child: Expression): TraversableOnce[InternalRow] = {
    child.dataType match {
      case _: BinaryType => //WKB case
        val wkb = child.eval(input).asInstanceOf[Array[Byte]]
        val geom = Conversions.wkb2geom(wkb)
        val output = flattenGeom(geom)
        val writer = new WKBWriter()
        output.map(g => InternalRow.fromSeq(Seq(writer.write(g))))
      case _: StringType => //WTK case
        val wkt = child.eval(input).asInstanceOf[UTF8String]
        val geom = Conversions.wkt2geom(wkt)
        val output = flattenGeom(geom)
        val writer = new WKTWriter()
        output.map(g => InternalRow.fromSeq(Seq(UTF8String.fromString(writer.write(g)))))
      case _: HexType => //HEX case
        val hex = child.eval(input).asInstanceOf[InternalRow]
        val geom = Conversions.hex2geom(hex)
        val output = flattenGeom(geom)
        output.map(g => InternalRow.fromSeq(Seq(Conversions.geom2hex(g))))
      case _: InternalGeometryType => //COORDS case
        val coords = InternalGeometry(child.eval(input).asInstanceOf[InternalRow])
        val geom = coords.toGeom
        val output = flattenGeom(geom)
        output.map(g => InternalRow.fromSeq(Seq(InternalGeometry(g).serialize)))
    }
  }

  /**
   * [[FlattenPolygons]] expression can only be called on supported data types.
   * The supported data types are [[BinaryType]] for WKB encoding, [[StringType]]
   * for WKT encoding, [[HexType]] ([[StringType]] wrapper) for HEX encoding
   * and [[InternalGeometryType]] for primitive types encoding via [[ArrayType]].
   * @return An instance of [[TypeCheckResult]] indicating success or a failure.
   */
  def checkInputDataTypesImpl(child: Expression): TypeCheckResult = child.dataType match {
    case _: BinaryType => TypeCheckResult.TypeCheckSuccess
    case _: StringType => TypeCheckResult.TypeCheckSuccess
    case _: HexType => TypeCheckResult.TypeCheckSuccess
    case _: InternalGeometryType => TypeCheckResult.TypeCheckSuccess
    case _ =>
      TypeCheckResult.TypeCheckFailure(
        "input to function explode should be array or map type, " +
          s"not ${child.dataType.catalogString}")
  }

  /**
   * [[FlattenPolygons]] is a generator expression. All generator
   * expressions require the element schema to be provided.
   * Since we are flattening the geometries the element type is the
   * same type of the input data.
   * @see [[CollectionGenerator]] for the API of generator expressions.
   * @return The schema of the child element. Has to be provided as
   *         a [[StructType]].
   */
  def elementSchemaImpl(child: Expression): StructType = child.dataType match {
    case _: BinaryType => StructType(Seq(StructField("element", BinaryType)))
    case _: StringType => StructType(Seq(StructField("element", StringType)))
    case _: HexType => StructType(Seq(StructField("element", HexType)))
    case _: InternalGeometryType => StructType(Seq(StructField("element", InternalGeometryType)))
  }

  /**
   * The actual flattening performed on an instance of [[Geometry]] that
   * is either a Polygon or a MultiPolygon.
   * This method assumes only a single level of nesting.
   * @param geom An instance of [[Geometry]] to be flattened.
   * @return A collection of piece-wise geometries.
   */
  private def flattenGeom(geom: Geometry): Seq[Geometry] = geom.getGeometryType match {
    case "Polygon" => List(geom)
    case "MultiPolygon" => for (
      i <- 0 until geom.getNumGeometries
    ) yield geom.getGeometryN(i)
  }

  def makeCopyImpl(newArgs: Array[AnyRef], geometryAPIName: String, instance: FlattenPolygons): Expression = {
    val asArray = newArgs.take(1).map(_.asInstanceOf[Expression])
    val res = FlattenPolygons(asArray(0), geometryAPIName)
    res.copyTagsFrom(instance)
    res
  }

}

