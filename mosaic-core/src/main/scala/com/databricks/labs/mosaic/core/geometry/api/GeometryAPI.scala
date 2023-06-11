package com.databricks.labs.mosaic.core.geometry.api

import com.databricks.labs.mosaic.core.codegen.format.GeometryIOCodeGen
import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * An abstract class that defines the API for the geometry frameworks.
 * In order to integrate a new geometry framework, this class must be extended.
 * The fully qualified name of the class must be added to the META-INF/services/com.databricks.labs.mosaic.core.geometry.api.GeometryAPI file.
 * This is where [[com.databricks.labs.mosaic.core.GenericServiceFactory.GeometryAPIFactory]] will look for the available geometry frameworks.
 *
 * @param reader An instance of [[GeometryReader]].
 */
abstract class GeometryAPI(
                            reader: GeometryReader
                          ) extends Serializable {

  def name: String

  /**
   * Constructs an instance of [[MosaicPoint]] based on a collection of
   * [[Coordinates]].
   *
   * @param points   An instance of [[Coordinates]].
   * @param geomType The geometry type.
   * @return An instance of [[MosaicPoint]].
   */
  def pointsToGeometry(points: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value): MosaicGeometry = reader.fromSeq(points, geomType)

  /**
   * Constructs an instance of [[MosaicGeometry]] based on an instance of
   * spark internal data.
   *
   * @param inputData
   * An instance of [[InternalRow]].
   * @param dataType
   * A data type of the geometry.
   * @return
   * An instance of [[MosaicGeometry]].
   */
  def rowToGeometry(inputData: InternalRow, dataType: DataType): MosaicGeometry = {
    dataType match {
      case _: BinaryType => reader.fromWKB(inputData.getBinary(0))
      case _: StringType => reader.fromWKT(inputData.getString(0))
      case _: HexType => reader.fromHEX(inputData.get(0, HexType).asInstanceOf[InternalRow].getString(0))
      case _: GeoJSONType => reader.fromJSON(inputData.get(0, GeoJSONType).asInstanceOf[InternalRow].getString(0))
      case _ => throw new Error(s"$dataType not supported.")
    }
  }

  /**
   * Constructs an instance of [[MosaicGeometry]] based on Any instance
   * coming from spark nullSafeEval method.
   *
   * @param inputData
   * An instance of [[InternalRow]].
   * @param dataType
   * A data type of the geometry.
   * @return
   * An instance of [[MosaicGeometry]].
   */
  def valueToGeometry(inputData: Any, dataType: DataType): MosaicGeometry = {
    dataType match {
      case _: BinaryType => reader.fromWKB(inputData.asInstanceOf[Array[Byte]])
      case _: StringType => reader.fromWKT(inputData.asInstanceOf[UTF8String].toString)
      case _: HexType => reader.fromHEX(inputData.asInstanceOf[InternalRow].getString(0))
      case _: GeoJSONType => reader.fromJSON(inputData.asInstanceOf[InternalRow].getString(0))
      case _ => throw new Error(s"$dataType not supported.")
    }
  }

  /**
   * Serializes an instance of [[MosaicGeometry]] to a spark internal data.
   * The format is selected based on the data type.
   *
   * @param geometry An instance of [[MosaicGeometry]].
   * @param dataType A data type representing the format.
   * @return A spark internal data.
   */
  def serialize(geometry: MosaicGeometry, dataType: DataType): Any = {
    dataType match {
      case _: BinaryType => geometry.toWKB
      case _: StringType => UTF8String.fromString(geometry.toWKT)
      case _: HexType => InternalRow.fromSeq(Seq(UTF8String.fromString(geometry.toHEX)))
      case _: GeoJSONType => InternalRow.fromSeq(Seq(UTF8String.fromString(geometry.toJSON)))
      case _ => throw new Error(s"$dataType not supported.")
    }
  }

  /**
   * Constructs an instance of [[MosaicGeometry]] based on a collection of [[Coordinates]].
   *
   * @param point An instance of [[Coordinates]].
   * @return An instance of [[MosaicGeometry]].
   */
  def fromGeoCoord(point: Coordinates): MosaicPoint

  /**
   * Constructs an instance of [[MosaicGeometry]] based on a collection of [[Double]].
   *
   * @param coords A collection of [[Double]].
   * @return An instance of [[MosaicGeometry]].
   */
  def fromCoords(coords: Seq[Double]): MosaicPoint

  /**
   * Accessor for the [[GeometryIOCodeGen]].
   *
   * @return An instance of [[GeometryIOCodeGen]].
   */
  def ioCodeGen: GeometryIOCodeGen

  /**
   * Generates a try catch block around the code if required by the geometry framework.
   * Not all geometry frameworks require this, so it is up to the implementation to decide.
   *
   * @param code The code to wrap.
   * @return The wrapped code.
   */
  def codeGenTryWrap(code: String): String

  /**
   * The fully qualified class name of the geometry.
   *
   * @return The class name.
   */
  def geometryClass: String

  /**
   * The fully qualified class name of the mosaic geometry.
   *
   * @return The class name.
   */
  def mosaicGeometryClass: String

}
