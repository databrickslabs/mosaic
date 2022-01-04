package com.databricks.mosaic.core.geometry.api

import com.databricks.mosaic.core.geometry.point.{MosaicPoint, MosaicPointJTS, MosaicPointOGC}
import com.databricks.mosaic.core.geometry.{GeometryReader, MosaicGeometry, MosaicGeometryJTS, MosaicGeometryOGC}
import com.databricks.mosaic.core.types.{HexType, InternalGeometryType, JSONType}
import com.uber.h3core.util.GeoCoord
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

abstract class GeometryAPI(
  reader: GeometryReader
) {

  def name: String
  def geometry(points: Seq[MosaicPoint]): MosaicGeometry = reader.fromPoints(points)

  /**
   * Constructs an instance of [[MosaicGeometry]] based on an instance
   * of spark internal data.
   * @param inputData An instance of [[InternalRow]].
   * @param dataType A data type of the geometry.
   * @return An instance of [[MosaicGeometry]].
   */
  def geometry(inputData: InternalRow, dataType: DataType): MosaicGeometry = {
    dataType match {
      case _: BinaryType => reader.fromWKB(inputData.getBinary(0))
      case _: StringType => reader.fromWKT(inputData.getString(0))
      case _: HexType => reader.fromHEX(inputData.get(0, HexType).asInstanceOf[InternalRow].getString(0))
      case _: JSONType => reader.fromJSON(inputData.get(0, JSONType).asInstanceOf[InternalRow].getString(0))
      case _: InternalGeometryType => reader.fromInternal(inputData.get(0, InternalGeometryType).asInstanceOf[InternalRow])
    }
  }

  /**
   * Constructs an instance of [[MosaicGeometry]] based on Any instance
   * coming from spark nullSafeEval method.
   * @param inputData An instance of [[InternalRow]].
   * @param dataType A data type of the geometry.
   * @return An instance of [[MosaicGeometry]].
   */
  def geometry(inputData: Any, dataType: DataType): MosaicGeometry =
    dataType match {
      case _: BinaryType => reader.fromWKB(inputData.asInstanceOf[Array[Byte]])
      case _: StringType => reader.fromWKT(inputData.asInstanceOf[UTF8String].toString)
      case _: HexType => reader.fromHEX(inputData.asInstanceOf[InternalRow].getString(0))
      case _: JSONType => reader.fromJSON(inputData.asInstanceOf[InternalRow].getString(0))
      case _: InternalGeometryType => reader.fromInternal(inputData.asInstanceOf[InternalRow])
    }

  def fromGeoCoord(point: GeoCoord): MosaicPoint

}

object GeometryAPI {

  object OGC extends GeometryAPI(MosaicGeometryOGC) {

    override def name: String = "OGC"

    override def fromGeoCoord(point: GeoCoord): MosaicPoint = MosaicPointOGC(point)
  }

  object JTS extends GeometryAPI(MosaicGeometryJTS) {

    override def name: String = "JTS"

    override def fromGeoCoord(geoCoord: GeoCoord): MosaicPoint = MosaicPointJTS(geoCoord)

  }

  def apply(name: String): GeometryAPI = name match {
    case "JTS" => JTS
    case "OGC" => OGC
  }

  def getReader(name: String): GeometryReader = name match {
    case "JTS" => MosaicGeometryJTS
    case "OGC" => MosaicGeometryOGC
  }

}