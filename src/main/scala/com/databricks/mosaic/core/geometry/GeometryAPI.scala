package com.databricks.mosaic.core.geometry

import com.databricks.mosaic.core.types
import com.databricks.mosaic.core.types.{HexType, InternalGeometryType, JSONType}
import com.uber.h3core.util.GeoCoord
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.{Geometry => GeometryJTS}

sealed trait GeometryAPI {
  def name: String
  def geometry(points: Seq[MosaicPoint]): MosaicGeometry
  def geometry(geom: Any): MosaicGeometry
  def geometry(inputData: InternalRow, dataType: DataType): MosaicGeometry
  def geometry(inputData: Any, dataType: DataType): MosaicGeometry
  def fromGeoGoord(point: GeoCoord): MosaicPoint
}

object GeometryAPI {

  def apply(name: String): GeometryAPI = name match {
    case "JTS" => JTS
    case "OGC" => OGC
  }

  case object JTS extends GeometryAPI {

    override def name: String = "JTS"

    override def geometry(geom: Any): MosaicGeometry = MosaicGeometryJTS(geom.asInstanceOf[GeometryJTS])

    override def geometry(inputData: InternalRow, dataType: DataType): MosaicGeometry = {
      val geom = types.struct2geom(inputData, dataType)
      MosaicGeometryJTS(geom)
    }

    override def geometry(inputData: Any, dataType: DataType): MosaicGeometry = {
      val geom = types.any2geometry(inputData, dataType)
      MosaicGeometryJTS(geom)
    }

    override def geometry(points: Seq[MosaicPoint]): MosaicGeometry = MosaicGeometryJTS.fromPoints(points)

    override def fromGeoGoord(geoCoord: GeoCoord): MosaicPoint = MosaicPointJTS(geoCoord)
  }

  case object OGC extends GeometryAPI {

    override def name: String = "OGC"

    override def geometry(geom: Any): MosaicGeometry = ???

    override def geometry(inputData: InternalRow, dataType: DataType): MosaicGeometry = {
      dataType match {
          case _: BinaryType => MosaicGeometryOGC.fromWKB(inputData.getBinary(0))
          case _: StringType => MosaicGeometryOGC.fromWKT(inputData.getString(0))
          case _: HexType => MosaicGeometryOGC.fromHEX(inputData.get(0, HexType).asInstanceOf[InternalRow].getString(0))
          case _: JSONType => MosaicGeometryOGC.fromJSON(inputData.get(0, JSONType).asInstanceOf[InternalRow].getString(0))
          case _: InternalGeometryType => ??? // Implementation missing
        }
    }

    override def geometry(inputData: Any, dataType: DataType): MosaicGeometry =
      dataType match {
        case _: BinaryType => MosaicGeometryOGC.fromWKB(inputData.asInstanceOf[Array[Byte]])
        case _: StringType => MosaicGeometryOGC.fromWKT(inputData.asInstanceOf[UTF8String].toString)
        case _: HexType => MosaicGeometryOGC.fromHEX(inputData.asInstanceOf[InternalRow].getString(0))
        case _: JSONType => MosaicGeometryOGC.fromJSON(inputData.asInstanceOf[InternalRow].getString(0))
        case _: InternalGeometryType => ??? // Implementation missing
      }

    override def geometry(points: Seq[MosaicPoint]): MosaicGeometry = MosaicGeometryOGC.fromPoints(points)

    override def fromGeoGoord(point: GeoCoord): MosaicPoint = MosaicPointOGC(point)
  }

}