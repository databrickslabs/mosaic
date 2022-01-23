package com.databricks.mosaic.core.geometry.api

import java.util.Locale

import com.uber.h3core.util.GeoCoord

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.databricks.mosaic.core.geometry._
import com.databricks.mosaic.core.geometry.point._
import com.databricks.mosaic.core.types._
import com.databricks.mosaic.core.types.model.{GeometryTypeEnum, InternalGeometry}

abstract class GeometryAPI(
    reader: GeometryReader
) extends Serializable {

    def name: String

    def geometry(input: Any, typeName: String): MosaicGeometry = {
        typeName match {
            case "WKT" => reader.fromWKT(input.asInstanceOf[String])
            case "HEX" => reader.fromHEX(input.asInstanceOf[String])
            case "WKB" => reader.fromWKB(input.asInstanceOf[Array[Byte]])
            case "GEOJSON" => reader.fromJSON(input.asInstanceOf[String])
            case _ => throw new UnsupportedOperationException
        }
    }

    def geometry(points: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value): MosaicGeometry = reader.fromPoints(points, geomType)

    /**
      * Constructs an instance of [[MosaicGeometry]] based on an instance of
      * spark internal data.
      * @param inputData
      *   An instance of [[InternalRow]].
      * @param dataType
      *   A data type of the geometry.
      * @return
      *   An instance of [[MosaicGeometry]].
      */
    def geometry(inputData: InternalRow, dataType: DataType): MosaicGeometry = {
        dataType match {
            case _: BinaryType           => reader.fromWKB(inputData.getBinary(0))
            case _: StringType           => reader.fromWKT(inputData.getString(0))
            case _: HexType              => reader.fromHEX(inputData.get(0, HexType).asInstanceOf[InternalRow].getString(0))
            case _: JSONType             => reader.fromJSON(inputData.get(0, JSONType).asInstanceOf[InternalRow].getString(0))
            case _: InternalGeometryType => reader.fromInternal(inputData.get(0, InternalGeometryType).asInstanceOf[InternalRow])
        }
    }

    /**
      * Constructs an instance of [[MosaicGeometry]] based on Any instance
      * coming from spark nullSafeEval method.
      * @param inputData
      *   An instance of [[InternalRow]].
      * @param dataType
      *   A data type of the geometry.
      * @return
      *   An instance of [[MosaicGeometry]].
      */
    def geometry(inputData: Any, dataType: DataType): MosaicGeometry =
        dataType match {
            case _: BinaryType           => reader.fromWKB(inputData.asInstanceOf[Array[Byte]])
            case _: StringType           => reader.fromWKT(inputData.asInstanceOf[UTF8String].toString)
            case _: HexType              => reader.fromHEX(inputData.asInstanceOf[InternalRow].getString(0))
            case _: JSONType             => reader.fromJSON(inputData.asInstanceOf[InternalRow].getString(0))
            case _: InternalGeometryType => reader.fromInternal(inputData.asInstanceOf[InternalRow])
            case _: KryoType             => reader.fromKryo(inputData.asInstanceOf[InternalRow])
        }

    def serialize(geometry: MosaicGeometry, dataType: DataType): Any = {
        dataType match {
            case _: BinaryType           => geometry.toWKB
            case _: StringType           => UTF8String.fromString(geometry.toWKT)
            case _: HexType              => InternalRow.fromSeq(Seq(UTF8String.fromString(geometry.toHEX)))
            case _: JSONType             => InternalRow.fromSeq(Seq(UTF8String.fromString(geometry.toJSON)))
            case _: InternalGeometryType => geometry.toInternal.serialize
            case _: KryoType => InternalRow.fromSeq(Seq(GeometryTypeEnum.fromString(geometry.getGeometryType).id, geometry.toKryo))
            case _           => throw new Error(s"$dataType not supported.")
        }
    }

    def serialize(geometry: MosaicGeometry, dataTypeName: String): Any = {
        dataTypeName.toUpperCase(Locale.ROOT) match {
            case "WKB"     => geometry.toWKB
            case "WKT"     => UTF8String.fromString(geometry.toWKT)
            case "HEX"     => InternalRow.fromSeq(Seq(UTF8String.fromString(geometry.toHEX)))
            case "GEOJSON" => InternalRow.fromSeq(Seq(UTF8String.fromString(geometry.toJSON)))
            case "COORDS"  => geometry.toInternal.serialize
            case "KRYO"    => InternalRow.fromSeq(Seq(GeometryTypeEnum.fromString(geometry.getGeometryType).id, geometry.toKryo))
            case _         => throw new Error(s"$dataTypeName not supported.")
        }
    }

    def fromGeoCoord(point: GeoCoord): MosaicPoint

}

object GeometryAPI extends Serializable {

    def apply(name: String): GeometryAPI =
        name match {
            case "JTS" => JTS
            case "OGC" => OGC
        }

    def getReader(name: String): GeometryReader =
        name match {
            case "JTS" => MosaicGeometryJTS
            case "OGC" => MosaicGeometryOGC
        }

    object OGC extends GeometryAPI(MosaicGeometryOGC) {

        override def name: String = "OGC"

        override def fromGeoCoord(point: GeoCoord): MosaicPoint = MosaicPointOGC(point)

    }

    object JTS extends GeometryAPI(MosaicGeometryJTS) {

        override def name: String = "JTS"

        override def fromGeoCoord(geoCoord: GeoCoord): MosaicPoint = MosaicPointJTS(geoCoord)

    }

}
