package com.databricks.labs.mosaic.core.geometry.api

import com.databricks.labs.mosaic.codegen.format._
import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.point._
import com.databricks.labs.mosaic.core.types._
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum
import com.esri.core.geometry.ogc.OGCGeometry
import com.uber.h3core.util.GeoCoord
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.{Geometry => JTSGeometry}

import java.util.Locale

abstract class GeometryAPI(
    reader: GeometryReader
) extends Serializable {

    def name: String

    def geometry(input: Any, typeName: String): MosaicGeometry = {
        typeName match {
            case "WKT"     => reader.fromWKT(input.asInstanceOf[String])
            case "HEX"     => reader.fromHEX(input.asInstanceOf[String])
            case "WKB"     => reader.fromWKB(input.asInstanceOf[Array[Byte]])
            case "GEOJSON" => reader.fromJSON(input.asInstanceOf[String])
            case "COORDS"  => throw new Error(s"$typeName not supported.")
            case _         => throw new Error(s"$typeName not supported.")
        }
    }

    def geometry(points: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value): MosaicGeometry = reader.fromSeq(points, geomType)

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
            case _                       => throw new Error(s"$dataType not supported.")
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
            case _                       => throw new Error(s"$dataType not supported.")
        }

    def serialize(geometry: MosaicGeometry, dataType: DataType): Any = {
        serialize(geometry, GeometryFormat.getDefaultFormat(dataType))
    }

    def serialize(geometry: MosaicGeometry, dataFormatName: String): Any = {
        dataFormatName.toUpperCase(Locale.ROOT) match {
            case "WKB"        => geometry.toWKB
            case "WKT"        => UTF8String.fromString(geometry.toWKT)
            case "HEX"        => InternalRow.fromSeq(Seq(UTF8String.fromString(geometry.toHEX)))
            case "JSONOBJECT" => InternalRow.fromSeq(Seq(UTF8String.fromString(geometry.toJSON)))
            case "GEOJSON"    => UTF8String.fromString(geometry.toJSON)
            case "COORDS"     => geometry.toInternal.serialize
            case _         => throw new Error(s"$dataFormatName not supported.")
        }
    }

    def fromGeoCoord(point: GeoCoord): MosaicPoint = throw new Error("Unimplemented")

    def fromCoords(coords: Seq[Double]): MosaicPoint = throw new Error("Unimplemented")

    def ioCodeGen: GeometryIOCodeGen = throw new Error("Unimplemented")

    def codeGenTryWrap(code: String): String = throw new Error("Unimplemented")

    def geometryClass: String = throw new Error("Unimplemented")

    def mosaicGeometryClass: String = throw new Error("Unimplemented")

    def geometryAreaCode: String = throw new Error("Unimplemented")

    def geometryTypeCode: String = throw new Error("Unimplemented")

    def geometryIsValidCode: String = throw new Error("Unimplemented")

    def geometryLengthCode: String = throw new Error("Unimplemented")

    def geometrySRIDCode(geomInRef: String): String = throw new Error("Unimplemented")
}

object GeometryAPI extends Serializable {

    def apply(name: String): GeometryAPI =
        name match {
            case "JTS"  => JTS
            case "ESRI" => ESRI
            case _      => IllegalAPI
        }

    object ESRI extends GeometryAPI(MosaicGeometryESRI) {

        override def name: String = "ESRI"

        override def fromGeoCoord(point: GeoCoord): MosaicPoint = MosaicPointESRI(point)

        override def fromCoords(coords: Seq[Double]): MosaicPoint = MosaicPointESRI(coords)

        override def ioCodeGen: GeometryIOCodeGen = MosaicGeometryIOCodeGenESRI

        override def codeGenTryWrap(code: String): String = code

        override def geometryClass: String = classOf[OGCGeometry].getName

        override def mosaicGeometryClass: String = classOf[MosaicGeometryESRI].getName

        override def geometryAreaCode: String = "getEsriGeometry().calculateArea2D()"

        override def geometryTypeCode: String = "geometryType()"

        override def geometryIsValidCode: String = "isSimple()"

        override def geometryLengthCode: String = "getEsriGeometry().calculateLength2D()"

        override def geometrySRIDCode(geomInRef: String): String = s"($geomInRef.esriSR == null) ? 0 : $geomInRef.getEsriSpatialReference().getID()"

    }

    object JTS extends GeometryAPI(MosaicGeometryJTS) {

        override def name: String = "JTS"

        override def fromGeoCoord(geoCoord: GeoCoord): MosaicPoint = MosaicPointJTS(geoCoord)

        override def fromCoords(coords: Seq[Double]): MosaicPoint = MosaicPointJTS(coords)

        override def ioCodeGen: GeometryIOCodeGen = MosaicGeometryIOCodeGenJTS

        override def codeGenTryWrap(code: String): String =
            s"""
               |try {
               |$code
               |} catch (Exception e) {
               | throw e;
               |}
               |""".stripMargin

        override def geometryClass: String = classOf[JTSGeometry].getName

        override def mosaicGeometryClass: String = classOf[MosaicGeometryJTS].getName

        override def geometryAreaCode: String = "getArea()"

        override def geometryTypeCode: String = "getGeometryType()"

        override def geometryIsValidCode: String = "isValid()"

        override def geometryLengthCode: String = "getLength()"

        override def geometrySRIDCode(geomInRef: String): String = s"$geomInRef.getSRID()"
    }

    object IllegalAPI extends GeometryAPI(null) {
        override def name: String = "ILLEGAL"
    }

}
