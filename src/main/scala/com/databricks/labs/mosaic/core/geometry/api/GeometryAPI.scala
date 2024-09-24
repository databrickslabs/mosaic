package com.databricks.labs.mosaic.core.geometry.api

import com.databricks.labs.mosaic.MOSAIC_GEOMETRY_API
import com.databricks.labs.mosaic.codegen.format._
import com.databricks.labs.mosaic.core.crs.CRSBoundsProvider
import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.point._
import com.databricks.labs.mosaic.core.types._
import com.databricks.labs.mosaic.core.types.model.{Coordinates, GeometryTypeEnum}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.util.Locale

abstract class GeometryAPI(
    reader: GeometryReader
) extends Serializable {

    def createBbox(xMin: Double, yMin: Double, xMax: Double, yMax: Double): MosaicGeometry = {
        val p1 = fromGeoCoord(Coordinates(yMin, xMin))
        val p2 = fromGeoCoord(Coordinates(yMax, xMin))
        val p3 = fromGeoCoord(Coordinates(yMax, xMax))
        val p4 = fromGeoCoord(Coordinates(yMin, xMax))
        geometry(Seq(p1, p2, p3, p4, p1), GeometryTypeEnum.POLYGON)
    }

    def geographicExtent(spatialReferenceID: Int): MosaicGeometry = {
        val bounds = CRSBoundsProvider(this).reprojectedBounds("EPSG", spatialReferenceID)
        createBbox(bounds.lowerLeft.getX, bounds.lowerLeft.getY, bounds.upperRight.getX, bounds.upperRight.getY)
    }


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
            case _            => throw new Error(s"$dataFormatName not supported.")
        }
    }

    def fromGeoCoord(point: Coordinates): MosaicPoint

    def fromCoords(coords: Seq[Double]): MosaicPoint

    def fromSeq(geoms: Seq[MosaicGeometry], geomType: GeometryTypeEnum.Value): MosaicGeometry = {
        reader.fromSeq(geoms, geomType)
    }

    def ioCodeGen: GeometryIOCodeGen

    def codeGenTryWrap(code: String): String

    def geometryClass: String

    def mosaicGeometryClass: String

}

object GeometryAPI extends Serializable {

    def apply(name: String): GeometryAPI =
        name match {
            case "JTS"  => JTS
            case _      => throw new Error(s"Unsupported API name: $name.")
        }

    def apply(sparkSession: SparkSession): GeometryAPI = {
        val apiName = sparkSession.conf.get(MOSAIC_GEOMETRY_API, "JTS")
        apply(apiName)
    }

}
