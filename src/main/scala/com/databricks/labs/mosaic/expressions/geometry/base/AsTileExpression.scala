package com.databricks.labs.mosaic.expressions.geometry.base

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.ogr.{DataSource, FieldDefn, Geometry, Layer, Feature, ogrConstants}
import org.gdal.osr.SpatialReference

import scala.collection.mutable

trait AsTileExpression {

    def getSRS(firstRow: Any, geometryExpr: Expression, geometryAPI: GeometryAPI): SpatialReference = {
        val firstGeomRaw = firstRow
            .asInstanceOf[InternalRow]
            .get(0, geometryExpr.dataType)

        val firstGeom = geometryAPI.geometry(firstGeomRaw, geometryExpr.dataType)
        val srsOSR = firstGeom.getSpatialReferenceOSR

        val srs = new org.gdal.osr.SpatialReference()
        if (srsOSR != null) {
            srs.ImportFromWkt(srsOSR.ExportToWkt())
        } else {
            srs.ImportFromEPSG(4326)
        }

        srs
    }

    def createLayer(ds: DataSource, srs: SpatialReference, schema: StructType): Layer = {
        val layer = ds.CreateLayer("tiles", srs, ogrConstants.wkbUnknown)
        for (field <- schema) {
            val fieldDefn = field.dataType match {
                case StringType    =>
                    val fieldDefn = new FieldDefn(field.name, ogrConstants.OFTString)
                    fieldDefn.SetWidth(255)
                    fieldDefn
                case IntegerType   => new FieldDefn(field.name, ogrConstants.OFTInteger)
                case LongType      => new FieldDefn(field.name, ogrConstants.OFTInteger64)
                case FloatType     => new FieldDefn(field.name, ogrConstants.OFTReal)
                case DoubleType    => new FieldDefn(field.name, ogrConstants.OFTReal)
                case BooleanType   => new FieldDefn(field.name, ogrConstants.OFTInteger)
                case DateType      => new FieldDefn(field.name, ogrConstants.OFTDate)
                case TimestampType => new FieldDefn(field.name, ogrConstants.OFTDateTime)
                case _             => throw new Error(s"Unsupported data type: ${field.dataType}")
            }
            layer.CreateField(fieldDefn)
        }
        layer
    }

    def insertRows(
        buffer: mutable.ArrayBuffer[Any],
        layer: Layer,
        geometryExpr: Expression,
        geometryAPI: GeometryAPI,
        attributesExpr: Expression
    ): Unit = {
        for (row <- buffer) {
            val geom = row.asInstanceOf[InternalRow].get(0, geometryExpr.dataType)
            val geomOgr = Geometry.CreateFromWkb(geometryAPI.geometry(geom, geometryExpr.dataType).toWKB)
            val attrs = row.asInstanceOf[InternalRow].get(1, attributesExpr.dataType)
            val feature = new Feature(layer.GetLayerDefn)
            feature.SetGeometryDirectly(geomOgr)
            var i = 0
            for (field <- attributesExpr.dataType.asInstanceOf[StructType]) {
                val value = attrs.asInstanceOf[InternalRow].get(i, field.dataType)
                field.dataType match {
                    case StringType    => feature.SetField(field.name, value.asInstanceOf[UTF8String].toString)
                    case IntegerType   => feature.SetField(field.name, value.asInstanceOf[Int])
                    case LongType      => feature.SetField(field.name, value.asInstanceOf[Long])
                    case FloatType     => feature.SetField(field.name, value.asInstanceOf[Float])
                    case DoubleType    => feature.SetField(field.name, value.asInstanceOf[Double])
                    case BooleanType   => feature.SetField(field.name, value.asInstanceOf[Boolean].toString)
                    case DateType      => feature.SetField(field.name, value.asInstanceOf[java.sql.Date].toString)
                    case TimestampType => feature.SetField(field.name, value.asInstanceOf[java.sql.Timestamp].toString)
                    case _             => throw new Error(s"Unsupported data type: ${field.dataType}")
                }
            }
            layer.CreateFeature(feature)
            i += 1
        }
    }

}
