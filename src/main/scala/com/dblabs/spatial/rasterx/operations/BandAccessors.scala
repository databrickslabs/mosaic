package com.dblabs.spatial.rasterx.operations

import org.gdal.gdal.Band
import org.gdal.gdalconst.gdalconstConstants

import scala.jdk.CollectionConverters.DictionaryHasAsScala
import scala.util.Try

object BandAccessors {

    def getMetadata(band: Band): Map[String, String] = {
        if (Option(band).isEmpty) Map.empty
        else {
            Option(band.GetMetadata_Dict())
                .map(_.asScala.toMap.asInstanceOf[Map[String, String]])
                .getOrElse(Map.empty[String, String])
        }
    }

    def getNoDataValue(band: Band): Double = {
        if (Option(band).isEmpty) Double.NaN
        else {
            val noDataVal = Array.fill[java.lang.Double](1)(0)
            band.GetNoDataValue(noDataVal)
            if (noDataVal(0) == null || noDataVal(0).isNaN) Double.NaN
            else {
                val noDataValue = noDataVal(0).doubleValue()
                if (noDataValue == Double.NaN) Double.NaN
                else noDataValue
            }
        }
    }

    def dataTypeHuman(band: Band): String =
        Try(band.getDataType).getOrElse(0) match {
            case gdalconstConstants.GDT_Byte     => "Byte"
            case gdalconstConstants.GDT_UInt16   => "UInt16"
            case gdalconstConstants.GDT_Int16    => "Int16"
            case gdalconstConstants.GDT_UInt32   => "UInt32"
            case gdalconstConstants.GDT_Int32    => "Int32"
            case gdalconstConstants.GDT_Float32  => "Float32"
            case gdalconstConstants.GDT_Float64  => "Float64"
            case gdalconstConstants.GDT_CInt16   => "ComplexInt16"
            case gdalconstConstants.GDT_CInt32   => "ComplexInt32"
            case gdalconstConstants.GDT_CFloat32 => "ComplexFloat32"
            case gdalconstConstants.GDT_CFloat64 => "ComplexFloat64"
            case _                               => "Unknown"
        }

}
