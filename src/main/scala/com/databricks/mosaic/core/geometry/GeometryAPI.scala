package com.databricks.mosaic.core.geometry

import com.databricks.mosaic.core.types
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataType
import org.locationtech.jts.geom.{Geometry => GeometryJTS}

sealed trait GeometryAPI {
  def name: String
  def geometry(geom: Any): MosaicGeometry
  def geometry(inputData: InternalRow, dataType: DataType): MosaicGeometry
  def geometry(inputData: Any, dataType: DataType): MosaicGeometry
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
  }

  case object OGC extends GeometryAPI {

    override def name: String = "OGC"

    override def geometry(geom: Any): MosaicGeometry = ???

    override def geometry(geom: InternalRow, dataType: DataType): MosaicGeometry = ???

    override def geometry(inputData: Any, dataType: DataType): MosaicGeometry = ???

  }

}