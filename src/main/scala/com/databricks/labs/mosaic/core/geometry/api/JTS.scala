package com.databricks.labs.mosaic.core.geometry.api

import com.databricks.labs.mosaic.codegen.format.{GeometryIOCodeGen, MosaicGeometryIOCodeGenJTS}
import com.databricks.labs.mosaic.core.geometry.{MosaicGeometry, MosaicGeometryJTS}
import com.databricks.labs.mosaic.core.geometry.point.{MosaicPoint, MosaicPointJTS}
import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygonJTS
import com.databricks.labs.mosaic.core.types.model.{Coordinates, InternalCoord}
import org.locationtech.jts.geom.{Geometry => JTSGeometry}

object JTS extends GeometryAPI(MosaicGeometryJTS) {

    override def name: String = "JTS"

    override def fromGeoCoord(geoCoord: Coordinates): MosaicPointJTS = MosaicPointJTS(geoCoord)

    override def fromCoords(coords: Seq[Double]): MosaicPointJTS = MosaicPointJTS(coords)

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

    override def polygon(shells: Seq[Seq[MosaicPoint]], holes: Seq[Seq[Seq[MosaicPoint]]], srid: Int): MosaicGeometry = {
        MosaicPolygonJTS.fromRings(
          shells.head.toArray.map(_.asSeq).map(InternalCoord(_)),
          holes.head.toArray.map(_.map(_.asSeq).map(InternalCoord(_)).toArray),
          srid
        )
    }

    override def polygon(shells: Seq[Seq[MosaicPoint]], holes: Seq[Seq[Seq[MosaicPoint]]]): MosaicGeometry = {
        MosaicPolygonJTS.fromRings(
          shells.head.toArray.map(_.asSeq).map(InternalCoord(_)),
          holes.head.toArray.map(_.map(_.asSeq).map(InternalCoord(_)).toArray),
          4326
        )
    }

}
