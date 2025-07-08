package com.databricks.labs.mosaic.core.geometry.api

import com.databricks.labs.mosaic.codegen.format.{GeometryIOCodeGen, MosaicGeometryIOCodeGenJTS}
import com.databricks.labs.mosaic.core.geometry.MosaicGeometryJTS
import com.databricks.labs.mosaic.core.geometry.point.MosaicPointJTS
import com.databricks.labs.mosaic.core.types.model.Coordinates
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, MultiPolygon, Polygon, Geometry => JTSGeometry}

object JTS extends GeometryAPI(MosaicGeometryJTS) {

    val geometryFactory = new GeometryFactory()

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


    def makePolygonFromCoords(shellCoords: Seq[(Double, Double)], holeCoords: Seq[Seq[(Double, Double)]]): Polygon = {
        val shell = geometryFactory.createLinearRing(shellCoords.map { case (x, y) => new Coordinate(x, y) }.toArray)
        val holes = holeCoords.map { ring =>
            geometryFactory.createLinearRing(ring.map { case (x, y) => new Coordinate(x, y) }.toArray)
        }.toArray
        geometryFactory.createPolygon(shell, holes)
    }

    def makeMultiPolygon(polygons: Seq[Polygon]): MultiPolygon = {
        geometryFactory.createMultiPolygon(polygons.toArray)
    }

}
