package com.databricks.labs.mosaic.core.geometry.api

import com.databricks.labs.mosaic.codegen.format.{GeometryIOCodeGen, MosaicGeometryIOCodeGenJTS}
import com.databricks.labs.mosaic.core.geometry.MosaicGeometryJTS
import com.databricks.labs.mosaic.core.geometry.point.{MosaicPoint, MosaicPointJTS}
import com.databricks.labs.mosaic.core.types.model.Coordinates
import org.locationtech.jts.geom.{Point, Geometry => JTSGeometry}

object JTS extends GeometryAPI(MosaicGeometryJTS) {

    override def name: String = "JTS"

    override def fromGeoCoord(geoCoord: Coordinates): MosaicPoint = MosaicPointJTS(geoCoord)

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

    override def geometryTypeCode: String = "getGeometryType()"

    override def geometryIsValidCode: String = "isValid()"

    override def geometryLengthCode: String = "getLength()"

    override def geometrySRIDCode(geomInRef: String): String = s"$geomInRef.getSRID()"

    override def pointClassName: String = classOf[Point].getName

    override def centroidCode: String = "getCentroid()"

    override def xCode: String = "getX()"

    override def yCode: String = "getY()"

    override def envelopeCode: String = "getEnvelope()"

}
