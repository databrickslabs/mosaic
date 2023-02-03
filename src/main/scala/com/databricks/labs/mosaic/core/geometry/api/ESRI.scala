package com.databricks.labs.mosaic.core.geometry.api

import com.databricks.labs.mosaic.codegen.format.{GeometryIOCodeGen, MosaicGeometryIOCodeGenESRI}
import com.databricks.labs.mosaic.core.geometry.MosaicGeometryESRI
import com.databricks.labs.mosaic.core.geometry.point.{MosaicPoint, MosaicPointESRI}
import com.databricks.labs.mosaic.core.types.model.Coordinates
import com.esri.core.geometry.ogc.{OGCGeometry, OGCPoint}

object ESRI extends GeometryAPI(MosaicGeometryESRI) {

    override def name: String = "ESRI"

    override def fromGeoCoord(point: Coordinates): MosaicPoint = MosaicPointESRI(point)

    override def fromCoords(coords: Seq[Double]): MosaicPoint = MosaicPointESRI(coords)

    override def ioCodeGen: GeometryIOCodeGen = MosaicGeometryIOCodeGenESRI

    override def codeGenTryWrap(code: String): String = code

    override def geometryClass: String = classOf[OGCGeometry].getName

    override def mosaicGeometryClass: String = classOf[MosaicGeometryESRI].getName

    override def geometryTypeCode: String = "geometryType()"

    override def geometryIsValidCode: String = "isSimple()"

    override def geometryLengthCode: String = "getEsriGeometry().calculateLength2D()"

    override def geometrySRIDCode(geomInRef: String): String =
        s"($geomInRef.esriSR == null) ? 0 : $geomInRef.getEsriSpatialReference().getID()"

    override def pointClassName: String = classOf[OGCPoint].getName

    override def centroidCode: String = "centroid()"

    override def xCode: String = "X()"

    override def yCode: String = "Y()"

    override def envelopeCode: String = "envelope()"
}
