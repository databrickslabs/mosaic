package com.databricks.labs.mosaic.core.geometry.api

import com.databricks.labs.mosaic.codegen.format.{GeometryIOCodeGen, MosaicGeometryIOCodeGenESRI}
import com.databricks.labs.mosaic.core.geometry.MosaicGeometryESRI
import com.databricks.labs.mosaic.core.geometry.point.MosaicPointESRI
import com.databricks.labs.mosaic.core.types.model.Coordinates
import com.esri.core.geometry.ogc.OGCGeometry

object ESRI extends GeometryAPI(MosaicGeometryESRI) {

    override def name: String = "ESRI"

    override def fromGeoCoord(point: Coordinates): MosaicPointESRI = MosaicPointESRI(point)

    override def fromCoords(coords: Seq[Double]): MosaicPointESRI = MosaicPointESRI(coords)

    override def ioCodeGen: GeometryIOCodeGen = MosaicGeometryIOCodeGenESRI

    override def codeGenTryWrap(code: String): String = code

    override def geometryClass: String = classOf[OGCGeometry].getName

    override def mosaicGeometryClass: String = classOf[MosaicGeometryESRI].getName

}
