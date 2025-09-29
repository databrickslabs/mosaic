package com.databricks.labs.gbx.vectorx.ds.shp

import com.databricks.labs.gbx.vectorx.ds.ogr.OGR_DataSource

//noinspection ScalaUnusedSymbol
class ShapeFile_DataSource extends OGR_DataSource {

    override def shortName(): String = "shapefile"

}
