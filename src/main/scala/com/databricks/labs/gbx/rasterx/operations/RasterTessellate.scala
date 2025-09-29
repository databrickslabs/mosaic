package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.gridx.grid.H3
import com.databricks.labs.gbx.rasterx.gdal.GDAL
import org.gdal.gdal.Dataset

/** RasterTessellate is a helper object for tessellating rasters. */
object RasterTessellate {

    def getTile(ds: Dataset, options: Map[String, String], cell: Long): (Long, Dataset, Map[String, String]) = {
        val cellGeom = H3.cellIdToGeometry(cell)
        val (resDs, resMtd) = ClipToGeom.clip(ds, options, cellGeom, GDAL.WSG84)
        if (RasterAccessors.isEmpty(resDs)) return null
        resDs.SetMetadataItem("RASTERX_CELL_ID", cell.toString)
        resDs.FlushCache()
        (cell, resDs, resMtd)
    }

    /**
      * Tessellates a raster into tiles. The raster is projected into the grid
      * system and then split into tiles. Each tile corresponds to a cell in the
      * grid system.
      *
      * @param ds
      *   The raster to tessellate.
      * @param resolution
      *   The resolution of the tiles.
      *
      * @return
      *   A sequence of Raster objects.
      */
    def tessellateH3Iter(
        ds: Dataset,
        options: Map[String, String],
        resolution: Int
    ): Iterator[(Long, Dataset, Map[String, String])] = {
        val bbox = BoundingBox.bbox(ds, GDAL.WSG84)
        val bufR = H3.getBufferRadius(bbox, resolution)
        val cells = H3.polyfill(bbox.buffer(bufR), resolution)

        new Iterator[(Long, Dataset, Map[String, String])] with AutoCloseable {
            private var closed = false
            private var fetched = false
            private var _ds = ds
            private val _cells = cells
            private var cc = 0
            private var nextTile: (Long, Dataset, Map[String, String]) = _

            private def advance(): Unit = {
                fetched = true
                nextTile = null
                while (cc < _cells.length && nextTile == null) {
                    val cell = _cells(cc)
                    nextTile = getTile(_ds, options, cell)
                    cc += 1
                }
                if (cc >= _cells.length && nextTile == null) close()
            }

            override def hasNext: Boolean = {
                if (!fetched && !closed) advance()
                !closed && nextTile != null
            }

            override def next(): (Long, Dataset, Map[String, String]) = {
                if (!fetched && !closed) advance()
                fetched = false
                nextTile
            }

            override def close(): Unit = {
                if (!closed) {
                    closed = true
                    RasterAccessors.unlink(_ds)
                    _ds = null
                }
            }
        }
    }

}
