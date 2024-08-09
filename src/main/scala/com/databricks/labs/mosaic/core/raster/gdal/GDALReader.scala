package com.databricks.labs.mosaic.core.raster.gdal

import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}

trait GDALReader {

    /**
     * Reads a tile from the given input [[StringType]] or [[BinaryType]] data.
     *   - If it is a byte array, it will read the tile from the byte array.
     *   - If it is a string, it will read the tile from the path.
     *   - Path may be a zip file.
     *   - Path may be a subdataset.
     *   - This is only called from `RST_MakeTiles` currently
     *
     * @param inputRaster
     *   The tile, based on inputDT. Path based rasters with subdatasets are
     *   supported.
     * @param createInfo
     *   Creation info of the tile as relating to [[RasterTile]]
     *   serialization. Note: This is not the same as the metadata of the
     *   tile. This is not the same as GDAL creation options.
     * @param inputDT
     *   [[DataType]] for the tile, either [[StringType]] or [[BinaryType]].
     * @param exprConfigOpt
     *   Option [[ExprConfig]]
     * @return
     *   Returns a [[RasterGDAL]] object.
     */
    def readRasterExpr(
                          inputRaster: Any,
                          createInfo: Map[String, String],
                          inputDT: DataType,
                          exprConfigOpt: Option[ExprConfig]
                      ): RasterGDAL

}
