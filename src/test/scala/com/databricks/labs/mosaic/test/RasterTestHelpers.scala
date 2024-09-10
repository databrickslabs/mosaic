package com.databricks.labs.mosaic.test

import com.databricks.labs.mosaic.core.raster.gdal.RasterGDAL
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BinaryType, StringType}
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants.GA_ReadOnly

import java.util.{Vector => JVector}
import scala.util.control.Exception.allCatch

object RasterTestHelpers {

    /**
     * Convert a [[DataFrame]] tile col to a RasterTile object.
     * - Only tries for first result.
     * - If df is empty or null, returns tile with an empty [[RasterGDAL]].
     * - If no [[ExprConfig]], defaults to H3.
     *
     * @param df
     *   DataFrame from which to select.
     * @param exprConfigOpt
     *   Config to use for expected index system.
     * @param tileCol
     *   Tile column to select, default 'tile'.
     * @return
     *   [[RasterTile]].
     */
    def getFirstTile(df: DataFrame, exprConfigOpt: Option[ExprConfig], tileCol: String = "tile"): RasterTile = {
        if (df == null || df.count() == 0) {
            // empty raster
            RasterTile(null, raster = RasterGDAL(), rasterType = BinaryType)
        } else {
            // [1] select first tileCol [Row]
            val base = df.select(col(tileCol)).first

            // [2] first value as row with schema
            val tileStruct = base.getStruct(0)

            // [3] cellid based on expected datatype
            val index = tileStruct.get(0).asInstanceOf[Either[Long, String]]

            // [4] raster data type
            val rasterDT = allCatch.opt(tileStruct.getString(1)) match {
                case Some(_) => StringType
                case _ => BinaryType
            }

            // [5] create info map
            // - extractMap not needed here
            // - `toMap` converts map to scala.collection.immutable
            val createInfo = tileStruct
                .getMap[String, String](2)
                .toMap[String, String]

            // [6] return [[RasterTile]]
            //RasterTile(null, raster = RasterGDAL(), rasterType = BinaryType)
            RasterTile(
                index,
                raster = RasterGDAL(createInfo, exprConfigOpt),
                rasterType = rasterDT
            )
        }

    }

    /**
     * Load path using gdal's `OpenEx`.
     *
     * @param rawPath
     *   Path to load.
     * @param driverName
     *   Driver to use.
     * @return
     *   [[Dataset]]
     */
    def datasetFromPathUsingDriver(rawPath: String, driverName: String): Dataset = {
        val drivers = new JVector[String]() // java.util.Vector
        drivers.add(driverName)
        gdal.OpenEx(rawPath, GA_ReadOnly, drivers)
    }

}
