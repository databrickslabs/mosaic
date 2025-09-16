package com.dblabs.spatial.rasterx

import com.dblabs.spatial.expressions.{ExpressionConfig, RegistryDelegate}
import com.dblabs.spatial.rasterx.expressions._
import com.dblabs.spatial.rasterx.expressions.accessors._
import com.dblabs.spatial.rasterx.expressions.agg.{RST_CombineAvgAgg, RST_DerivedBandAgg, RST_MergeAgg}
import com.dblabs.spatial.rasterx.expressions.constructor._
import com.dblabs.spatial.rasterx.expressions.generators._
import com.dblabs.spatial.rasterx.expressions.grid._
import com.dblabs.spatial.rasterx.gdal.CheckpointManager
import com.dblabs.spatial.rasterx.util.CleanupListener
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, SparkSession}

object functions extends Serializable {

    var initialized = false

    def register(spark: SparkSession): Unit = {
        if (initialized) return // Prevent multiple registrations

        val expressionConfig = ExpressionConfig(spark)
        CheckpointManager.init(expressionConfig)
        spark.sparkContext.addSparkListener(new CleanupListener(spark))

        val registry = spark.sessionState.functionRegistry
        val rd = RegistryDelegate(registry)

        // Accessors
        rd.register(RST_Avg)
        rd.register(RST_BandMetaData)
        rd.register(RST_BoundingBox)
        rd.register(RST_Format)
        rd.register(RST_GeoReference)
        rd.register(RST_GetNoData)
        rd.register(RST_GetSubdataset)
        rd.register(RST_Height)
        rd.register(RST_Max)
        rd.register(RST_Median)
        rd.register(RST_MemSize)
        rd.register(RST_MetaData)
        rd.register(RST_Min)
        rd.register(RST_NumBands)
        rd.register(RST_PixelCount)
        rd.register(RST_PixelHeight)
        rd.register(RST_PixelWidth)
        rd.register(RST_Rotation)
        rd.register(RST_ScaleX)
        rd.register(RST_ScaleY)
        rd.register(RST_SkewX)
        rd.register(RST_SkewY)
        rd.register(RST_SRID)
        rd.register(RST_Subdatasets)
        rd.register(RST_Summary)
        rd.register(RST_Type)
        rd.register(RST_UpperLeftX)
        rd.register(RST_UpperLeftY)
        rd.register(RST_Width)

        // Aggregators
        rd.register(RST_CombineAvgAgg)
        rd.register(RST_DerivedBandAgg)
        rd.register(RST_MergeAgg)

        // Constructors
        rd.register(RST_FromBands)
        rd.register(RST_FromContent)
        rd.register(RST_FromFile)

        // Generators
        rd.register(RST_H3_Tessellate)
        rd.register(RST_MakeTiles)
        rd.register(RST_ReTile)
        rd.register(RST_SeparateBands)
        rd.register(RST_ToOverlappingTiles)

        // Grid
        rd.register(RST_H3_RasterToGridAvg)
        rd.register(RST_H3_RasterToGridCount)
        rd.register(RST_H3_RasterToGridMax)
        rd.register(RST_H3_RasterToGridMin)
        rd.register(RST_H3_RasterToGridMedian)

        // Operations
        rd.register(RST_AsFormat)
        rd.register(RST_Clip)
        rd.register(RST_CombineAvg)
        rd.register(RST_Convolve)
        rd.register(RST_DerivedBand)
        rd.register(RST_DTMFromGeoms)
        rd.register(RST_Filter)
        rd.register(RST_InitNoData)
        rd.register(RST_IsEmpty)
        rd.register(RST_MapAlgebra)
        rd.register(RST_Merge)
        rd.register(RST_NDVI)
        rd.register(RST_RasterToWorldCoord)
        rd.register(RST_RasterToWorldCoordX)
        rd.register(RST_RasterToWorldCoordY)
        rd.register(RST_Transform)
        rd.register(RST_TryOpen)
        rd.register(RST_UpdateType)
        rd.register(RST_WorldToRasterCoord)
        rd.register(RST_WorldToRasterCoordX)
        rd.register(RST_WorldToRasterCoordY)

        initialized = true
    }

    // Accessors
    def rst_avg(tileExpr: Column): Column = ColumnAdapter("rst_avg", Seq(tileExpr))
    def rst_bandmetadata(tileExpr: Column, band: Column): Column = ColumnAdapter("rst_bandmetadata", Seq(tileExpr, band))
    def rst_boundingbox(tileExpr: Column): Column = ColumnAdapter("rst_boundingbox", Seq(tileExpr))
    def rst_format(tileExpr: Column): Column = ColumnAdapter("rst_format", Seq(tileExpr))
    def rst_georeference(tileExpr: Column): Column = ColumnAdapter("rst_georeference", Seq(tileExpr))
    def rst_getnodata(tileExpr: Column): Column = ColumnAdapter("rst_getnodata", Seq(tileExpr))
    def rst_getsubdataset(tileExpr: Column, subsetName: Column): Column = ColumnAdapter("rst_getsubdataset", Seq(tileExpr, subsetName))
    def rst_height(tileExpr: Column): Column = ColumnAdapter("rst_height", Seq(tileExpr))
    def rst_max(tileExpr: Column): Column = ColumnAdapter("rst_max", Seq(tileExpr))
    def rst_median(tileExpr: Column): Column = ColumnAdapter("rst_median", Seq(tileExpr))
    def rst_memsize(tileExpr: Column): Column = ColumnAdapter("rst_memsize", Seq(tileExpr))
    def rst_metadata(tileExpr: Column): Column = ColumnAdapter("rst_metadata", Seq(tileExpr))
    def rst_min(tileExpr: Column): Column = ColumnAdapter("rst_min", Seq(tileExpr))
    def rst_numbands(tileExpr: Column): Column = ColumnAdapter("rst_numbands", Seq(tileExpr))
    def rst_pixelcount(tileExpr: Column): Column = ColumnAdapter("rst_pixelcount", Seq(tileExpr))
    def rst_pixelheight(tileExpr: Column): Column = ColumnAdapter("rst_pixelheight", Seq(tileExpr))
    def rst_pixelwidth(tileExpr: Column): Column = ColumnAdapter("rst_pixelwidth", Seq(tileExpr))
    def rst_rotation(tileExpr: Column): Column = ColumnAdapter("rst_rotation", Seq(tileExpr))
    def rst_scalex(tileExpr: Column): Column = ColumnAdapter("rst_scalex", Seq(tileExpr))
    def rst_scaley(tileExpr: Column): Column = ColumnAdapter("rst_scaley", Seq(tileExpr))
    def rst_skewx(tileExpr: Column): Column = ColumnAdapter("rst_skewx", Seq(tileExpr))
    def rst_skewy(tileExpr: Column): Column = ColumnAdapter("rst_skewy", Seq(tileExpr))
    def rst_srid(tileExpr: Column): Column = ColumnAdapter("rst_srid", Seq(tileExpr))
    def rst_subdatasets(tileExpr: Column): Column = ColumnAdapter("rst_subdatasets", Seq(tileExpr))
    def rst_summary(tileExpr: Column): Column = ColumnAdapter("rst_summary", Seq(tileExpr))
    def rst_type(tileExpr: Column): Column = ColumnAdapter("rst_type", Seq(tileExpr))
    def rst_upperleftx(tileExpr: Column): Column = ColumnAdapter("rst_upperleftx", Seq(tileExpr))
    def rst_upperlefty(tileExpr: Column): Column = ColumnAdapter("rst_upperlefty", Seq(tileExpr))
    def rst_width(tileExpr: Column): Column = ColumnAdapter("rst_width", Seq(tileExpr))

    // Aggregators
    def rst_combineavgagg(tileExpr: Column): Column = ColumnAdapter("rst_combine_avg_agg", Seq(tileExpr))
    def rst_derivedbandagg(tileExpr: Column, pyfunc: String, funcName: String): Column =
        ColumnAdapter("rst_derived_band_agg", Seq(tileExpr, lit(pyfunc), lit(funcName)))
    def rst_mergeagg(tileExpr: Column): Column = ColumnAdapter("rst_merge_agg", Seq(tileExpr))

    // Constructors
    def rst_fromcontent(content: Column, driver: Column): Column = ColumnAdapter("rst_fromcontent", Seq(content, driver))
    def rst_fromfile(path: Column, driver: Column): Column = ColumnAdapter("rst_fromfile", Seq(path, driver))
    def rst_frombands(bands: Column): Column = ColumnAdapter("rst_frombands", Seq(bands))

    // Generators
    def rst_h3_tessellate(tileExpr: Column, resolution: Column): Column = ColumnAdapter("rst_h3_tessellate", Seq(tileExpr, resolution))
    def rst_maketiles(tileExpr: Column, tileWidth: Column, tileHeight: Column): Column =
        ColumnAdapter("rst_maketiles", Seq(tileExpr, tileWidth, tileHeight))
    def rst_retile(tileExpr: Column, tileWidth: Column, tileHeight: Column): Column =
        ColumnAdapter("rst_retile", Seq(tileExpr, tileWidth, tileHeight))
    def rst_separatebands(tileExpr: Column): Column = ColumnAdapter("rst_separatebands", Seq(tileExpr))
    def rst_tooverlappingtiles(tileExpr: Column, tileWidth: Column, tileHeight: Column, overlap: Column): Column =
        ColumnAdapter("rst_tooverlappingtiles", Seq(tileExpr, tileWidth, tileHeight, overlap))

    // Grid
    def rst_h3_rastertogridavg(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter("rst_h3_rastertogridavg", Seq(tileExpr, resolution))
    def rst_h3_rastertogridcount(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter("rst_h3_rastertogridcount", Seq(tileExpr, resolution))
    def rst_h3_rastertogridmax(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter("rst_h3_rastertogridmax", Seq(tileExpr, resolution))
    def rst_h3_rastertogridmin(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter("rst_h3_rastertogridmin", Seq(tileExpr, resolution))
    def rst_h3_rastertogridmedian(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter("rst_h3_rastertogridmedian", Seq(tileExpr, resolution))

    // Operations
    def rst_asformat(tileExpr: Column, newFormat: Column): Column = ColumnAdapter("rst_asformat", Seq(tileExpr, newFormat))
    def rst_clip(tileExpr: Column, clip: Column, cutlineAllTouched: Column): Column =
        ColumnAdapter("rst_clip", Seq(tileExpr, clip, cutlineAllTouched))
    def rst_combineavg(tiles: Column): Column = ColumnAdapter("rst_combineavg", Seq(tiles))
    def rst_convolve(tileExpr: Column, kernel: Column): Column = ColumnAdapter("rst_convolve", Seq(tileExpr, kernel))
    def rst_derivedband(tileExpr: Column, pyfunc: String, funcName: String): Column =
        ColumnAdapter("rst_derivedband", Seq(tileExpr, lit(pyfunc), lit(funcName)))
    def rst_dtmfromgeoms(geometries: Column, pixelSize: Column, extent: Column): Column =
        ColumnAdapter("rst_dtmfromgeoms", Seq(geometries, pixelSize, extent))
    def rst_filter(tileExpr: Column, kernelSize: Column, operation: Column): Column =
        ColumnAdapter("rst_filter", Seq(tileExpr, kernelSize, operation))
    def rst_initnodata(tileExpr: Column, noDataValue: Column): Column = ColumnAdapter("rst_initnodata", Seq(tileExpr, noDataValue))
    def rst_isempty(tileExpr: Column): Column = ColumnAdapter("rst_isempty", Seq(tileExpr))
    def rst_mapalgebra(tiles: Column, expression: Column): Column = ColumnAdapter("rst_mapalgebra", Seq(tiles, expression))
    def rst_merge(tiles: Column): Column = ColumnAdapter("rst_merge", Seq(tiles))
    def rst_ndvi(tileExpr: Column, nirBand: Column, redBand: Column): Column = ColumnAdapter("rst_ndvi", Seq(tileExpr, nirBand, redBand))
    def rst_rastertoworldcoord(tileExpr: Column, pixelX: Column, pixelY: Column): Column =
        ColumnAdapter("rst_rastertoworldcoord", Seq(tileExpr, pixelX, pixelY))
    def rst_rastertoworldcoordx(tileExpr: Column, pixelX: Column, pixelY: Column): Column =
        ColumnAdapter("rst_rastertoworldcoordx", Seq(tileExpr, pixelX, pixelY))
    def rst_rastertoworldcoordy(tileExpr: Column, pixelX: Column, pixelY: Column): Column =
        ColumnAdapter("rst_rastertoworldcoordy", Seq(tileExpr, pixelX, pixelY))
    def rst_transform(tileExpr: Column, targetSrid: Column): Column = ColumnAdapter("rst_transform", Seq(tileExpr, targetSrid))
    def rst_tryopen(path: Column): Column = ColumnAdapter("rst_tryopen", Seq(path))
    def rst_updatetype(tileExpr: Column, newType: Column): Column = ColumnAdapter("rst_updatetype", Seq(tileExpr, newType))
    def rst_worldtorastercoord(tileExpr: Column, worldX: Column, worldY: Column): Column =
        ColumnAdapter("rst_worldtorastercoord", Seq(tileExpr, worldX, worldY))
    def rst_worldtorastercoordx(tileExpr: Column, worldX: Column, worldY: Column): Column =
        ColumnAdapter("rst_worldtorastercoordx", Seq(tileExpr, worldX, worldY))
    def rst_worldtorastercoordy(tileExpr: Column, worldX: Column, worldY: Column): Column =
        ColumnAdapter("rst_worldtorastercoordy", Seq(tileExpr, worldX, worldY))

}
