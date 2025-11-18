package com.databricks.labs.gbx.rasterx

import com.databricks.labs.gbx.expressions.{ExpressionConfig, RegistryDelegate}
import com.databricks.labs.gbx.rasterx.expressions.accessors._
import com.databricks.labs.gbx.rasterx.expressions.agg.{RST_CombineAvgAgg, RST_DerivedBandAgg, RST_MergeAgg}
import com.databricks.labs.gbx.rasterx.expressions.constructor.{RST_FromBands, RST_FromContent, RST_FromFile}
import com.databricks.labs.gbx.rasterx.expressions.generators._
import com.databricks.labs.gbx.rasterx.expressions.grid._
import com.databricks.labs.gbx.rasterx.expressions._
import com.databricks.labs.gbx.rasterx.gdal.CheckpointManager
import com.databricks.labs.gbx.rasterx.util.CleanupListener
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, SparkSession}

object functions extends Serializable {

    val flag = "com.databricks.labs.gbx.rasterx.registered"

    def register(spark: SparkSession): Unit = {
        val sc = spark.sparkContext
        if (sc.getConf.get(flag, "false") == "true") return // Prevent multiple registrations

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
//        rd.register(RST_DTMFromGeoms)
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

        sc.getConf.set(flag, "true")
    }

    // Accessors
    def rst_avg(tileExpr: Column): Column = ColumnAdapter(RST_Avg.name, Seq(tileExpr))
    def rst_bandmetadata(tileExpr: Column, band: Column): Column = ColumnAdapter(RST_BandMetaData.name, Seq(tileExpr, band))
    def rst_boundingbox(tileExpr: Column): Column = ColumnAdapter(RST_BoundingBox.name, Seq(tileExpr))
    def rst_format(tileExpr: Column): Column = ColumnAdapter(RST_Format.name, Seq(tileExpr))
    def rst_georeference(tileExpr: Column): Column = ColumnAdapter(RST_GeoReference.name, Seq(tileExpr))
    def rst_getnodata(tileExpr: Column): Column = ColumnAdapter(RST_GetNoData.name, Seq(tileExpr))
    def rst_getsubdataset(tileExpr: Column, subsetName: Column): Column = ColumnAdapter(RST_GetSubdataset.name, Seq(tileExpr, subsetName))
    def rst_height(tileExpr: Column): Column = ColumnAdapter(RST_Height.name, Seq(tileExpr))
    def rst_max(tileExpr: Column): Column = ColumnAdapter(RST_Max.name, Seq(tileExpr))
    def rst_median(tileExpr: Column): Column = ColumnAdapter(RST_Median.name, Seq(tileExpr))
    def rst_memsize(tileExpr: Column): Column = ColumnAdapter(RST_MemSize.name, Seq(tileExpr))
    def rst_metadata(tileExpr: Column): Column = ColumnAdapter(RST_MetaData.name, Seq(tileExpr))
    def rst_min(tileExpr: Column): Column = ColumnAdapter(RST_Min.name, Seq(tileExpr))
    def rst_numbands(tileExpr: Column): Column = ColumnAdapter(RST_NumBands.name, Seq(tileExpr))
    def rst_pixelcount(tileExpr: Column): Column = ColumnAdapter(RST_PixelCount.name, Seq(tileExpr))
    def rst_pixelheight(tileExpr: Column): Column = ColumnAdapter(RST_PixelHeight.name, Seq(tileExpr))
    def rst_pixelwidth(tileExpr: Column): Column = ColumnAdapter(RST_PixelWidth.name, Seq(tileExpr))
    def rst_rotation(tileExpr: Column): Column = ColumnAdapter(RST_Rotation.name, Seq(tileExpr))
    def rst_scalex(tileExpr: Column): Column = ColumnAdapter(RST_ScaleX.name, Seq(tileExpr))
    def rst_scaley(tileExpr: Column): Column = ColumnAdapter(RST_ScaleY.name, Seq(tileExpr))
    def rst_skewx(tileExpr: Column): Column = ColumnAdapter(RST_SkewX.name, Seq(tileExpr))
    def rst_skewy(tileExpr: Column): Column = ColumnAdapter(RST_SkewY.name, Seq(tileExpr))
    def rst_srid(tileExpr: Column): Column = ColumnAdapter(RST_SRID.name, Seq(tileExpr))
    def rst_subdatasets(tileExpr: Column): Column = ColumnAdapter(RST_Subdatasets.name, Seq(tileExpr))
    def rst_summary(tileExpr: Column): Column = ColumnAdapter(RST_Summary.name, Seq(tileExpr))
    def rst_type(tileExpr: Column): Column = ColumnAdapter(RST_Type.name, Seq(tileExpr))
    def rst_upperleftx(tileExpr: Column): Column = ColumnAdapter(RST_UpperLeftX.name, Seq(tileExpr))
    def rst_upperlefty(tileExpr: Column): Column = ColumnAdapter(RST_UpperLeftY.name, Seq(tileExpr))
    def rst_width(tileExpr: Column): Column = ColumnAdapter(RST_Width.name, Seq(tileExpr))

    // Aggregators
    def rst_combineavgagg(tileExpr: Column): Column = ColumnAdapter(RST_CombineAvgAgg.name, Seq(tileExpr))
    def rst_derivedbandagg(tileExpr: Column, pyfunc: String, funcName: String): Column =
        ColumnAdapter(RST_DerivedBandAgg.name, Seq(tileExpr, lit(pyfunc), lit(funcName)))
    def rst_mergeagg(tileExpr: Column): Column = ColumnAdapter(RST_MergeAgg.name, Seq(tileExpr))

    // Constructors
    def rst_fromcontent(content: Column, driver: Column): Column = ColumnAdapter(RST_FromContent.name, Seq(content, driver))
    def rst_fromfile(path: Column, driver: Column): Column = ColumnAdapter(RST_FromFile.name, Seq(path, driver))
    def rst_frombands(bands: Column): Column = ColumnAdapter(RST_FromBands.name, Seq(bands))

    // Generators
    def rst_h3_tessellate(tileExpr: Column, resolution: Column): Column = ColumnAdapter(RST_H3_Tessellate.name, Seq(tileExpr, resolution))
    def rst_maketiles(tileExpr: Column, tileWidth: Column, tileHeight: Column): Column =
        ColumnAdapter(RST_MakeTiles.name, Seq(tileExpr, tileWidth, tileHeight))
    def rst_retile(tileExpr: Column, tileWidth: Column, tileHeight: Column): Column =
        ColumnAdapter(RST_ReTile.name, Seq(tileExpr, tileWidth, tileHeight))
    def rst_separatebands(tileExpr: Column): Column = ColumnAdapter(RST_SeparateBands.name, Seq(tileExpr))
    def rst_tooverlappingtiles(tileExpr: Column, tileWidth: Column, tileHeight: Column, overlap: Column): Column =
        ColumnAdapter(RST_ToOverlappingTiles.name, Seq(tileExpr, tileWidth, tileHeight, overlap))

    // Grid
    def rst_h3_rastertogridavg(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_H3_RasterToGridAvg.name, Seq(tileExpr, resolution))
    def rst_h3_rastertogridcount(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_H3_RasterToGridCount.name, Seq(tileExpr, resolution))
    def rst_h3_rastertogridmax(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_H3_RasterToGridMax.name, Seq(tileExpr, resolution))
    def rst_h3_rastertogridmin(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_H3_RasterToGridMin.name, Seq(tileExpr, resolution))
    def rst_h3_rastertogridmedian(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_H3_RasterToGridMedian.name, Seq(tileExpr, resolution))

    // Operations
    def rst_asformat(tileExpr: Column, newFormat: Column): Column = ColumnAdapter(RST_AsFormat.name, Seq(tileExpr, newFormat))
    def rst_clip(tileExpr: Column, clip: Column, cutlineAllTouched: Column): Column =
        ColumnAdapter(RST_Clip.name, Seq(tileExpr, clip, cutlineAllTouched))
    def rst_combineavg(tiles: Column): Column = ColumnAdapter(RST_CombineAvg.name, Seq(tiles))
    def rst_convolve(tileExpr: Column, kernel: Column): Column = ColumnAdapter(RST_Convolve.name, Seq(tileExpr, kernel))
    def rst_derivedband(tileExpr: Column, pyfunc: String, funcName: String): Column =
        ColumnAdapter(RST_DerivedBand.name, Seq(tileExpr, lit(pyfunc), lit(funcName)))
//    def rst_dtmfromgeoms(geometries: Column, pixelSize: Column, extent: Column): Column =
//        ColumnAdapter(RST_DTMFromGeoms.name, Seq(geometries, pixelSize, extent))
    def rst_filter(tileExpr: Column, kernelSize: Column, operation: Column): Column =
        ColumnAdapter(RST_Filter.name, Seq(tileExpr, kernelSize, operation))
    def rst_initnodata(tileExpr: Column): Column = ColumnAdapter(RST_InitNoData.name, Seq(tileExpr))
    def rst_isempty(tileExpr: Column): Column = ColumnAdapter(RST_IsEmpty.name, Seq(tileExpr))
    def rst_mapalgebra(tiles: Column, expression: Column): Column = ColumnAdapter(RST_MapAlgebra.name, Seq(tiles, expression))
    def rst_merge(tiles: Column): Column = ColumnAdapter(RST_Merge.name, Seq(tiles))
    def rst_ndvi(tileExpr: Column, redBand: Column, nirBand: Column): Column = ColumnAdapter(RST_NDVI.name, Seq(tileExpr, redBand, nirBand))
    def rst_rastertoworldcoord(tileExpr: Column, pixelX: Column, pixelY: Column): Column =
        ColumnAdapter(RST_RasterToWorldCoord.name, Seq(tileExpr, pixelX, pixelY))
    def rst_rastertoworldcoordx(tileExpr: Column, pixelX: Column, pixelY: Column): Column =
        ColumnAdapter(RST_RasterToWorldCoordX.name, Seq(tileExpr, pixelX, pixelY))
    def rst_rastertoworldcoordy(tileExpr: Column, pixelX: Column, pixelY: Column): Column =
        ColumnAdapter(RST_RasterToWorldCoordY.name, Seq(tileExpr, pixelX, pixelY))
    def rst_transform(tileExpr: Column, targetSrid: Column): Column = ColumnAdapter(RST_Transform.name, Seq(tileExpr, targetSrid))
    def rst_tryopen(tileExpr: Column): Column = ColumnAdapter(RST_TryOpen.name, Seq(tileExpr))
    def rst_updatetype(tileExpr: Column, newType: Column): Column = ColumnAdapter(RST_UpdateType.name, Seq(tileExpr, newType))
    def rst_worldtorastercoord(tileExpr: Column, worldX: Column, worldY: Column): Column =
        ColumnAdapter(RST_WorldToRasterCoord.name, Seq(tileExpr, worldX, worldY))
    def rst_worldtorastercoordx(tileExpr: Column, worldX: Column, worldY: Column): Column =
        ColumnAdapter(RST_WorldToRasterCoordX.name, Seq(tileExpr, worldX, worldY))
    def rst_worldtorastercoordy(tileExpr: Column, worldX: Column, worldY: Column): Column =
        ColumnAdapter(RST_WorldToRasterCoordY.name, Seq(tileExpr, worldX, worldY))

}
