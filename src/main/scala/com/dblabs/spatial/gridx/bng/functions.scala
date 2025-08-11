package com.dblabs.spatial.gridx.bng

import com.dblabs.spatial.expressions.{ExpressionConfig, RegistryDelegate}
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, SparkSession}

object functions extends Serializable {

    def register(spark: SparkSession): Unit = {
        val expressionConfig = ExpressionConfig(spark)
        val registry = spark.sessionState.functionRegistry
        val rd = RegistryDelegate(registry)

        rd.registerExpression[BNG_AsWKB](expressionConfig)
        rd.registerExpression[BNG_AsWKT](expressionConfig)
        rd.registerExpression[BNG_CellArea](expressionConfig)
        rd.registerExpression[BNG_CellIntersection](expressionConfig)
        rd.registerExpression[BNG_CellIntersectionAgg](expressionConfig)
        rd.registerExpression[BNG_CellUnion](expressionConfig)
        rd.registerExpression[BNG_CellUnionAgg](expressionConfig)
        rd.registerExpression[BNG_Distance](expressionConfig)
        rd.registerExpression[BNG_EastNorthAsBNG](expressionConfig)
        rd.registerExpression[BNG_GeometryKLoop](expressionConfig)
        rd.registerExpression[BNG_GeometryKLoopExplode](expressionConfig)
        rd.registerExpression[BNG_GeometryKRing](expressionConfig)
        rd.registerExpression[BNG_GeometryKRingExplode](expressionConfig)
        rd.registerExpression[BNG_KLoop](expressionConfig)
        rd.registerExpression[BNG_KLoopExplode](expressionConfig)
        rd.registerExpression[BNG_KRing](expressionConfig)
        rd.registerExpression[BNG_KRingExplode](expressionConfig)
        rd.registerExpression[BNG_PointAsBNG](expressionConfig)
        rd.registerExpression[BNG_Polyfill](expressionConfig)
        rd.registerExpression[BNG_Tessellate](expressionConfig)
        rd.registerExpression[BNG_TessellateExplode](expressionConfig)
    }

    def bng_aswkb(cellId: Column): Column = ColumnAdapter("bng_aswkb", Seq(cellId))
    def bng_aswkt(cellId: Column): Column = ColumnAdapter("bng_aswkt", Seq(cellId))
    def bng_cell_area(cellId: Column): Column = ColumnAdapter("bng_cellarea", Seq(cellId))
    def bng_cell_intersection(c1: Column, c2: Column): Column = ColumnAdapter("bng_cellintersection", Seq(c1, c2))
    def bng_cell_intersection_agg(c1: Column, c2: Column): Column = ColumnAdapter("bng_cellintersectionagg", Seq(c1, c2))
    def bng_cell_union(c1: Column, c2: Column): Column = ColumnAdapter("bng_cellunion", Seq(c1, c2))
    def bng_cell_union_agg(c1: Column, c2: Column): Column = ColumnAdapter("bng_cellunion", Seq(c1, c2))
    def bng_distance(c1: Column, c2: Column): Column = ColumnAdapter("bng_distance", Seq(c1, c2))
    def bng_eastnorthasbng(east: Column, north: Column): Column = ColumnAdapter("bng_eastnorthasbng", Seq(east, north))
    def bng_geometry_kloop(geom: Column, res: Column, k: Column): Column = ColumnAdapter("bng_geometrykloop", Seq(geom, res, k))
    def bng_geometry_kloopexplode(geom: Column, res: Column, k: Column): Column =
        ColumnAdapter("bng_geometrykloopexplode", Seq(geom, res, k))
    def bng_geometry_kring(geom: Column, res: Column, k: Column): Column = ColumnAdapter("bng_geometrykring", Seq(geom, res, k))
    def bng_geometry_kringexplode(geom: Column, res: Column, k: Column): Column =
        ColumnAdapter("bng_geometrykringexplode", Seq(geom, res, k))
    def bng_kloop(cellId: Column, k: Column): Column = ColumnAdapter("bng_kloop", Seq(cellId, k))
    def bng_kloopexplode(cellId: Column, k: Column): Column = ColumnAdapter("bng_kloopexplode", Seq(cellId, k))
    def bng_kring(cellId: Column, k: Column): Column = ColumnAdapter("bng_kring", Seq(cellId, k))
    def bng_kringexplode(cellId: Column, k: Column): Column = ColumnAdapter("bng_kringexplode", Seq(cellId, k))
    def bng_pointasbng(point: Column): Column = ColumnAdapter("bng_pointasbng", Seq(point))
    def bng_polyfill(geom: Column, res: Column): Column = ColumnAdapter("bng_polyfill", Seq(geom, res))
    def bng_tessellate(geom: Column, res: Column): Column = ColumnAdapter("bng_tessellate", Seq(geom, res, lit(true)))
    def bng_tessellateexplode(geom: Column, res: Column): Column = ColumnAdapter("bng_tessellateexplode", Seq(geom, res, lit(true)))

}
