package com.dblabs.spatial.gridx.bng

import com.dblabs.spatial.expressions.RegistryDelegate
import com.dblabs.spatial.gridx.bng.agg._
import com.dblabs.spatial.gridx.bng.generators._
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, SparkSession}

object functions extends Serializable {

    val flag = "com.dblabs.spatial.gridx.bng.registered"

    def register(spark: SparkSession): Unit = {
        val sc = spark.sparkContext
        if (sc.getConf.get(flag, "false") == "true") return // Prevent multiple registrations

        val registry = spark.sessionState.functionRegistry
        val rd = RegistryDelegate(registry)

        rd.register(BNG_AsWKB)
        rd.register(BNG_AsWKT)
        rd.register(BNG_CellArea)
        rd.register(BNG_CellIntersection)
        rd.register(BNG_CellUnion)
        rd.register(BNG_Centroid)
        rd.register(BNG_Distance)
        rd.register(BNG_EastNorthAsBNG)
        rd.register(BNG_EuclideanDistance)
        rd.register(BNG_GeometryKLoop)
        rd.register(BNG_GeometryKRing)
        rd.register(BNG_KLoop)
        rd.register(BNG_KRing)
        rd.register(BNG_PointAsBNG)
        rd.register(BNG_Polyfill)
        rd.register(BNG_Tessellate)

        // Aggregators
        rd.register(BNG_CellIntersectionAgg)
        rd.register(BNG_CellUnionAgg)

        // Generators
        rd.register(BNG_GeometryKLoopExplode)
        rd.register(BNG_GeometryKRingExplode)
        rd.register(BNG_KLoopExplode)
        rd.register(BNG_KRingExplode)
        rd.register(BNG_TessellateExplode)

        sc.getConf.set(flag, "true")
    }

    def bng_aswkb(cellId: Column): Column = ColumnAdapter("bng_aswkb", Seq(cellId))
    def bng_aswkt(cellId: Column): Column = ColumnAdapter("bng_aswkt", Seq(cellId))
    def bng_cell_area(cellId: Column): Column = ColumnAdapter("bng_cellarea", Seq(cellId))
    def bng_cell_intersection(c1: Column, c2: Column): Column = ColumnAdapter("bng_cellintersection", Seq(c1, c2))
    def bng_cell_union(c1: Column, c2: Column): Column = ColumnAdapter("bng_cellunion", Seq(c1, c2))
    def bng_centroid(cellId: Column): Column = ColumnAdapter("bng_centroid", Seq(cellId))
    def bng_distance(c1: Column, c2: Column): Column = ColumnAdapter("bng_distance", Seq(c1, c2))
    def bng_eastnorthasbng(east: Column, north: Column, resolution: Column): Column =
        ColumnAdapter("bng_eastnorthasbng", Seq(east, north, resolution))
    def bng_euclideandistance(c1: Column, c2: Column): Column = ColumnAdapter("bng_euclideandistance", Seq(c1, c2))
    def bng_geometry_kloop(geom: Column, res: Column, k: Column): Column = ColumnAdapter("bng_geometrykloop", Seq(geom, res, k))
    def bng_geometry_kring(geom: Column, res: Column, k: Column): Column = ColumnAdapter("bng_geometrykring", Seq(geom, res, k))
    def bng_kloop(cellId: Column, k: Column): Column = ColumnAdapter("bng_kloop", Seq(cellId, k))
    def bng_kring(cellId: Column, k: Column): Column = ColumnAdapter("bng_kring", Seq(cellId, k))
    def bng_pointasbng(point: Column, resolution: Column): Column = ColumnAdapter("bng_pointasbng", Seq(point, resolution))
    def bng_polyfill(geom: Column, res: Column): Column = ColumnAdapter("bng_polyfill", Seq(geom, res))
    def bng_tessellate(geom: Column, res: Column): Column = ColumnAdapter("bng_tessellate", Seq(geom, res, lit(true)))

    // Aggregators
    def bng_cell_intersection_agg(c1: Column): Column = ColumnAdapter("bng_cell_intersection_agg", Seq(c1))
    def bng_cell_union_agg(c1: Column): Column = ColumnAdapter("bng_cell_union_agg", Seq(c1))

    // Generators
    def bng_geometry_kloopexplode(geom: Column, res: Column, k: Column): Column =
        ColumnAdapter("bng_geometrykloopexplode", Seq(geom, res, k))
    def bng_geometry_kringexplode(geom: Column, res: Column, k: Column): Column =
        ColumnAdapter("bng_geometrykringexplode", Seq(geom, res, k))
    def bng_kloopexplode(cellId: Column, k: Column): Column = ColumnAdapter("bng_kloopexplode", Seq(cellId, k))
    def bng_kringexplode(cellId: Column, k: Column): Column = ColumnAdapter("bng_kringexplode", Seq(cellId, k))
    def bng_tessellateexplode(geom: Column, res: Column): Column = ColumnAdapter("bng_tessellateexplode", Seq(geom, res, lit(true)))

}
