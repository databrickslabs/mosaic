package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.expressions.RegistryDelegate
import com.databricks.labs.gbx.gridx.bng.agg.{BNG_CellIntersectionAgg, BNG_CellUnionAgg}
import com.databricks.labs.gbx.gridx.bng.generators._
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, SparkSession}

object functions extends Serializable {

    val flag = "com.databricks.labs.gbx.gridx.bng.registered"

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

    def bng_aswkb(cellId: Column): Column = ColumnAdapter(BNG_AsWKB.name, Seq(cellId))
    def bng_aswkt(cellId: Column): Column = ColumnAdapter(BNG_AsWKT.name, Seq(cellId))
    def bng_cell_area(cellId: Column): Column = ColumnAdapter(BNG_CellArea.name, Seq(cellId))
    def bng_cell_intersection(c1: Column, c2: Column): Column = ColumnAdapter(BNG_CellIntersection.name, Seq(c1, c2))
    def bng_cell_union(c1: Column, c2: Column): Column = ColumnAdapter(BNG_CellUnion.name, Seq(c1, c2))
    def bng_centroid(cellId: Column): Column = ColumnAdapter(BNG_Centroid.name, Seq(cellId))
    def bng_distance(c1: Column, c2: Column): Column = ColumnAdapter(BNG_Distance.name, Seq(c1, c2))
    def bng_eastnorthasbng(east: Column, north: Column, resolution: Column): Column =
        ColumnAdapter(BNG_EastNorthAsBNG.name, Seq(east, north, resolution))
    def bng_euclideandistance(c1: Column, c2: Column): Column = ColumnAdapter(BNG_EuclideanDistance.name, Seq(c1, c2))
    def bng_geometry_kloop(geom: Column, res: Column, k: Column): Column = ColumnAdapter(BNG_GeometryKLoop.name, Seq(geom, res, k))
    def bng_geometry_kring(geom: Column, res: Column, k: Column): Column = ColumnAdapter(BNG_GeometryKRing.name, Seq(geom, res, k))
    def bng_kloop(cellId: Column, k: Column): Column = ColumnAdapter(BNG_KLoop.name, Seq(cellId, k))
    def bng_kring(cellId: Column, k: Column): Column = ColumnAdapter(BNG_KRing.name, Seq(cellId, k))
    def bng_pointasbng(point: Column, resolution: Column): Column = ColumnAdapter(BNG_PointAsBNG.name, Seq(point, resolution))
    def bng_polyfill(geom: Column, res: Column): Column = ColumnAdapter(BNG_Polyfill.name, Seq(geom, res))
    def bng_tessellate(geom: Column, res: Column): Column = ColumnAdapter(BNG_Tessellate.name, Seq(geom, res, lit(true)))

    // Aggregators
    def bng_cell_intersection_agg(c1: Column): Column = ColumnAdapter(BNG_CellIntersectionAgg.name, Seq(c1))
    def bng_cell_union_agg(c1: Column): Column = ColumnAdapter(BNG_CellUnionAgg.name, Seq(c1))

    // Generators
    def bng_geometry_kloopexplode(geom: Column, res: Column, k: Column): Column =
        ColumnAdapter(BNG_GeometryKLoopExplode.name, Seq(geom, res, k))
    def bng_geometry_kringexplode(geom: Column, res: Column, k: Column): Column =
        ColumnAdapter(BNG_GeometryKRingExplode.name, Seq(geom, res, k))
    def bng_kloopexplode(cellId: Column, k: Column): Column = ColumnAdapter(BNG_KLoopExplode.name, Seq(cellId, k))
    def bng_kringexplode(cellId: Column, k: Column): Column = ColumnAdapter(BNG_KRingExplode.name, Seq(cellId, k))
    def bng_tessellateexplode(geom: Column, res: Column): Column = ColumnAdapter(BNG_TessellateExplode.name, Seq(geom, res, lit(true)))

}
