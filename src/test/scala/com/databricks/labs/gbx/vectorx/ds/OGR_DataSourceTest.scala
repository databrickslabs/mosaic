package com.databricks.labs.gbx.vectorx.ds

import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper


class OGR_DataSourceTest extends PlanTest with SilentSparkSession {

    test("GDAL Data Source must read OGR files") {
        val sp = spark
        import com.databricks.labs.gbx.udfs._
        import sp.implicits._

        // shapefile

        val shpPath = this.getClass.getResource("/binary/elevation/sd46_dtm_breakline.shp").toString.replace("file:", "")

        val res_sp1 = spark.read
            .format("ogr")
            .option("chunkSize", "100")
            .load(shpPath)
            .limit(10)
            .select(st_area(col("geom_0")).as("area"))
            .as[Double]
            .collect()

        res_sp1.foreach(v => v should be >= 0.0)

        val res_sp2 = spark.read
            .format("shapefile")
            .option("chunkSize", "100")
            .load(shpPath)

        res_sp2.count() should be > 0L

        // shapefile zip

        val shpZipPath = this.getClass.getResource("/binary/shapefile/zip/tl_rd22_13037_addrfeat.zip").toString.replace("file:", "")

        val res_spz = spark.read
            .format("shapefile")
            .load(shpZipPath)

        res_spz.count() should be > 0L

        // file_gdb

        val gdbZipPath = this.getClass.getResource("/binary/gdb/bridges.gdb.zip").toString.replace("file:", "")

        val res_gdb = spark.read
            .format("file_gdb")
            .load(gdbZipPath)

        res_gdb.count() should be > 0L

        // geojson

        val gjPath = this.getClass.getResource("/text/NYC_Taxi_Zones.geojson").toString.replace("file:", "")

        val res_gj1 = spark.read
            .format("geojson")
            .load(gjPath)

        res_gj1.count() should be > 1L // newline geoms

        val res_gj2 = spark.read
            .format("geojson")
            .option("multi", "false")
            .load(gjPath)

        res_gj2.count() shouldEqual 1L // single geom (read)

        val gjAltPath = this.getClass.getResource("/text/sample.geojson").toString.replace("file:", "")
        val res_gj3 = spark.read
            .format("geojson")
            .option("multi", "false")
            .load(gjAltPath)

        res_gj3.count() should be > 1L // multiple geoms

        // gpkg (zip fails)

        val gpkgPath = this.getClass.getResource("/binary/gpkg/util_wastewater_discharge.gpkg").toString.replace("file:", "")

        val res_gpkg = spark.read
            .format("ogr_gpkg")
            .load(gpkgPath)

        res_gpkg.count() should be > 0L

    }

}
