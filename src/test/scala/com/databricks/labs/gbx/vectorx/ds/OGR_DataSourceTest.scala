package com.databricks.labs.gbx.vectorx.ds

import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class OGR_DataSourceTest extends PlanTest with SilentSparkSession {

    test("GDAL Data Source must read tif files") {
        val sp = spark
        import com.databricks.labs.gbx.udfs._
        import sp.implicits._

        val shpPath = this.getClass.getResource("/binary/elevation/sd46_dtm_breakline.shp").toString.replace("file:", "")

        val res = spark.read
            .format("ogr")
            .option("chunkSize", "100")
            .load(shpPath)
            .limit(10)
            .select(st_area(col("geom_0")).as("area"))
            .as[Double]
            .collect()

        res.foreach(v => v should be >= 0.0)

        val res2 = spark.read
            .format("shapefile")
            .option("chunkSize", "100")
            .load(shpPath)

        res2.count() should be > 0L

    }

}
