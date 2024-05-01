package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{mocks, MosaicSpatialQueryTest}
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers._

//noinspection ScalaDeprecation
trait GridDistanceBehaviors extends MosaicSpatialQueryTest {

    def behaviorGridDistance(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val resolution = 4

        val boroughs: DataFrame = getBoroughs(mc)
            .withColumn("centroid", st_centroid(col("wkt")))
            .withColumn("cell_id", grid_pointascellid(col("centroid"), resolution))

        val cellPairs = boroughs
            .select(
              col("cell_id").as("cell_id1")
            )
            .join(
              boroughs.select(
                col("cell_id").as("cell_id2")
              ),
              col("cell_id1") =!= col("cell_id2")
            )
            .withColumn(
              "grid_distance",
              grid_distance(col("cell_id1"), col("cell_id2"))
            )

        cellPairs.where(col("grid_distance") =!= 0).count() shouldEqual cellPairs.count()

        boroughs.createOrReplaceTempView("boroughs")

        val sqlResult = spark
            .sql("""with subquery (
                   | select grid_distance(grid_pointascellid(st_centroid(wkt), 4), grid_pointascellid(st_centroid(wkt), 4)) as dist from boroughs
                   |) select * from subquery""".stripMargin)
            .as[Long]
            .collect()

        sqlResult.foreach(_ shouldEqual 0)

    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val sc = spark
        import sc.implicits._
        val mc = mosaicContext
        mc.register(spark)

        val wkt = mocks.getWKTRowsDf(mc.getIndexSystem).limit(1).select("wkt").as[String].collect().head

        val gridDistanceExpr = GridDistance(
          mc.functions.grid_pointascellid(mc.functions.st_centroid(lit(wkt)), lit(4)).expr,
          mc.functions.grid_pointascellid(mc.functions.st_centroid(lit(wkt)), lit(4)).expr,
          mc.getIndexSystem,
          mc.getGeometryAPI.name
        )

        mc.getIndexSystem match {
            case H3IndexSystem  => gridDistanceExpr.dataType shouldEqual LongType
            case BNGIndexSystem => gridDistanceExpr.dataType shouldEqual LongType
            case _              => gridDistanceExpr.dataType shouldEqual LongType
        }

        noException should be thrownBy mc.functions.grid_distance(lit(1L), lit(1L))
        noException should be thrownBy gridDistanceExpr.makeCopy(gridDistanceExpr.children.toArray)
        noException should be thrownBy gridDistanceExpr.prettyName
    }

}
