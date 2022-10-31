package com.databricks.labs.mosaic.models.knn

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers._

trait HexRingNeighboursBehaviors extends QueryTest {

    def leftTransform(indexSystem: IndexSystem, geometryAPI: GeometryAPI, resolution: Int): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val sc = spark
        import sc.implicits._

        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register()

        val boroughs: DataFrame = getBoroughs(mc)

        def getCounts(df: DataFrame): Array[Long] = df
            .groupBy("id")
            .count()
            .select("count")
            .as[Long]
            .collect()
        def getExpectedCounts(df: DataFrame): Array[Long] = df
            .select(org.apache.spark.sql.functions.size(col("neighbours")))
            .as[Long]
            .collect()

        val hexRingNeighbours = HexRingNeighbours(boroughs)
            .setLeftFeatureCol("wkt")
            .setIndexResolution(resolution)

        // test iteration 0
        val hexRingNeighbours0 = hexRingNeighbours.setIterationID(0)
        val result0 = getCounts(hexRingNeighbours0.leftTransform(boroughs))
        val expected0 = getExpectedCounts(
          boroughs
              .withColumn("neighbours", grid_geometrykring(col("wkt"), resolution, 1))
        )
        result0 should contain theSameElementsAs expected0

        // test iteration != 0
        val hexRingNeighbours3 = hexRingNeighbours.setIterationID(3)
        val result3 = getCounts(hexRingNeighbours3.leftTransform(boroughs))
        val expected3 = getExpectedCounts(
          boroughs
              .withColumn("neighbours", grid_geometrykdisc(col("wkt"), resolution, 4))
        )
        result3 should contain theSameElementsAs expected3

        // test final iteration, iteration ID == -1
        val hexRingNeighboursFinal = hexRingNeighbours.setIterationID(-1)
        val inputFinal = boroughs.withColumn("iteration", pmod(col("id"), lit(5)).cast("int"))
        val resultFinal = getCounts(hexRingNeighboursFinal.leftTransform(inputFinal))
        val expectedFinal = getExpectedCounts(
          inputFinal
              .withColumn("neighbours", grid_geometrykdisc(col("wkt"), resolution, col("iteration") + 2))
        )
        resultFinal should contain theSameElementsAs expectedFinal
    }

    def resultTransform(indexSystem: IndexSystem, geometryAPI: GeometryAPI, resolution: Int): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val sc = spark
        import sc.implicits._

        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register()

        val boroughs: DataFrame = getBoroughs(mc)

        val hexRingNeighbours = HexRingNeighbours(boroughs)
            .setLeftFeatureCol("wkt")
            .setRightFeatureCol("wkt")
            .setIndexResolution(resolution)
            .setIterationID(2)

        val left = hexRingNeighbours.leftTransform(
          boroughs.withColumn("left_miid", monotonically_increasing_id())
        )
        val right = hexRingNeighbours
            .rightTransform(
              boroughs.withColumn("right_miid", monotonically_increasing_id())
            )
            .withColumn("right_wkt_grid", grid_tessellateexplode(col("wkt"), resolution))

        val rightUniqueColumns = hexRingNeighbours.uniqueFields(right.schema, left.schema)

        val rightProjected = hexRingNeighbours.rightProjection(right, rightUniqueColumns.fieldNames)

        val joined = hexRingNeighbours.mergeTransform(
          left,
          right,
          col("left_wkt_kring").getItem("index_id") === col("right_wkt_grid").getItem("index_id"),
          how = "left_outer"
        )

        val result = hexRingNeighbours.resultTransform(joined)

        result.columns should contain allElementsOf left.columns.filterNot(_ == "left_wkt_kring")
        result.columns should contain allElementsOf rightProjected.columns.filterNot(_ == "right_wkt_grid")
        result.columns should contain allElementsOf Seq("left_miid", "right_miid", "wkt_wkt_distance", "neighbour_number")

        val expectedCounts = joined
            .where("right_miid is not null")
            .groupBy("id")
            .count()
            .withColumn("expected_count", col("count"))
            .drop("count")
        val resultCounts = result
            .where("right_miid is not null")
            .groupBy("id")
            .count()

        expectedCounts
            .join(resultCounts, Seq("id"))
            .select(col("expected_count") === col("count"))
            .as[Boolean]
            .collect()
            .forall(identity) shouldBe true

    }

    def transform(indexSystem: IndexSystem, geometryAPI: GeometryAPI, resolution: Int, iteration: Int): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val sc = spark
        import sc.implicits._

        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register()

        val boroughs: DataFrame = getBoroughs(mc)

        val hexRingNeighbours = HexRingNeighbours(boroughs)
            .setLeftFeatureCol("wkt")
            .setRightFeatureCol("wkt")
            .setIndexResolution(resolution)
            .setIterationID(iteration)

        val leftInput = boroughs.withColumn("left_miid", monotonically_increasing_id())
        val rightInput = boroughs.withColumn("right_miid", monotonically_increasing_id())

        val left = hexRingNeighbours
            .leftTransform(leftInput)
        val right = hexRingNeighbours
            .rightTransform(rightInput)
            .withColumn("right_wkt_grid", grid_tessellateexplode(col("wkt"), resolution))

        val rightUniqueColumns = hexRingNeighbours.uniqueFields(right.schema, left.schema)

        val rightProjected = hexRingNeighbours.rightProjection(right, rightUniqueColumns.fieldNames)

        val hexRingNeighboursNew = hexRingNeighbours.setRight(right)
        val result = hexRingNeighboursNew
            .transform(leftInput)
            //.where(col(hexRingNeighbours.getRightRowID).isNotNull)

        result.columns should contain allElementsOf left.columns.filterNot(_ == "left_wkt_kring")
        result.columns should contain allElementsOf rightProjected.columns.filterNot(_ == "right_wkt_grid")
        result.columns should contain allElementsOf Seq("left_miid", "right_miid", "wkt_wkt_distance", "neighbour_number")

        val allCounts = result
            .groupBy(hexRingNeighbours.getLeftRowID)
            .agg(
              count("*").as("count"),
              first("id").as("id")
            )

        val nonNullCounts = result
            .where(col(hexRingNeighbours.getRightRowID).isNotNull)
            .groupBy(hexRingNeighbours.getLeftRowID)
            .agg(
              count("*").as("count"),
              first("id").as("id")
            )

        val expectedMatches = left
            .join(
              right.withColumnRenamed("id", "right_id"),
              col("left_wkt_kring").getItem("index_id") === col("right_wkt_grid").getItem("index_id"),
              "left_outer"
            )
            .groupBy("id", "right_id")
            .count()

        val expectedCounts = expectedMatches
            .groupBy("id")
            .agg(count("*").as("expected_count"))

        val expectedNonNullCounts = expectedMatches
            .where("right_id is not null")
            .groupBy("id")
            .agg(count("*").as("expected_count"))

        allCounts
            .join(expectedCounts, Seq("id"))
            .select(col("count") === col("expected_count"))
            .as[Boolean]
            .collect()
            .forall(identity) shouldBe true
        nonNullCounts
            .join(expectedNonNullCounts, Seq("id"))
            .select(col("count") === col("expected_count"))
            .as[Boolean]
            .collect()
            .forall(identity) shouldBe true

    }

}
