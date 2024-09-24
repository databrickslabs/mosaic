package com.databricks.labs.mosaic.models.knn

import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers._

trait GridRingNeighboursBehaviors extends MosaicSpatialQueryTest {

    def leftTransform(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val sc = spark
        import sc.implicits._

        val mc = mosaicContext
        import mc.functions._
        mc.register()

        val (resolution, distanceThreshold) = mc.getIndexSystem match {
            case H3IndexSystem  => (7, 0.1)
            case BNGIndexSystem => (2, 50000)
            case _ => (11, 1)
        }

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

        val hexRingNeighbours = GridRingNeighbours(boroughs)
            .setLeftFeatureCol("wkt")
            .setRightFeatureCol("wkt")
            .setIndexResolution(resolution)

        // test iteration 0
        val hexRingNeighbours0 = hexRingNeighbours.setIterationID(1)
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
              .withColumn("neighbours", grid_geometrykloop(col("wkt"), resolution, 3))
        )
        result3 should contain theSameElementsAs expected3

        // test final iteration, iteration ID == -1
        val hexRingNeighboursFinal = hexRingNeighbours.setIterationID(-1)
        val inputFinal = boroughs
            .withColumn("iteration", pmod(col("id"), lit(5)).cast("int"))
            .withColumn(hexRingNeighbours.distanceCol, lit(distanceThreshold))
            .withColumn("left_miid", col("id"))
        val resultFinal = getCounts(hexRingNeighboursFinal.leftTransform(inputFinal))
        val expectedFinal = getExpectedCounts(
          inputFinal
              .withColumn("to_exclude", grid_geometrykring(col("wkt"), resolution, col("iteration")))
              // here it is fine not to use max over window since our distance is constant
              .withColumn("buffer", st_buffer(col("wkt"), col(hexRingNeighbours.distanceCol)))
              .withColumn("candidates", grid_tessellate(col("buffer"), resolution).getField("chips").getField("index_id"))
              .withColumn("neighbours", array_except(col("candidates"), col("to_exclude")))
              .drop("to_exclude", "buffer", "candidates")
        )
        resultFinal should contain theSameElementsAs expectedFinal
    }

    def resultTransform(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val sc = spark
        import sc.implicits._

        val mc = mosaicContext
        import mc.functions._
        mc.register()

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 5
            case BNGIndexSystem => -4
            case _ => 5
        }

        val boroughs: DataFrame = getBoroughs(mc)

        val hexRingNeighbours = GridRingNeighbours(boroughs)
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

    def transform(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val sc = spark
        import sc.implicits._

        val mc = mosaicContext
        import mc.functions._
        mc.register()

        val (resolution, iteration) = mc.getIndexSystem match {
            case H3IndexSystem  => (5, 4)
            case BNGIndexSystem => (-4, 2)
            case _ => (5, 4)
        }

        val boroughs: DataFrame = getBoroughs(mc)

        val hexRingNeighbours = GridRingNeighbours(boroughs)
            .setLeftFeatureCol("wkt")
            .setRightFeatureCol("wkt")
            .setIndexResolution(resolution)
            .setIterationID(iteration)

        val leftInput = boroughs.withColumn("left_miid", monotonically_increasing_id())
        val rightInput = boroughs
            .withColumn("right_miid", monotonically_increasing_id())
            .withColumn("right_wkt_grid", grid_tessellateexplode(col("wkt"), resolution))

        val left = hexRingNeighbours
            .leftTransform(leftInput)
        val right = hexRingNeighbours
            .rightTransform(rightInput)

        val rightUniqueColumns = hexRingNeighbours.uniqueFields(right.schema, left.schema)

        val rightProjected = hexRingNeighbours.rightProjection(right, rightUniqueColumns.fieldNames)

        val hexRingNeighboursNew = hexRingNeighbours.setRight(rightInput)
        val result = hexRingNeighboursNew
            .transform(leftInput)

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
            .where("right_id != id")
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
