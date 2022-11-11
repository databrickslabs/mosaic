package com.databricks.labs.mosaic.models

import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.models.knn.ApproximateSpatialKNN
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.matchers.must.Matchers.{be, contain, noException}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.nio.file.Files

trait ApproximateSpatialKNNBehaviors extends MosaicSpatialQueryTest {

    def behavior(mosaicContext: MosaicContext): Unit = {
        val mc = mosaicContext
        mc.register()
        val sc = spark
        import sc.implicits._

        val (resolution, distanceThreshold) = mc.getIndexSystem match {
            case H3IndexSystem  => (3, 100.0)
            case BNGIndexSystem => (-3, 10000.0)
        }

        val boroughs: DataFrame = getBoroughs(mc)

        val tempLocation = Files.createTempDirectory("mosaic").toAbsolutePath.toString
        spark.sparkContext.setCheckpointDir(tempLocation)
        spark.sparkContext.setLogLevel("ERROR")

        val knn = ApproximateSpatialKNN(boroughs)
            .setUseTableCheckpoint(false)
            .setKNeighbours(20)
            .setLeftFeatureCol("wkt")
            .setRightFeatureCol("wkt")
            .setMaxIterations(100)
            .setEarlyStopping(3)
            // note this is CRS specific
            .setDistanceThreshold(distanceThreshold)
            .setIndexResolution(resolution)
            .setCheckpointTablePrefix(tempLocation)

        val matches = knn
            .transform(boroughs)
            .withColumn("left_hash", hash(col("wkt")))
            .withColumn("right_hash", hash(col("right_wkt")))

        matches
            .select(
              max("wkt_wkt_distance")
            )
            .as[Double]
            .collect()
            .head should be <= distanceThreshold

        matches
            .select(
              max("iteration")
            )
            .as[Int]
            .collect()
            .head should be <= 100

        matches
            .select(
              countDistinct("left_miid")
            )
            .as[Long]
            .collect()
            .head should be(boroughs.count())

        matches
            .select(
              countDistinct("right_miid")
            )
            .as[Long]
            .collect()
            .head should be(boroughs.count())

        matches
            .select(
              max("neighbour_number")
            )
            .as[Int]
            .collect()
            .head should be <= 20

        noException should be thrownBy knn.getParams
        noException should be thrownBy knn.getMetrics

        knn.write
            .overwrite()
            .save(s"$tempLocation/knn")

        val loadedKnn = ApproximateSpatialKNN.load(s"$tempLocation/knn")

        knn.getParams should contain theSameElementsAs loadedKnn.getParams

    }

}
