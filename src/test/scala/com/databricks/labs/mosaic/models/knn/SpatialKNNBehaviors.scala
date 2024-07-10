package com.databricks.labs.mosaic.models.knn

import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, CustomIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.{ExprConfig, MosaicContext}
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.{be, contain, noException}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper


trait SpatialKNNBehaviors { this: AnyFlatSpec =>

    def noApproximation(mosaicContext: MosaicContext, spark: SparkSession): Unit = {
        val sc = spark
        import sc.implicits._
        sc.sparkContext.setLogLevel("ERROR")

        // init
        val mc = mosaicContext
        mc.register(sc)
        import mc.functions._

        // could use spark checkpoint (slower for this)
        // sc.sparkContext.setCheckpointDir("/tmp/mosaic_tmp/spark_checkpoints")

        val (resolution, distanceThreshold) = mc.getIndexSystem match {
            case H3IndexSystem  => (3, 100.0)
            case BNGIndexSystem => (-3, 10000.0)
            case CustomIndexSystem(_) => (3, 10000.0)
            case _ => (3, 100.0)
        }

        val boroughs: DataFrame = getBoroughs(mc)

        val exprConfigOpt = Option(ExprConfig(sc))
        val tempLocation = MosaicContext.createTmpContextDir(exprConfigOpt)
        spark.sparkContext.setCheckpointDir(tempLocation)
        spark.sparkContext.setLogLevel("ERROR")

        // uncomment for speed-up settings
        val (kNeighbours, maxIterations, stopIterations) = (3, 2, 2)
        val relaxCandidatesRange = 2 // relax landmarks found in candidates

        // uncomment for original (slower)
        //val (kNeighbours, maxIterations, stopIterations) = (5, 10, 3)
        //val relaxCandidatesRange = 0 // all landmarks must match

        val knn = SpatialKNN(boroughs)
            .setUseTableCheckpoint(false)
            .setApproximate(false)
            .setKNeighbours(kNeighbours)
            .setLandmarksFeatureCol("wkt")
            .setLandmarksRowID("landmark_id")
            .setCandidatesFeatureCol("wkt")
            .setCandidatesRowID("candidate_id")
            .setMaxIterations(maxIterations)
            .setEarlyStopIterations(stopIterations)
            // note this is CRS specific
            .setDistanceThreshold(distanceThreshold)
            .setIndexResolution(resolution)
            .setCheckpointTablePrefix("checkpoint_table_knn")

        info("... starting knn matches [no-approximation]")
        val matches = knn
            .transform(boroughs)
            .withColumn("left_hash", hash(col("wkt")))
            .withColumn("right_hash", hash(col("right_wkt")))
            .select("wkt_wkt_distance", "iteration", "landmark_id", "candidate_id", "neighbour_number")
            .collect()

        matches.map(r => r.getDouble(0)).max should be <= distanceThreshold // wkt_wkt_distance
        matches.map(r => r.getInt(1)).max should be <= maxIterations // iteration
        matches.map(r => r.getInt(4)).max should be  <= kNeighbours // neighbour_number
        matches.map(r => r.getLong(2)).distinct.length should be (boroughs.count()) // landmarks_miid
        relaxCandidatesRange match {
            // candidates_miid
            case n if n > 0 =>
                val minus = boroughs.count() - n
                val len = matches.map (r => r.getLong (3)).distinct.length
                len >= minus && len <= boroughs.count() should be(true)
            case _ =>
                matches.map (r => r.getLong (3)).distinct.length should be(boroughs.count())
        }

        noException should be thrownBy knn.getParams
        noException should be thrownBy knn.getMetrics

        info("... starting knn write [no-approximation]")
        knn.write
            .overwrite()
            .save(s"$tempLocation/knn")

        info("... starting knn load [no-approximation]")
        val loadedKnn = SpatialKNN.load(s"$tempLocation/knn")
        knn.getParams should contain theSameElementsAs loadedKnn.getParams
    }

    def behaviorApproximate(mosaicContext: MosaicContext, spark: SparkSession): Unit = {
        val sc = spark
        import sc.implicits._
        sc.sparkContext.setLogLevel("ERROR")

        // init
        val mc = mosaicContext
        mc.register(sc)
        import mc.functions._


        val (resolution, distanceThreshold) = mc.getIndexSystem match {
            case H3IndexSystem  => (3, 100.0)
            case BNGIndexSystem => (-3, 10000.0)
            case _ => (3, 100.0)
        }

        if (mc.getIndexSystem.name.startsWith("CUSTOM")) {
            // Skip the KNN tests for custom grid
            // TODO: Fix this
            return
        }

        val boroughs: DataFrame = getBoroughs(mc)
        val exprConfigOpt = Option(ExprConfig(sc))
        val tempLocation = MosaicContext.createTmpContextDir(exprConfigOpt)
        spark.sparkContext.setCheckpointDir(tempLocation)
        spark.sparkContext.setLogLevel("ERROR")

        val knn = SpatialKNN(boroughs)
            .setUseTableCheckpoint(false)
            .setApproximate(true)
            .setKNeighbours(5)
            .setLandmarksFeatureCol("wkt")
            .setLandmarksRowID("landmark_id")
            .setCandidatesFeatureCol("wkt")
            .setCandidatesRowID("candidate_id")
            .setMaxIterations(10)
            .setEarlyStopIterations(3)
            // note this is CRS specific
            .setDistanceThreshold(distanceThreshold)
            .setIndexResolution(resolution)
            .setCheckpointTablePrefix(tempLocation)

        info("... starting knn matches [behavior-approximation]")
        val matches = knn
            .transform(boroughs)
            .withColumn("left_hash", hash(col("wkt")))
            .withColumn("right_hash", hash(col("right_wkt")))
            .select("wkt_wkt_distance", "iteration", "landmark_id", "candidate_id", "neighbour_number")
            .collect()

        matches.map(r => r.getDouble(0)).max should be <= distanceThreshold // wkt_wkt_distance
        matches.map(r => r.getInt(1)).max should be <= 10 // iteration
        matches.map(r => r.getLong(2)).distinct.length should be(boroughs.count()) // landmarks_miid
        matches.map(r => r.getLong(3)).distinct.length should be(boroughs.count()) // candidates_miid
        matches.map(r => r.getInt(4)).max should be  <= 5 // neighbour_number

        noException should be thrownBy knn.getParams
        noException should be thrownBy knn.getMetrics

        info("... starting knn write [behavior-approximation]")
        knn.write
            .overwrite()
            .save(s"$tempLocation/knn")

        info("... starting knn load [behavior-approximation]")
        val loadedKnn = SpatialKNN.read.load(s"$tempLocation/knn")

        knn.getParams should contain theSameElementsAs loadedKnn.getParams
    }

}
