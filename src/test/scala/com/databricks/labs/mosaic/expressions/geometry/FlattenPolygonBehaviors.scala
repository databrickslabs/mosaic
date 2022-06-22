package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.types.{HexType, InternalGeometryType}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.getWKTRowsDf
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.locationtech.jts.io.{WKBReader, WKTReader}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

trait FlattenPolygonBehaviors { this: AnyFlatSpec =>

    def flattenWKBPolygon(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val df = getWKTRowsDf(mc).withColumn("wkb", convert_to(col("wkt"), "wkb"))

        val flattened = df
            .withColumn(
              "wkb",
              flatten_polygons(col("wkb"))
            )
            .select("wkb")

        val geoms = df
            .select("wkb")
            .collect()
            .map(g => new WKBReader().read(g.get(0).asInstanceOf[Array[Byte]]))
            .flatMap(g => for (i <- 0 until g.getNumGeometries) yield g.getGeometryN(i))

        val flattenedGeoms = flattened
            .collect()
            .map(g => new WKBReader().read(g.get(0).asInstanceOf[Array[Byte]]))

        flattenedGeoms.zip(geoms).foreach { case (l, r) => l.equals(r) shouldEqual true }

        val flattenedGeoms2 = df
            .select(st_dump(col("wkb")))
            .collect()
            .map(g => new WKBReader().read(g.get(0).asInstanceOf[Array[Byte]]))

        flattenedGeoms2.zip(geoms).foreach { case (l, r) => l.equals(r) shouldEqual true }

        df.createOrReplaceTempView("source")
        val sqlFlattenedGeoms = spark
            .sql("select flatten_polygons(wkb) from source")
            .collect()
            .map(g => new WKBReader().read(g.get(0).asInstanceOf[Array[Byte]]))

        sqlFlattenedGeoms.zip(geoms).foreach { case (l, r) => l.equals(r) shouldEqual true }

        val sqlFlattenedGeoms2 = spark
            .sql("select st_dump(wkb) from source")
            .collect()
            .map(g => new WKBReader().read(g.get(0).asInstanceOf[Array[Byte]]))

        sqlFlattenedGeoms2.zip(geoms).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def flattenWKTPolygon(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val df = getWKTRowsDf(mc)

        val flattened = df
            .withColumn(
              "wkt",
              flatten_polygons(col("wkt"))
            )
            .select("wkt")

        val geoms = df
            .select("wkt")
            .collect()
            .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))
            .flatMap(g => for (i <- 0 until g.getNumGeometries) yield g.getGeometryN(i))

        val flattenedGeoms = flattened
            .collect()
            .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))

        flattenedGeoms.zip(geoms).foreach { case (l, r) => l.equals(r) shouldEqual true }

        val flattenedGeoms2 = df
            .select(st_dump(col("wkt")))
            .collect()
            .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))

        flattenedGeoms2.zip(geoms).foreach { case (l, r) => l.equals(r) shouldEqual true }

        df.createOrReplaceTempView("source")
        val sqlFlattenedGeoms = spark
            .sql("select flatten_polygons(wkt) from source")
            .collect()
            .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))

        sqlFlattenedGeoms.zip(geoms).foreach { case (l, r) => l.equals(r) shouldEqual true }

        val sqlFlattenedGeoms2 = spark
            .sql("select st_dump(wkt) from source")
            .collect()
            .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))

        sqlFlattenedGeoms2.zip(geoms).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def flattenCOORDSPolygon(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val df = getWKTRowsDf(mc)
            .withColumn("coords", convert_to(col("wkt"), "coords"))

        val flattened = df
            .withColumn(
              "coords",
              flatten_polygons(col("coords"))
            )
            .select("coords")

        val geoms = df
            .withColumn("wkt", convert_to(col("coords"), "wkt"))
            .select("wkt")
            .collect()
            .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))
            .flatMap(g => for (i <- 0 until g.getNumGeometries) yield g.getGeometryN(i))

        val flattenedGeoms = flattened
            .withColumn("wkt", convert_to(col("coords"), "wkt"))
            .select("wkt")
            .collect()
            .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))

        flattenedGeoms.zip(geoms).foreach { case (l, r) => l.equals(r) shouldEqual true }

        val flattenedGeoms2 = df
            .select(st_dump(col("coords")).alias("coords"))
            .select(convert_to(col("coords"), "wkt"))
            .collect()
            .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))

        flattenedGeoms2.zip(geoms).foreach { case (l, r) => l.equals(r) shouldEqual true }

        df.createOrReplaceTempView("source")
        val sqlFlattenedGeoms = spark
            .sql("""with subquery (
                   |  select flatten_polygons(coords) as coords
                   |  from source
                   |) select convert_to_wkt(coords) from subquery""".stripMargin)
            .collect()
            .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))

        sqlFlattenedGeoms.zip(geoms).foreach { case (l, r) => l.equals(r) shouldEqual true }

        val sqlFlattenedGeoms2 = spark
            .sql("""with subquery (
                   |  select st_dump(coords) as coords
                   |  from source
                   |) select convert_to_wkt(coords) from subquery""".stripMargin)
            .collect()
            .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))

        sqlFlattenedGeoms2.zip(geoms).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def flattenHEXPolygon(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val df = getWKTRowsDf(mc)
            .withColumn("hex", convert_to(col("wkt"), "hex"))

        val flattened = df
            .withColumn(
              "hex",
              flatten_polygons(col("hex"))
            )
            .select("hex")

        val geoms = df
            .withColumn("wkt", convert_to(col("hex"), "wkt"))
            .select("wkt")
            .collect()
            .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))
            .flatMap(g => for (i <- 0 until g.getNumGeometries) yield g.getGeometryN(i))

        val flattenedGeoms = flattened
            .withColumn("wkt", convert_to(col("hex"), "wkt"))
            .select("wkt")
            .collect()
            .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))

        flattenedGeoms.zip(geoms).foreach { case (l, r) => l.equals(r) shouldEqual true }

        val flattenedGeoms2 = df
            .select(st_dump(col("hex")).alias("hex"))
            .select(convert_to(col("hex"), "wkt"))
            .collect()
            .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))

        flattenedGeoms2.zip(geoms).foreach { case (l, r) => l.equals(r) shouldEqual true }

        df.createOrReplaceTempView("source")
        val sqlFlattenedGeoms = spark
            .sql("""with subquery (
                   |  select flatten_polygons(hex) as hex
                   |  from source
                   |) select convert_to_wkt(hex) from subquery""".stripMargin)
            .collect()
            .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))

        sqlFlattenedGeoms.zip(geoms).foreach { case (l, r) => l.equals(r) shouldEqual true }

        val sqlFlattenedGeoms2 = spark
            .sql("""with subquery (
                   |  select st_dump(hex) as hex
                   |  from source
                   |) select convert_to_wkt(hex) from subquery""".stripMargin)
            .collect()
            .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))

        sqlFlattenedGeoms2.zip(geoms).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def failDataTypeCheck(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val df = getWKTRowsDf(mc)
            .withColumn("hex", struct(lit(1), convert_to(col("wkt"), "hex")))

        an[AnalysisException] should be thrownBy {
            df.withColumn(
              "hex",
              flatten_polygons(col("hex"))
            ).select("hex")
        }

    }

    def auxiliaryMethods(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val df = getWKTRowsDf(mc)
            .withColumn("hex", convert_to(col("wkt"), "hex"))
            .withColumn("wkb", convert_to(col("wkt"), "wkb"))
            .withColumn("coords", convert_to(col("wkt"), "coords"))

        val wktFlatten = FlattenPolygons(df.col("wkt").expr, mosaicContext.getGeometryAPI.name)
        val wkbFlatten = FlattenPolygons(df.col("wkb").expr, mosaicContext.getGeometryAPI.name)
        val hexFlatten = FlattenPolygons(df.col("hex").expr, mosaicContext.getGeometryAPI.name)
        val coordsFlatten = FlattenPolygons(df.col("coords").expr, mosaicContext.getGeometryAPI.name)

        wktFlatten.elementSchema.head.dataType shouldEqual StringType
        wkbFlatten.elementSchema.head.dataType shouldEqual BinaryType
        hexFlatten.elementSchema.head.dataType shouldEqual HexType
        coordsFlatten.elementSchema.head.dataType shouldEqual InternalGeometryType

        wktFlatten.collectionType shouldEqual StringType
        wkbFlatten.collectionType shouldEqual BinaryType
        hexFlatten.collectionType shouldEqual HexType
        coordsFlatten.collectionType shouldEqual InternalGeometryType

        wktFlatten.position shouldEqual false
        wkbFlatten.position shouldEqual false
        hexFlatten.position shouldEqual false
        coordsFlatten.position shouldEqual false

        noException should be thrownBy wktFlatten.makeCopy(Array(wktFlatten.child))
        noException should be thrownBy wkbFlatten.makeCopy(Array(wktFlatten.child))
        noException should be thrownBy hexFlatten.makeCopy(Array(wktFlatten.child))
        noException should be thrownBy coordsFlatten.makeCopy(Array(wktFlatten.child))

    }

}
