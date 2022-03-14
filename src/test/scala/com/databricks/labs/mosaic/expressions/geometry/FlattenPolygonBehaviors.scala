package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.getWKTRowsDf
import org.locationtech.jts.io.{WKBReader, WKTReader}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

trait FlattenPolygonBehaviors { this: AnyFlatSpec =>

    def flattenWKBPolygon(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val df = getWKTRowsDf.withColumn("wkb", convert_to(col("wkt"), "wkb"))

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

        val df = getWKTRowsDf

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

        val df = getWKTRowsDf
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

        val df = getWKTRowsDf
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

}
