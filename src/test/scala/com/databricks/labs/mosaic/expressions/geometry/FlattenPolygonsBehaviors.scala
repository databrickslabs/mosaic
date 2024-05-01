package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.types.{HexType, InternalGeometryType}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.getWKTRowsDf
import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.locationtech.jts.io.{WKBReader, WKTReader}
import org.scalatest.matchers.should.Matchers._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

trait FlattenPolygonsBehaviors extends QueryTest {

    def flattenWKBPolygon(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

        val df = getWKTRowsDf().withColumn("wkb", convert_to(col("wkt"), "wkb"))

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

    def flattenWKTPolygon(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

        val df = getWKTRowsDf()

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

    def flattenCOORDSPolygon(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

        val df = getWKTRowsDf()
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

    def flattenHEXPolygon(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

        val df = getWKTRowsDf()
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

    def failDataTypeCheck(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

        val df = getWKTRowsDf()
            .withColumn("hex", struct(lit(1), convert_to(col("wkt"), "hex")))

        an[AnalysisException] should be thrownBy {
            df.withColumn(
              "hex",
              flatten_polygons(col("hex"))
            ).select("hex")
        }

    }

    def auxiliaryMethods(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

        val df = getWKTRowsDf()
            .withColumn("hex", convert_to(col("wkt"), "hex"))
            .withColumn("wkb", convert_to(col("wkt"), "wkb"))
            .withColumn("coords", convert_to(col("wkt"), "coords"))

        val wktFlatten = FlattenPolygons(df.col("wkt").expr, geometryAPI.name)
        val wkbFlatten = FlattenPolygons(df.col("wkb").expr, geometryAPI.name)
        val hexFlatten = FlattenPolygons(df.col("hex").expr, geometryAPI.name)
        val coordsFlatten = FlattenPolygons(df.col("coords").expr, geometryAPI.name)
        val structFlatten = FlattenPolygons(struct(lit(1), df.col("coords")).expr, geometryAPI.name)

        wktFlatten.elementSchema.head.dataType shouldEqual StringType
        wkbFlatten.elementSchema.head.dataType shouldEqual BinaryType
        hexFlatten.elementSchema.head.dataType shouldEqual HexType
        coordsFlatten.elementSchema.head.dataType shouldEqual InternalGeometryType
        an [Error] should be thrownBy structFlatten.elementSchema

        wktFlatten.collectionType shouldEqual StringType
        wkbFlatten.collectionType shouldEqual BinaryType
        hexFlatten.collectionType shouldEqual HexType
        coordsFlatten.collectionType shouldEqual InternalGeometryType

        wktFlatten.position shouldEqual false
        wkbFlatten.position shouldEqual false
        hexFlatten.position shouldEqual false
        coordsFlatten.position shouldEqual false

        wktFlatten.inline shouldEqual false
        wkbFlatten.inline shouldEqual false
        hexFlatten.inline shouldEqual false
        coordsFlatten.inline shouldEqual false

        noException should be thrownBy wktFlatten.makeCopy(Array(wktFlatten.child))
        noException should be thrownBy wkbFlatten.makeCopy(Array(wktFlatten.child))
        noException should be thrownBy hexFlatten.makeCopy(Array(wktFlatten.child))
        noException should be thrownBy coordsFlatten.makeCopy(Array(wktFlatten.child))

    }

}
