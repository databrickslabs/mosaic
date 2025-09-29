package com.databricks.labs.gbx.vectorx.jts

import com.databricks.labs.gbx.udfs
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, StructField, StructType}
import org.locationtech.jts.geom.Coordinate
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.collection.mutable

class ST_LegacyTest extends PlanTest with SilentSparkSession {

    test("ST legacy operations should work") {
        val sp = spark
        import com.databricks.labs.gbx.vectorx.jts.legacy.functions
        import com.databricks.labs.gbx.udfs._
        import sp.implicits._
        functions.register(spark)
        import functions._

        val schema = StructType(
          Seq(
            StructField(
              "geom",
              StructType(
                Seq(
                  StructField("typeId", IntegerType, nullable = false),
                  StructField("srid", IntegerType, nullable = false),
                  // boundaries: Array[Array[InternalCoord]] -> array<array<array<double>>>
                  StructField(
                    "boundaries",
                    ArrayType(ArrayType(ArrayType(DoubleType, containsNull = false), containsNull = false), containsNull = false),
                    nullable = false
                  ),
                  // holes: Array[Array[Array[InternalCoord]]] -> array<array<array<array<double>>>>
                  StructField(
                    "holes",
                    ArrayType(
                      ArrayType(ArrayType(ArrayType(DoubleType, containsNull = false), containsNull = false), containsNull = false),
                      containsNull = false
                    ),
                    nullable = false
                  )
                )
              )
            )
          )
        )

        type Coord = Seq[Double]
        type Ring = Seq[Coord]
        type Bounds = Seq[Ring]
        type Holes = Seq[Seq[Ring]]

        def coord(x: Double, y: Double): Coord = Seq(x, y)
        val srid = 4326

        val pointRow = Row(
          Row(
            GeometryTypeEnum.POINT.id,
            srid,
            Seq(Seq(Seq(coord(-0.1278, 51.5074): _*))),
            Seq.empty[Seq[Seq[Seq[Double]]]]
          )
        )

        val multiPointRow = Row(
          Row(
            GeometryTypeEnum.MULTIPOINT.id,
            srid,
            Seq(Seq(Seq(coord(-3.7038, 40.4168): _*), Seq(coord(2.1734, 41.3851): _*), Seq(coord(13.4050, 52.5200): _*))),
            Seq.empty[Seq[Seq[Seq[Double]]]]
          )
        )

        val lineStringRow = Row(
          Row(
            GeometryTypeEnum.LINESTRING.id,
            srid,
            Seq(Seq(Seq(coord(-122.42, 37.77): _*), Seq(coord(-118.24, 34.05): _*), Seq(coord(-115.14, 36.17): _*))),
            Seq.empty[Seq[Seq[Seq[Double]]]]
          )
        )

        val multiLineStringRow = Row(
          Row(
            GeometryTypeEnum.MULTILINESTRING.id,
            srid,
            Seq(
              Seq(Seq(coord(-0.2, 51.5): _*), Seq(coord(-0.1, 51.51): _*), Seq(coord(0.0, 51.52): _*)),
              Seq(Seq(coord(0.1, 51.49): _*), Seq(coord(0.2, 51.48): _*))
            ),
            Seq.empty[Seq[Seq[Seq[Double]]]]
          )
        )

        val polyRing: Ring = Seq(coord(-1, 51), coord(0, 51), coord(0, 52), coord(-1, 52), coord(-1, 51))
        val polygonRow = Row(
          Row(
            GeometryTypeEnum.POLYGON.id,
            srid,
            Seq(Seq(polyRing: _*)),
            Seq(Seq.empty[Ring])
          )
        )

        val ring1: Ring = Seq(coord(1, 51), coord(2, 51), coord(2, 52), coord(1, 52), coord(1, 51))
        val ring2: Ring = Seq(coord(2.5, 51.2), coord(3.0, 51.2), coord(3.0, 51.7), coord(2.5, 51.7), coord(2.5, 51.2))
        val multiPolygonRow = Row(
          Row(
            GeometryTypeEnum.MULTIPOLYGON.id,
            srid,
            Seq(Seq(ring1: _*), Seq(ring2: _*)),
            Seq(Seq.empty[Ring], Seq.empty[Ring])
          )
        )

        val rows = Seq(pointRow, multiPointRow, lineStringRow, multiLineStringRow, polygonRow, multiPolygonRow)
        val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

        noException should be thrownBy {
            df.select(st_legacyaswkb(col("geom"))).collect()
        }

        df.select(
          udfs.st_type(
            st_legacyaswkb(col("geom"))
          ).alias("geom_type")
        ).distinct()
            .count() shouldBe 6L

    }

}
