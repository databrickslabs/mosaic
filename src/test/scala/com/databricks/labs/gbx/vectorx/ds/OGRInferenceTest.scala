package com.databricks.labs.gbx.vectorx.ds

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.vectorx.ds.ogr.OGR_SchemaInference
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.gdal.ogr.{Feature, FieldDefn, ogr, ogrConstants}
import org.gdal.osr.SpatialReference
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.sql.Date
import java.time.Instant
import java.util

class OGRInferenceTest extends AnyFunSuite {

    test("OGR_SchemaInference should enable ogr drivers") {
        noException shouldBe thrownBy {
            GDALManager.loadSharedObjects(Seq.empty)
            GDALManager.configureGDAL("/tmp", "/tmp")
            ogr.RegisterAll()
        }
    }

    test("OGR_SchemaInference should return correct data type") {
        OGR_SchemaInference.getType("Boolean") shouldBe BooleanType
        OGR_SchemaInference.getType("Integer") shouldBe IntegerType
        OGR_SchemaInference.getType("String") shouldBe StringType
        OGR_SchemaInference.getType("Real") shouldBe DoubleType
        OGR_SchemaInference.getType("Date") shouldBe DateType
        OGR_SchemaInference.getType("Time") shouldBe TimestampType
        OGR_SchemaInference.getType("DateTime") shouldBe TimestampType
        OGR_SchemaInference.getType("Binary") shouldBe BinaryType
        OGR_SchemaInference.getType("IntegerList") shouldBe ArrayType(IntegerType)
        OGR_SchemaInference.getType("RealList") shouldBe ArrayType(DoubleType)
        OGR_SchemaInference.getType("StringList") shouldBe ArrayType(StringType)
        OGR_SchemaInference.getType("WideString") shouldBe StringType
        OGR_SchemaInference.getType("WideStringList") shouldBe ArrayType(StringType)
        OGR_SchemaInference.getType("Integer64") shouldBe LongType
        OGR_SchemaInference.getType("Unknown") shouldBe StringType
    }

    test("OGR_SchemaInference should coerce types") {
        OGR_SchemaInference.coerceTypeList(Seq(IntegerType, LongType)) shouldBe LongType
        OGR_SchemaInference.coerceTypeList(Seq(IntegerType, DoubleType)) shouldBe DoubleType
        OGR_SchemaInference.coerceTypeList(Seq(StringType, DoubleType)) shouldBe StringType
        OGR_SchemaInference.coerceTypeList(Seq(IntegerType, FloatType)) shouldBe FloatType
        OGR_SchemaInference.coerceTypeList(Seq(ShortType, IntegerType)) shouldBe IntegerType
        OGR_SchemaInference.coerceTypeList(Seq(ShortType, ShortType)) shouldBe ShortType
        OGR_SchemaInference.coerceTypeList(Seq(BinaryType, ByteType)) shouldBe BinaryType
        OGR_SchemaInference.coerceTypeList(Seq(ByteType, ByteType)) shouldBe ByteType
        OGR_SchemaInference.coerceTypeList(Seq(BooleanType)) shouldBe BooleanType
        OGR_SchemaInference.coerceTypeList(Seq(DateType, TimestampType)) shouldBe TimestampType
        OGR_SchemaInference.coerceTypeList(Seq(DateType)) shouldBe DateType
        OGR_SchemaInference.coerceTypeList(Seq()) shouldBe StringType
    }

    test("OGR_SchemaInference should get a field value") {
        GDALManager.loadSharedObjects(Seq.empty)
        GDALManager.configureGDAL("/tmp", "/tmp")
        ogr.RegisterAll()

        val drv = ogr.GetDriverByName("Memory")
        val ds = drv.CreateDataSource("mem")
        val srs = new SpatialReference(); srs.ImportFromEPSG(4326)
        val lyr = ds.CreateLayer("t", srs, ogrConstants.wkbUnknown)

        // --- schema
        lyr.CreateField(new FieldDefn("i_arr", ogrConstants.OFTIntegerList))
        lyr.CreateField(new FieldDefn("r_arr", ogrConstants.OFTRealList))
        lyr.CreateField(new FieldDefn("s_arr", ogrConstants.OFTStringList))
        lyr.CreateField(new FieldDefn("bin", ogrConstants.OFTBinary))
        lyr.CreateField(new FieldDefn("d_only", ogrConstants.OFTDate))
        lyr.CreateField(new FieldDefn("dt", ogrConstants.OFTDateTime))
        lyr.CreateField(new FieldDefn("int", ogrConstants.OFTInteger))
        lyr.CreateField(new FieldDefn("long", ogrConstants.OFTInteger64))
        lyr.CreateField(new FieldDefn("real", ogrConstants.OFTReal))
        lyr.CreateField(new FieldDefn("str", ogrConstants.OFTString))

        val fdef = lyr.GetLayerDefn()
        val f = new Feature(fdef)

        val strVector = new util.Vector[String]()
        strVector.add("a")
        strVector.add("b")

        // --- set values
        f.SetFieldIntegerList(0, Array(1, 2, 3)) // int[]
        f.SetFieldDoubleList(1, Array(1.5, 2.5)) // double[]
        f.SetFieldStringList(2, strVector) // String[]
        f.SetFieldBinaryFromHexString(3, "01020304") // writes 0x01 0x02 0x03 0x04
        f.SetField(4, 2025, 9, 15, 0, 0, 0f, 0) // OFTDate
        f.SetField(5, 2025, 9, 15, 12, 34, 56f, 100) // OFTDateTime (100 = UTC)
        f.SetField(6, 123) // int
        f.SetField(7, 1234567890123L) // long
        f.SetField(8, 1.2345) // double
        f.SetField(9, "hello") // string

        // Write
        lyr.CreateFeature(f)

        OGR_SchemaInference.getValue(f, 0, ArrayType(IntegerType)) shouldBe Array(1, 2, 3)
        OGR_SchemaInference.getValue(f, 1, ArrayType(DoubleType)) shouldBe Array(1.5, 2.5)
        OGR_SchemaInference.getValue(f, 2, ArrayType(StringType)) shouldBe Array("a", "b")
        OGR_SchemaInference.getValue(f, 3, BinaryType) shouldBe Array(1.toByte, 2.toByte, 3.toByte, 4.toByte)
        OGR_SchemaInference.getValue(f, 4, DateType) shouldBe DateTimeUtils.fromJavaDate(Date.valueOf("2025-09-15"))
        OGR_SchemaInference.getValue(f, 5, TimestampType) shouldBe DateTimeUtils.instantToMicros(Instant.parse("2025-09-15T12:34:56Z"))
        OGR_SchemaInference.getValue(f, 6, IntegerType) shouldBe 123
        OGR_SchemaInference.getValue(f, 7, LongType) shouldBe 1234567890123L
        OGR_SchemaInference.getValue(f, 8, DoubleType) shouldBe 1.2345
        OGR_SchemaInference.getValue(f, 9, StringType) shouldBe "hello"

    }

}
