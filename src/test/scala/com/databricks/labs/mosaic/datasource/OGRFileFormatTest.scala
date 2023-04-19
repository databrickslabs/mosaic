package com.databricks.labs.mosaic.datasource

import com.databricks.labs.mosaic.expressions.util.OGRReadeWithOffset
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.gdal.ogr.{ogr, Feature}
import org.scalatest.matchers.must.Matchers.{be, noException}
import org.scalatest.matchers.should.Matchers.{an, convertToAnyShouldWrapper}

class OGRFileFormatTest extends QueryTest with SharedSparkSession {

    test("Read open geoDB with OGRFileFormat") {
        assume(System.getProperty("os.name") == "Linux")

        val geodb = "/binary/geodb/"
        val filePath = getClass.getResource(geodb).getPath

        noException should be thrownBy spark.read
            .format("ogr")
            .option("vsizip", "true")
            .load(filePath)
            .take(1)

        noException should be thrownBy spark.read
            .format("ogr")
            .option("driverName", "OpenFileGDB")
            .option("vsizip", "true")
            .option("asWKB", "true")
            .load(filePath)
            .take(1)

        noException should be thrownBy spark.read
            .format("ogr")
            .option("driverName", "OpenFileGDB")
            .option("vsizip", "true")
            .option("asWKB", "true")
            .load(filePath)
            .select("SHAPE_srid")
            .take(1)

    }

    test("Read shapefile with OGRFileFormat") {
        assume(System.getProperty("os.name") == "Linux")

        val shapefile = "/binary/shapefile/"
        val filePath = getClass.getResource(shapefile).getPath

        noException should be thrownBy spark.read
            .format("ogr")
            .load(filePath)
            .take(1)

        noException should be thrownBy spark.read
            .format("ogr")
            .option("driverName", "ESRI Shapefile")
            .option("asWKB", "true")
            .load(filePath)
            .take(1)

        noException should be thrownBy spark.read
            .format("ogr")
            .option("driverName", "ESRI Shapefile")
            .option("asWKB", "true")
            .load(filePath)
            .select("geom_0_srid")
            .take(1)

    }

    test("OGRFileFormat utility tests") {
        assume(System.getProperty("os.name") == "Linux")
        val reader = new OGRFileFormat()
        an[Error] should be thrownBy reader.prepareWrite(spark, null, null, null)

        noException should be thrownBy OGRFileFormat.enableOGRDrivers(force = true)

        val path = getClass.getResource("/binary/geodb/bridges.gdb.zip").getPath.replace("file:", "")
        val ds = ogr.Open(s"/vsizip/$path", 0)

        noException should be thrownBy OGRFileFormat.getLayer(ds, 0, "layer2")

        OGRFileFormat.getType("Boolean").typeName should be("boolean")
        OGRFileFormat.getType("Integer").typeName should be("integer")
        OGRFileFormat.getType("String").typeName should be("string")
        OGRFileFormat.getType("Real").typeName should be("double")
        OGRFileFormat.getType("Date").typeName should be("date")
        OGRFileFormat.getType("Time").typeName should be("timestamp")
        OGRFileFormat.getType("DateTime").typeName should be("timestamp")
        OGRFileFormat.getType("Binary").typeName should be("binary")
        OGRFileFormat.getType("IntegerList").typeName should be("array")
        OGRFileFormat.getType("RealList").typeName should be("array")
        OGRFileFormat.getType("StringList").typeName should be("array")
        OGRFileFormat.getType("WideString").typeName should be("string")
        OGRFileFormat.getType("WideStringList").typeName should be("array")
        OGRFileFormat.getType("Integer64").typeName should be("long")
        OGRFileFormat.getType("Integer64List").typeName should be("string")

        OGRFileFormat.coerceTypeList(Seq(DoubleType, LongType)).typeName should be("long")
        OGRFileFormat.coerceTypeList(Seq(IntegerType, LongType)).typeName should be("long")
        OGRFileFormat.coerceTypeList(Seq(IntegerType, DoubleType)).typeName should be("double")
        OGRFileFormat.coerceTypeList(Seq(IntegerType, StringType)).typeName should be("integer")
        OGRFileFormat.coerceTypeList(Seq(StringType, ShortType)).typeName should be("short")
        OGRFileFormat.coerceTypeList(Seq(StringType, ByteType)).typeName should be("byte")
        OGRFileFormat.coerceTypeList(Seq(StringType, BinaryType)).typeName should be("binary")
        OGRFileFormat.coerceTypeList(Seq(StringType, StringType)).typeName should be("string")
        OGRFileFormat.coerceTypeList(Seq(StringType, DateType)).typeName should be("date")
        OGRFileFormat.coerceTypeList(Seq(StringType, BooleanType)).typeName should be("boolean")
        OGRFileFormat.coerceTypeList(Seq(StringType, TimestampType)).typeName should be("timestamp")

        val feature = ds.GetLayer(0).GetNextFeature()
        noException should be thrownBy OGRFileFormat.getFieldIndex(feature, "SHAPE")
        noException should be thrownBy OGRFileFormat.getFieldIndex(feature, "field1")
        noException should be thrownBy OGRFileFormat.getDate(feature, 1)
        OGRReadeWithOffset(
          null,
          null,
          Map("driverName" -> "", "layerNumber" -> "1", "chunkSize" -> "200", "vsizip" -> "false", "layerName" -> "", "asWKB" -> "false"),
          null
        ).position should be(false)
    }

    test("OGRFileFormat should handle NULL geometries: ISSUE 343") {
        assume(System.getProperty("os.name") == "Linux")
        OGRFileFormat.enableOGRDrivers(force = true)

        val shapefile = "/binary/shapefile/"
        val filePath = getClass.getResource(shapefile).getPath
        val ds = ogr.Open(filePath + "map.shp")

        val feature1 = ds.GetLayer(0).GetNextFeature()
        val testFeature = feature1
        testFeature.SetGeomField(0, null)
        val schema = OGRFileFormat.inferSchemaImpl("", filePath, Map("driverName" -> "ESRI Shapefile", "asWKB" -> "true")).get

        noException should be thrownBy
            OGRFileFormat.getFeatureFields(testFeature, schema, asWKB = true)
    }

}
