package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.expressions.accessors.RST_BoundingBox
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.GDALTranslate
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.CollectionHasAsScala

class RST_ExpressionExecuteTest extends AnyFunSuite with BeforeAndAfterAll {

    var ds: Dataset = _

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp")
        gdal.AllRegister()
        val tifPath = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").toString.replace("file:/", "/")
        ds = gdal.Open(tifPath)
    }

    override def afterAll(): Unit = {
        ds.delete()
    }

    test("RST_AsFormat should convert raster to specified format") {
        val (convertedDs, _) = RST_AsFormat.execute(ds, Map("TILED" -> "YES"), "PNM")
        convertedDs should not be null
        convertedDs.GetDriver().getShortName shouldBe "PNM"
        RasterDriver.releaseDataset(convertedDs)
    }

    test("RST_Clip should clip raster to specified bounds") {
        val geom = RST_BoundingBox.execute(ds).buffer(-200000)
        val (clippedDs, _) = RST_Clip.execute(ds, Map.empty, geom, cutlineAllTouched = true)
        clippedDs should not be null
        clippedDs.getRasterXSize should be <= ds.getRasterXSize
        clippedDs.getRasterYSize should be <= ds.getRasterYSize
        RasterDriver.releaseDataset(clippedDs)
    }

    test("RST_CombineAvg should combine rasters by average") {
        val (_, combinedDs, _) = RST_CombineAvg.execute(Seq((1L, ds, Map.empty), (1L, ds, Map.empty)))
        combinedDs should not be null
        combinedDs.getRasterCount shouldBe ds.getRasterCount
        combinedDs.getRasterXSize shouldBe ds.getRasterXSize
        combinedDs.getRasterYSize shouldBe ds.getRasterYSize
        RasterDriver.releaseDataset(combinedDs)
    }

    test("RST_Convolve should apply convolution kernel to raster") {
        val miniDsPath = "/tmp/mini.tif"
        val (miniDs, _) = GDALTranslate.executeTranslate(miniDsPath, ds, "gdal_translate -outsize 10% 10% -r bilinear", Map.empty)
        val kernel = Array(
          Array(0.0, -1.0, 0.0),
          Array(-1.0, 5.0, -1.0),
          Array(0.0, -1.0, 0.0)
        )
        val (convolvedDs, _) = RST_Convolve.execute((1L, miniDs, Map.empty), kernel)
        convolvedDs should not be null
        convolvedDs.getRasterCount shouldBe miniDs.getRasterCount
        convolvedDs.getRasterXSize shouldBe miniDs.getRasterXSize
        convolvedDs.getRasterYSize shouldBe miniDs.getRasterYSize
        RasterDriver.releaseDataset(convolvedDs)
        RasterDriver.releaseDataset(miniDs)
        Files.deleteIfExists(Paths.get(miniDsPath))
    }

    test("RST_DerivedBand should compute derived band from raster") {
        val pyfunc = "def compute(pixel):\n  return pixel[0] * 2"
        val (derivedDs, _) = RST_DerivedBand.execute(Seq(ds, ds), Map.empty, pyfunc, "compute")
        derivedDs != null shouldBe true
        derivedDs.getRasterCount == ds.getRasterCount shouldBe true
        derivedDs.getRasterXSize == ds.getRasterXSize shouldBe true
        derivedDs.getRasterYSize == ds.getRasterYSize shouldBe true
        RasterDriver.releaseDataset(derivedDs)
    }

    test("RST_Filter should apply filter to raster") {
        val miniDsPath = "/tmp/mini.tif"
        val (miniDs, _) = GDALTranslate.executeTranslate(miniDsPath, ds, "gdal_translate -outsize 10% 10% -r bilinear", Map.empty)
        val (filteredDs, _) = RST_Filter.execute(miniDs, 3, "min")
        filteredDs should not be null
        filteredDs.getRasterCount shouldBe miniDs.getRasterCount
        filteredDs.getRasterXSize shouldBe miniDs.getRasterXSize
        filteredDs.getRasterYSize shouldBe miniDs.getRasterYSize
        RasterDriver.releaseDataset(filteredDs)
        RasterDriver.releaseDataset(miniDs)
        Files.deleteIfExists(Paths.get(miniDsPath))
    }

    test("RST_InitNoData should initialize NoData values in raster") {
        val (noDataDs, _) = RST_InitNoData.execute(ds, Map.empty)
        noDataDs should not be null
        noDataDs.GetRasterCount shouldBe ds.GetRasterCount
        noDataDs.getRasterXSize shouldBe ds.getRasterXSize
        noDataDs.getRasterYSize shouldBe ds.getRasterYSize
        noDataDs.delete()
    }

    test("RST_IsEmpty should check if raster is empty") {
        val isEmpty = RST_IsEmpty.execute(ds)
        isEmpty shouldBe false
    }

    test("RST_MapAlgebra should apply map algebra expression to raster") {
        val (mappedDs, _) = RST_MapAlgebra.execute(Seq(ds, ds, ds), Map.empty, "{\"calc\": \"A+B/C\"}")
        mappedDs should not be null
        mappedDs.getRasterCount shouldBe ds.getRasterCount
        mappedDs.getRasterXSize shouldBe ds.getRasterXSize
        mappedDs.getRasterYSize shouldBe ds.getRasterYSize
        val tmpFiles = mappedDs.GetFileList().asScala.toSeq.map(_.toString)
        RasterDriver.releaseDataset(mappedDs)
        tmpFiles.foreach(f => Files.deleteIfExists(Paths.get(f)))
    }

    test("RST_Merge should merge multiple rasters") {
        val (mergedDs, _) = RST_Merge.execute(Array(ds, ds, ds), Map.empty)
        mergedDs should not be null
        mergedDs.getRasterCount shouldBe ds.getRasterCount
        mergedDs.getRasterXSize should be >= ds.getRasterXSize
        mergedDs.getRasterYSize should be >= ds.getRasterYSize
        RasterDriver.releaseDataset(mergedDs)
    }

    test("RST_NDVI should compute NDVI from raster") {
        val (ndviDs, _) = RST_NDVI.execute(ds, 1, 1, Map.empty)
        ndviDs should not be null
        ndviDs.getRasterCount shouldBe 1
        ndviDs.getRasterXSize shouldBe ds.getRasterXSize
        ndviDs.getRasterYSize shouldBe ds.getRasterYSize
        val tmpFiles = ndviDs.GetFileList().asScala.toSeq.map(_.toString)
        RasterDriver.releaseDataset(ndviDs)
        tmpFiles.foreach(f => Files.deleteIfExists(Paths.get(f)))
    }

    test("RST_RasterToWorldCoord should convert raster to world coordinates") {
        val (x, y) = RST_RasterToWorldCoord.execute(ds, 10000, 10000)
        x should not be 0.0
        y should not be 0.0
    }

    test("RST_RasterToWorldCoordX should convert raster to world X coordinate") {
        val x = RST_RasterToWorldCoordX.execute(ds, 10000, 10000)
        x should not be 0.0
    }

    test("RST_RasterToWorldCoordY should convert raster to world Y coordinate") {
        val y = RST_RasterToWorldCoordY.execute(ds, 10000, 10000)
        y should not be 0.0
    }

    test("RST_Transform should transform raster with given options") {
        val (transformedDs, _) = RST_Transform.execute(ds, Map.empty, 4326)
        transformedDs should not be null
        transformedDs.getRasterCount shouldBe ds.getRasterCount
        transformedDs.delete()
    }

    test("RST_UpdateType should update raster data type") {
        val (updatedDs, _) = RST_UpdateType.execute(ds, Map.empty, "Float32")
        updatedDs should not be null
        updatedDs.GetRasterCount shouldBe ds.GetRasterCount
        updatedDs.getRasterXSize shouldBe ds.getRasterXSize
        updatedDs.getRasterYSize shouldBe ds.getRasterYSize
        updatedDs.delete()
    }

    test("RST_WorldToRasterCoord should convert world coordinates to raster") {
        val (x, y) = RST_WorldToRasterCoord.execute(ds, 0.0, 0.0)
        x should be >= 0
        y should be >= 0
    }

    test("RST_WorldToRasterCoordX should convert world X coordinate to raster") {
        val x = RST_WorldToRasterCoordX.execute(ds, 0.0, 0.0)
        x should be >= 0
    }

    test("RST_WorldToRasterCoordY should convert world Y coordinate to raster") {
        val y = RST_WorldToRasterCoordY.execute(ds, 0.0, 0.0)
        y should be >= 0
    }

}
