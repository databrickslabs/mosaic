package com.databricks.labs.mosaic.core.raster.gdal

import com.databricks.labs.mosaic.NO_PATH_STRING
import com.databricks.labs.mosaic.core.raster.io.RasterIO
import com.databricks.labs.mosaic.test.mocks.filePath
import org.apache.spark.sql.test.SharedSparkSessionGDAL
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants.GA_ReadOnly
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.util.{Vector => JVector}

class TestDatasetGDAL  extends SharedSparkSessionGDAL {

    test("DatasetGDAL handles empty and null") {
        val dsGDAL = DatasetGDAL()

        dsGDAL.dataset should be(null)
        dsGDAL.getDatasetOpt should be(None)
        dsGDAL.driverNameOpt should be(None)
        dsGDAL.pathGDAL.path should be(NO_PATH_STRING)
        dsGDAL.isHydrated should be(false)
    }

    test("DatasetGDAL handles path and driver updates") {
        val p = filePath("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        info(s"path -> '$p'")

        // update path and driver
        val dsGDAL = DatasetGDAL()
        dsGDAL.updatePath(p).getPath should be(p)
        dsGDAL.updateDriverName("GTiff").getDriverName should be("GTiff")
    }

    test("Dataset loads for tif") {
        val p = filePath("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        info(s"path -> '$p'")

        // load the dataset
        val dsOpt = RasterIO.rawPathAsDatasetOpt(p, None, getExprConfigOpt)
        dsOpt.isDefined should be(true)

        val dsGDAL = DatasetGDAL()
        try {
            // set on dsGDAL
            dsGDAL.updateDataset(dsOpt.get, doUpdateDriver = true)
            dsGDAL.getDriverName should be("GTiff")
            dsGDAL.isHydrated should be(true)
            info(s"dataset description -> '${dsGDAL.dataset.GetDescription()}'")

            val raster = RasterGDAL(dsOpt.get, getExprConfigOpt, dsGDAL.asCreateInfo)
            raster.updateCreateInfoRawPath(p, skipFlag = true)
            raster.finalizeRaster(toFuse = true)

            val outFusePath = raster.getRawPath
            info(s"out fuse path -> '$outFusePath'")
            info(s"...dsGDAL createInfo: ${dsGDAL.asCreateInfo}")
            info(s"...finalizeRaster - createInfo: ${raster.getCreateInfo}")

            // set the path for use outside this block
            dsGDAL.updatePath(outFusePath)

        } finally {
            dsGDAL.flushAndDestroy()
        }

        // reload the written dataset
        RasterIO.rawPathAsDatasetOpt(dsGDAL.getPath, driverNameOpt = None, getExprConfigOpt).isDefined should be(true)
    }

    test("Dataset loads for netcdf") {
        val p = filePath("/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220101.nc")
        info(s"path -> '$p'")

        // load the dataset
        val dsOpt = RasterIO.rawPathAsDatasetOpt(p, None, getExprConfigOpt)
        dsOpt.isDefined should be(true)

        val dsGDAL = DatasetGDAL()
        try {
            // set on dsGDAL
            dsGDAL.updateDataset(dsOpt.get, doUpdateDriver = true)
            dsGDAL.getDriverName should be("netCDF")
            dsGDAL.isHydrated should be(true)
            info(s"dataset description -> '${dsGDAL.dataset.GetDescription()}'")
            info(s"subdatasets -> ${dsGDAL.subdatasets(dsGDAL.pathGDAL)}")

            val raster = RasterGDAL(dsOpt.get, getExprConfigOpt, dsGDAL.asCreateInfo)
            raster.updateCreateInfoRawPath(p, skipFlag = true)
            raster.finalizeRaster(toFuse = true)

            val outFusePath = raster.getRawPath
            info(s"out fuse path -> '$outFusePath'")
            info(s"...dsGDAL createInfo: ${dsGDAL.asCreateInfo}")
            info(s"...finalizeRaster - createInfo: ${raster.getCreateInfo}")

            // set the path for use outside this block
            dsGDAL.updatePath(outFusePath)

        } finally {
            dsGDAL.flushAndDestroy()
        }

        // reload the written dataset
        RasterIO.rawPathAsDatasetOpt(dsGDAL.getPath, None, getExprConfigOpt).isDefined should be(true)
    }

    test("Dataset loads for netcdf subdataset") {
        val p = filePath("/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220101.nc")
        val sdName = "bleaching_alert_area"
        info(s"path -> '$p'")

        val drivers = new JVector[String]() // java.util.Vector
        drivers.add("netCDF")
        val result = gdal.OpenEx(
            "NETCDF:/root/mosaic/target/test-classes/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220101.nc:bleaching_alert_area",
            GA_ReadOnly,
            drivers
        )
        result != null should be(true)
        info(s"description -> '${result.GetDescription()}'")
        //info(s"metadata -> '${result.GetMetadata_Dict()}'")

        // (1) load the subdataset
        val sp = s"$p:$sdName"
        val dsOpt = RasterIO.rawPathAsDatasetOpt(sp, None, getExprConfigOpt)
        dsOpt.isDefined should be(true)

        val dsGDAL = DatasetGDAL()
        try {
            // set on dsGDAL
            dsGDAL.updateDataset(dsOpt.get, doUpdateDriver = true)
            dsGDAL.getDriverName should be("netCDF")
            dsGDAL.isHydrated should be(true)

            info(s"subdatasets -> ${dsGDAL.subdatasets(dsGDAL.pathGDAL)}")
            dsGDAL.updateSubdatasetName("bleaching_alert_area")
            info(s"dataset description -> '${dsGDAL.dataset.GetDescription()}'")

            val raster = RasterGDAL(dsOpt.get, getExprConfigOpt, dsGDAL.asCreateInfo)
            raster.updateCreateInfoRawPath(sp, skipFlag = true)
            raster.finalizeRaster(toFuse = true)

            val outFusePath = raster.getRawPath
            info(s"out fuse path -> '$outFusePath'")
            info(s"...dsGDAL createInfo: ${dsGDAL.asCreateInfo}")
            info(s"...finalizeRaster - createInfo: ${raster.getCreateInfo}")

            // set the path for use outside this block
            dsGDAL.updatePath(outFusePath)

        } finally {
            dsGDAL.flushAndDestroy()
        }

        // (2) reload the written subdataset
        RasterIO.rawPathAsDatasetOpt(dsGDAL.getPath, None, getExprConfigOpt).isDefined should be(true)

    }

    test("Dataset loads for zarr") {
        val p = filePath("/binary/zarr-example/zarr_test_data.zip")
        info(s"path -> '$p'")

        // load the dataset
        // ZIP FILES REQUIRE A DRIVER NAME
        val dsOpt = RasterIO.rawPathAsDatasetOpt(p, Some("Zarr"), getExprConfigOpt)
        dsOpt.isDefined should be(true)

        val dsGDAL = DatasetGDAL()
        try {
            // set on dsGDAL
            dsGDAL.updateDataset(dsOpt.get, doUpdateDriver = true)
            dsGDAL.getDriverName should be("Zarr")
            dsGDAL.isHydrated should be(true)
            info(s"dataset description -> '${dsGDAL.dataset.GetDescription()}'")
            info(s"subdatasets -> ${dsGDAL.subdatasets(dsGDAL.pathGDAL)}")
            info(s"metadata -> ${dsGDAL.metadata}")

            val raster = RasterGDAL(dsOpt.get, getExprConfigOpt, dsGDAL.asCreateInfo)
            raster.updateCreateInfoRawPath(p, skipFlag = true)
            raster.finalizeRaster(toFuse = true)

            val outFusePath = raster.getRawPath
            info(s"out fuse path -> '$outFusePath'")
            info(s"...dsGDAL createInfo: ${dsGDAL.asCreateInfo}")
            info(s"...finalizeRaster - createInfo: ${raster.getCreateInfo}")

            // set the path for use outside this block
            dsGDAL.updatePath(outFusePath)

        } finally {
            dsGDAL.flushAndDestroy()
        }

        // reload the written dataset
        RasterIO.rawPathAsDatasetOpt(dsGDAL.getPath, dsGDAL.driverNameOpt, getExprConfigOpt).isDefined should be(true)
    }

    test("Dataset loads for grib") {
        val p = filePath("/binary/grib-cams/adaptor.mars.internal-1650626950.0440469-3609-11-041ac051-015d-49b0-95df-b5daa7084c7e.grb")
        info(s"path -> '$p'")

        // load the dataset
        val dsOpt = RasterIO.rawPathAsDatasetOpt(p, None, getExprConfigOpt)
        dsOpt.isDefined should be(true)

        val dsGDAL = DatasetGDAL()
        try {
            // set on dsGDAL
            dsGDAL.updateDataset(dsOpt.get, doUpdateDriver = true)
            dsGDAL.getDriverName should be("GRIB")
            dsGDAL.isHydrated should be(true)
            info(s"dataset description -> '${dsGDAL.dataset.GetDescription()}'")
            info(s"subdatasets -> ${dsGDAL.subdatasets(dsGDAL.pathGDAL)}")
            info(s"metadata -> ${dsGDAL.metadata}")

            val raster = RasterGDAL(dsOpt.get, getExprConfigOpt, dsGDAL.asCreateInfo)
            raster.updateCreateInfoRawPath(p, skipFlag = true)
            raster.finalizeRaster(toFuse = true)

            val outFusePath = raster.getRawPath
            info(s"out fuse path -> '$outFusePath'")
            info(s"...dsGDAL createInfo: ${dsGDAL.asCreateInfo}")
            info(s"...finalizeRaster - createInfo: ${raster.getCreateInfo}")

            // set the path for use outside this block
            dsGDAL.updatePath(outFusePath)

        } finally {
            dsGDAL.flushAndDestroy()
        }

        // reload the written dataset
        RasterIO.rawPathAsDatasetOpt(dsGDAL.getPath, None, getExprConfigOpt).isDefined should be(true)
    }

}
