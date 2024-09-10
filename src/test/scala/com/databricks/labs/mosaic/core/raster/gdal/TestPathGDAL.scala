package com.databricks.labs.mosaic.core.raster.gdal

import com.databricks.labs.mosaic.NO_PATH_STRING
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.filePath
import org.apache.spark.sql.test.SharedSparkSessionGDAL
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.nio.file.{Files, Paths}

class TestPathGDAL extends SharedSparkSessionGDAL {

    test("PathGDAL handles empty paths (rest are in PathUtilsTest)") {

        val pathGDAL = PathGDAL() // <- calls to PathUtils
        info(s"sub name -> ${pathGDAL.getSubsetName}")

        pathGDAL.path should be(NO_PATH_STRING)
        pathGDAL.getPathOpt should be(None)
        pathGDAL.getExtOpt should be(None)

        pathGDAL.asFileSystemPath should be(NO_PATH_STRING)
        pathGDAL.asFileSystemPathOpt should be(None)
        pathGDAL.existsOnFileSystem should be(false)

        pathGDAL.isSubdataset should be(false)
        pathGDAL.asGDALPathOpt(driverNameOpt = None) should be(None)
        pathGDAL.getSubNameOpt should be(None)

        pathGDAL.isFusePath should be(false)
        pathGDAL.isPathSet should be(false)
        pathGDAL.isPathSetAndExists should be(false)

        pathGDAL.resetPath.path should be(NO_PATH_STRING)
        pathGDAL.updatePath("my_path").path should be("my_path")
        pathGDAL.resetPath.path should be(NO_PATH_STRING)
    }

    test("PathGDAL wildcard copies files.") {
        val p = filePath("/binary/grib-cams/adaptor.mars.internal-1650626950.0440469-3609-11-041ac051-015d-49b0-95df-b5daa7084c7e.grb")
        info(s"path -> '$p'")
        val pathGDAL = PathGDAL(p)

        val toDir = MosaicContext.createTmpContextDir(getExprConfigOpt)
        pathGDAL.rawPathWildcardCopyToDir(toDir, skipUpdatePath = false)

        val toPath = s"$toDir/${Paths.get(p).getFileName.toString}"
        pathGDAL.path should be(toPath)

        Files.list(Paths.get(toDir)).count() should be(2)
        Files.list(Paths.get(toDir)).forEach(f => info(s"... file '${f.toString}'"))
    }

    test("PathGDAL wildcard copies dirs.") {

        val p = Paths.get(
            filePath("/binary/grib-cams/adaptor.mars.internal-1650626950.0440469-3609-11-041ac051-015d-49b0-95df-b5daa7084c7e.grb")
        ).getParent.toString
        info(s"path -> '$p'")
        val pathGDAL = PathGDAL(p)

        val toDir = MosaicContext.createTmpContextDir(getExprConfigOpt)
        pathGDAL.rawPathWildcardCopyToDir(toDir, skipUpdatePath = false)
        info(s"...pathGDAL path: '${pathGDAL.path}'")
        val toPath = s"$toDir/${Paths.get(p).getFileName.toString}.zip"
        pathGDAL.path should be(toPath)

        Files.list(Paths.get(toDir)).count() should be(1)
        Files.list(Paths.get(toDir)).forEach(f => info(s"... file '${f.toString}'"))
    }

}
