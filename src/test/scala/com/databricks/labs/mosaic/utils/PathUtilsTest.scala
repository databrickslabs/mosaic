package com.databricks.labs.mosaic.utils

import com.databricks.labs.mosaic.NO_PATH_STRING
import com.databricks.labs.mosaic.core.raster.gdal.PathGDAL
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.filePath
import com.databricks.labs.mosaic.utils.PathUtils.VSI_ZIP_TOKEN
import org.apache.spark.sql.test.SharedSparkSessionGDAL
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.nio.file.{Files, Paths}

class PathUtilsTest extends SharedSparkSessionGDAL {

    test("PathUtils handles empty paths (more in PathGDAL)") {

        PathUtils.getCleanPath(NO_PATH_STRING, addVsiZipToken = false, uriGdalOpt = None) should be(NO_PATH_STRING)
        PathUtils.getCleanPath(NO_PATH_STRING, addVsiZipToken = true, uriGdalOpt = None) should be(NO_PATH_STRING)
        PathUtils.asFileSystemPath(NO_PATH_STRING, uriGdalOpt = None) should be(NO_PATH_STRING)
        PathUtils.asFileSystemPathOpt(NO_PATH_STRING, uriGdalOpt = None) should be(None)
        PathUtils.asSubdatasetGdalPathOpt(NO_PATH_STRING, uriGdalOpt = None) should be(None)
    }

    test("PathUtils handles uri paths") {
        info("... not testing wrong uriGDALOpt, e.g. 'file' or 'dbfs' as a user would have to go out of their way for that.")

        // (1) FILE URI
        var uri = "file"
        var myPath = s"$uri:/tmp/test/my.tif"
        var myCleanPath = s"/tmp/test/my.tif"
        var uriGdalOpt = PathUtils.parseGdalUriOpt(myPath, uriDeepCheck = false)
        uriGdalOpt should be(None) // <- 'file:' not a GDAL URI
        PathUtils.getCleanPath(myPath, addVsiZipToken = false, uriGdalOpt) should be(myCleanPath)
        PathUtils.getCleanPath(myPath, addVsiZipToken = true, uriGdalOpt) should be(myCleanPath)

        // (2) DBFS URI
        // - should trigger fuse conversion
        uri = "dbfs"
        myPath = s"$uri:/tmp/test/my.tif"
        myCleanPath = s"/dbfs/tmp/test/my.tif"
        uriGdalOpt = PathUtils.parseGdalUriOpt(myPath, uriDeepCheck = false)
        uriGdalOpt should be(None) // <- 'dbfs:' not a GDAL URI
        PathUtils.getCleanPath(myPath, addVsiZipToken = false, uriGdalOpt) should be(myCleanPath)
        PathUtils.getCleanPath(myPath, addVsiZipToken = true, uriGdalOpt) should be(myCleanPath)

        // (3) ZARR URI
        // - this is in the common uri list
        uri = "ZARR"
        myPath = s"$uri:/tmp/test/my.zip"
        myCleanPath = s"/tmp/test/my.zip"
        uriGdalOpt = PathUtils.parseGdalUriOpt(myPath, uriDeepCheck = false)
        uriGdalOpt should be(Some(uri)) // <- 'ZARR:' is a GDAL URI

        // (3a) Just Clean Path
        // - keeps the "ZARR:" URI
        PathUtils.getCleanPath(myPath, addVsiZipToken = false, uriGdalOpt) should be(myCleanPath)
        PathUtils.getCleanPath(myPath, addVsiZipToken = true, uriGdalOpt) should be(s"${VSI_ZIP_TOKEN}$myCleanPath")
    }

    test("PathUtils handles non-empty paths.") {
        val myPath = "file:/tmp/test/my.tif"
        val myFsPath = "/tmp/test/my.tif"
        val pathGDAL = PathGDAL(myPath)

        pathGDAL.path should be(myPath)
        pathGDAL.getPathOpt should be(Some(myPath))
        pathGDAL.getExtOpt should be(Some("tif"))

        pathGDAL.asFileSystemPath should be(myFsPath)
        pathGDAL.asFileSystemPathOpt should be(Some(myFsPath))
        pathGDAL.existsOnFileSystem should be(false)

        pathGDAL.isSubdataset should be(false)
        pathGDAL.asGDALPathOpt(driverNameOpt = None) should be(Some(myFsPath))
        pathGDAL.getSubNameOpt should be(None)

        pathGDAL.isFusePath should be(false)
        pathGDAL.isPathSet should be(true)
        pathGDAL.isPathSetAndExists should be(false)

        pathGDAL.resetPath.path should be(NO_PATH_STRING)
    }

    test("PathUtils handles normal zip paths.") {
        val myPath = "file:/tmp/test/my.zip"
        val myFsPath = "/tmp/test/my.zip"
        val pathGDAL = PathGDAL(myPath)

        pathGDAL.path should be(myPath)
        pathGDAL.getPathOpt should be(Some(myPath))
        pathGDAL.getExtOpt should be(Some("zip"))

        pathGDAL.asFileSystemPath should be(myFsPath)
        pathGDAL.asFileSystemPathOpt should be(Some(myFsPath))
        pathGDAL.existsOnFileSystem should be(false)

        pathGDAL.isSubdataset should be(false)
        pathGDAL.asGDALPathOpt(driverNameOpt = None) should be(Some(s"$VSI_ZIP_TOKEN$myFsPath"))
        pathGDAL.getSubNameOpt should be(None)

        pathGDAL.isFusePath should be(false)
        pathGDAL.isPathSet should be(true)
        pathGDAL.isPathSetAndExists should be(false)

        pathGDAL.resetPath.path should be(NO_PATH_STRING)
    }

    test("PathUtils handles fuse uris.") {
        // Workspace URI
        var p = "file:/Workspace/tmp/test/my.tif"
        var pFuse = "/Workspace/tmp/test/my.tif"
        var uriGdalOpt = PathUtils.parseGdalUriOpt(p, uriDeepCheck = false)
        uriGdalOpt should be(None) // <- 'file' not GDAL uri
        PathUtils.isFusePathOrDir(p, uriGdalOpt) should be(true)

        PathUtils.getCleanPath(p, addVsiZipToken = false, uriGdalOpt) should be(pFuse)
        PathUtils.getCleanPath(p, addVsiZipToken = true, uriGdalOpt) should be(pFuse)
        PathUtils.asFileSystemPath(p, uriGdalOpt) should be(pFuse)

        // Volumes URI
        p = "dbfs:/Volumes/tmp/test/my.tif"
        pFuse = "/Volumes/tmp/test/my.tif"
        uriGdalOpt = PathUtils.parseGdalUriOpt(p, uriDeepCheck = false)
        uriGdalOpt should be(None) // <- 'dbfs' not GDAL uri

        PathUtils.isFusePathOrDir(p, uriGdalOpt) should be(true)
        PathUtils.getCleanPath(p, addVsiZipToken = false, uriGdalOpt) should be(pFuse)
        PathUtils.getCleanPath(p, addVsiZipToken = true, uriGdalOpt) should be(pFuse)
        PathUtils.asFileSystemPath(p, uriGdalOpt) should be(pFuse)

        // DBFS URI
        p = "dbfs:/tmp/test/my.tif"
        pFuse = "/dbfs/tmp/test/my.tif"
        uriGdalOpt = PathUtils.parseGdalUriOpt(p, uriDeepCheck = false)
        uriGdalOpt should be(None) // <- 'dbfs' not GDAL uri

        PathUtils.isFusePathOrDir(p, uriGdalOpt) should be(true)
        PathUtils.getCleanPath(p, addVsiZipToken = false, uriGdalOpt) should be(pFuse)
        PathUtils.getCleanPath(p, addVsiZipToken = true, uriGdalOpt) should be(pFuse)
        PathUtils.asFileSystemPath(p, uriGdalOpt) should be(pFuse)

        // DBFS URI - ZIP
        p = "dbfs:/tmp/test/my.zip"
        pFuse = "/dbfs/tmp/test/my.zip"
        uriGdalOpt = PathUtils.parseGdalUriOpt(p, uriDeepCheck = false)
        uriGdalOpt should be(None) // <- 'dbfs' not GDAL uri

        PathUtils.isFusePathOrDir(p, uriGdalOpt) should be(true)
        PathUtils.getCleanPath(p, addVsiZipToken = false, uriGdalOpt) should be(pFuse)
        PathUtils.asFileSystemPath(p, uriGdalOpt) should be(pFuse)

        var pVsi = PathUtils.getCleanPath(p, addVsiZipToken = true, uriGdalOpt)
        pVsi should be(s"$VSI_ZIP_TOKEN$pFuse")
        PathUtils.isFusePathOrDir(pVsi, uriGdalOpt) should be (true)
        PathUtils.getCleanPath(pVsi, addVsiZipToken = true, uriGdalOpt) should be(pVsi)
        PathUtils.getCleanPath(pVsi, addVsiZipToken = false, uriGdalOpt) should be(pFuse)
        PathUtils.asFileSystemPath(pVsi, uriGdalOpt) should be(pFuse)

    }

    test("PathUtils handles fuse paths.") {
        // Workspace FUSE
        var p = "/Workspace/tmp/test/my.tif"
        var uriGdalOpt = PathUtils.parseGdalUriOpt(p, uriDeepCheck = false)
        uriGdalOpt should be(None)
        PathUtils.isFusePathOrDir(p, uriGdalOpt) should be(true)

        PathUtils.getCleanPath(p, addVsiZipToken = false, uriGdalOpt) should be(p)
        PathUtils.getCleanPath(p, addVsiZipToken = true, uriGdalOpt) should be(p)
        PathUtils.asFileSystemPath(p, uriGdalOpt) should be(p)

        // Volumes FUSE
        p = "/Volumes/tmp/test/my.tif"
        uriGdalOpt = PathUtils.parseGdalUriOpt(p, uriDeepCheck = false)
        uriGdalOpt should be(None)

        PathUtils.isFusePathOrDir(p, uriGdalOpt) should be(true)
        PathUtils.getCleanPath(p, addVsiZipToken = false, uriGdalOpt) should be(p)
        PathUtils.getCleanPath(p, addVsiZipToken = true, uriGdalOpt) should be(p)
        PathUtils.asFileSystemPath(p, uriGdalOpt) should be(p)

        // DBFS FUSE
        p = "/dbfs/tmp/test/my.tif"
        uriGdalOpt = PathUtils.parseGdalUriOpt(p, uriDeepCheck = false)
        uriGdalOpt should be(None)

        PathUtils.isFusePathOrDir(p, uriGdalOpt) should be(true)
        PathUtils.getCleanPath(p, addVsiZipToken = false, uriGdalOpt) should be(p)
        PathUtils.getCleanPath(p, addVsiZipToken = true, uriGdalOpt) should be(p)
        PathUtils.asFileSystemPath(p, uriGdalOpt) should be(p)

        // DBFS FUSE - ZIP
        p = "/dbfs/tmp/test/my.zip"
        uriGdalOpt = PathUtils.parseGdalUriOpt(p, uriDeepCheck = false)
        uriGdalOpt should be(None)

        PathUtils.isFusePathOrDir(p, uriGdalOpt) should be(true)
        PathUtils.getCleanPath(p, addVsiZipToken = false, uriGdalOpt) should be(p)
        PathUtils.asFileSystemPath(p, uriGdalOpt) should be(p)

        var pVsi = PathUtils.getCleanPath(p, addVsiZipToken = true, uriGdalOpt)
        pVsi should be(s"$VSI_ZIP_TOKEN$p")
        PathUtils.isFusePathOrDir(pVsi, uriGdalOpt) should be (true)
        PathUtils.getCleanPath(pVsi, addVsiZipToken = true, uriGdalOpt) should be(pVsi)
        PathUtils.getCleanPath(pVsi, addVsiZipToken = false, uriGdalOpt) should be(p)
        PathUtils.asFileSystemPath(pVsi, uriGdalOpt) should be(p)
    }

    test("PathUtils handles non-zip subdataset paths.") {

        // TIF - NO FUSE
        var myPath = "file:/tmp/test/my.tif:sdname"
        var myClean = "/tmp/test/my.tif"
        var myFS = "/tmp/test/my.tif"
        var mySub = "COG:/tmp/test/my.tif:sdname"
        var pathGDAL = PathGDAL(myPath)
        var uriGdalOpt = PathUtils.parseGdalUriOpt(myPath, uriDeepCheck = false)
        uriGdalOpt should be(None) // <- 'COG' added later ('file' not GDAL uri)

        PathUtils.getCleanPath(myPath, addVsiZipToken = false, uriGdalOpt) should be(myFS)
        PathUtils.getCleanPath(myPath, addVsiZipToken = true, uriGdalOpt) should be(myClean)

        pathGDAL.path should be(myPath)
        pathGDAL.getPathOpt should be(Some(myPath))
        pathGDAL.getExtOpt should be(Some("tif"))

        pathGDAL.asFileSystemPath should be(myFS)
        pathGDAL.asFileSystemPathOpt should be(Some(myFS))
        pathGDAL.existsOnFileSystem should be(false)

        pathGDAL.isSubdataset should be(true)
        pathGDAL.asGDALPathOpt(driverNameOpt = None) should be(Some(mySub))
        pathGDAL.getSubNameOpt should be(Some("sdname"))

        pathGDAL.isFusePath should be(false)
        pathGDAL.isPathSet should be(true)
        pathGDAL.isPathSetAndExists should be(false)

        pathGDAL.resetPath.path should be(NO_PATH_STRING)

        // TIF - FUSE URI
        myPath = "dbfs:/tmp/test/my.tif:sdname"
        myClean = "/dbfs/tmp/test/my.tif"
        myFS = "/dbfs/tmp/test/my.tif"
        mySub = "COG:/dbfs/tmp/test/my.tif:sdname"
        pathGDAL = PathGDAL(myPath)
        uriGdalOpt = PathUtils.parseGdalUriOpt(myPath, uriDeepCheck = false)
        uriGdalOpt should be(None) // <- 'COG' added later ('dbfs' not GDAL uri)

        PathUtils.getCleanPath(myPath, addVsiZipToken = false, uriGdalOpt) should be(myFS)
        PathUtils.getCleanPath(myPath, addVsiZipToken = true, uriGdalOpt) should be(myClean)

        pathGDAL.path should be(myPath)
        pathGDAL.getPathOpt should be(Some(myPath))
        pathGDAL.getExtOpt should be(Some("tif"))

        pathGDAL.asFileSystemPath should be(myFS)
        pathGDAL.asFileSystemPathOpt should be(Some(myFS))
        pathGDAL.existsOnFileSystem should be(false)

        pathGDAL.isSubdataset should be(true)
        pathGDAL.asGDALPathOpt(driverNameOpt = None) should be(Some(mySub))
        pathGDAL.getSubNameOpt should be(Some("sdname"))

        pathGDAL.isFusePath should be(true)
        pathGDAL.isPathSet should be(true)
        pathGDAL.isPathSetAndExists should be(false)

        pathGDAL.resetPath.path should be(NO_PATH_STRING)

        // TIF - FUSE
        myPath = "/dbfs/tmp/test/my.tif:sdname"
        myClean = "/dbfs/tmp/test/my.tif"
        myFS = "/dbfs/tmp/test/my.tif"
        mySub = "COG:/dbfs/tmp/test/my.tif:sdname"
        pathGDAL = PathGDAL(myPath)
        uriGdalOpt = PathUtils.parseGdalUriOpt(myPath, uriDeepCheck = false)
        uriGdalOpt should be(None)

        PathUtils.getCleanPath(myPath, addVsiZipToken = false, uriGdalOpt) should be(myFS)
        PathUtils.getCleanPath(myPath, addVsiZipToken = true, uriGdalOpt) should be(myClean)

        pathGDAL.path should be(myPath)
        pathGDAL.getPathOpt should be(Some(myPath))
        pathGDAL.getExtOpt should be(Some("tif"))

        pathGDAL.asFileSystemPath should be(myFS)
        pathGDAL.asFileSystemPathOpt should be(Some(myFS))
        pathGDAL.existsOnFileSystem should be(false)

        pathGDAL.isSubdataset should be(true)
        pathGDAL.asGDALPathOpt(driverNameOpt = None) should be(Some(mySub))
        pathGDAL.getSubNameOpt should be(Some("sdname"))

        pathGDAL.isFusePath should be(true)
        pathGDAL.isPathSet should be(true)
        pathGDAL.isPathSetAndExists should be(false)

        pathGDAL.resetPath.path should be(NO_PATH_STRING)
    }

    test("PathUtils handles zip subdataset paths.") {

        // ZIP - NO FUSE
        var myPath = "file:/tmp/test/my.zip:sdname"
        var myClean = s"$VSI_ZIP_TOKEN/tmp/test/my.zip"
        var myFS = "/tmp/test/my.zip"
        var mySub = s"$VSI_ZIP_TOKEN/tmp/test/my.zip/sdname"
        var pathGDAL = PathGDAL(myPath)
        var uriGdalOpt = PathUtils.parseGdalUriOpt(myPath, uriDeepCheck = false)
        uriGdalOpt should be(None) // <- 'file' not GDAL uri

        PathUtils.getCleanPath(myPath, addVsiZipToken = false, uriGdalOpt) should be(myFS)
        PathUtils.getCleanPath(myPath, addVsiZipToken = true, uriGdalOpt) should be(myClean)

        pathGDAL.path should be(myPath)
        pathGDAL.getPathOpt should be(Some(myPath))
        pathGDAL.getExtOpt should be(Some("zip"))

        pathGDAL.asFileSystemPath should be(myFS)
        pathGDAL.asFileSystemPathOpt should be(Some(myFS))
        pathGDAL.existsOnFileSystem should be(false)

        pathGDAL.isSubdataset should be(true)
        pathGDAL.asGDALPathOpt(driverNameOpt = None) should be(Some(mySub))
        pathGDAL.getSubNameOpt should be(Some("sdname"))

        pathGDAL.isFusePath should be(false)
        pathGDAL.isPathSet should be(true)
        pathGDAL.isPathSetAndExists should be(false)

        pathGDAL.resetPath.path should be(NO_PATH_STRING)

        // ZIP - FUSE URI
        myPath = "dbfs:/tmp/test/my.zip:sdname"
        myClean = s"$VSI_ZIP_TOKEN/dbfs/tmp/test/my.zip"
        myFS = "/dbfs/tmp/test/my.zip"
        mySub = s"$VSI_ZIP_TOKEN/dbfs/tmp/test/my.zip/sdname"
        pathGDAL = PathGDAL(myPath)
        uriGdalOpt = PathUtils.parseGdalUriOpt(myPath, uriDeepCheck = false)
        uriGdalOpt should be(None) // <- 'dbfs' not GDAL uri

        PathUtils.getCleanPath(myPath, addVsiZipToken = false, uriGdalOpt) should be(myFS)
        PathUtils.getCleanPath(myPath, addVsiZipToken = true, uriGdalOpt) should be(myClean)

        pathGDAL.path should be(myPath)
        pathGDAL.getPathOpt should be(Some(myPath))
        pathGDAL.getExtOpt should be(Some("zip"))

        pathGDAL.asFileSystemPath should be(myFS)
        pathGDAL.asFileSystemPathOpt should be(Some(myFS))
        pathGDAL.existsOnFileSystem should be(false)

        pathGDAL.isSubdataset should be(true)
        pathGDAL.asGDALPathOpt(driverNameOpt = None) should be(Some(mySub))
        pathGDAL.getSubNameOpt should be(Some("sdname"))

        pathGDAL.isFusePath should be(true)
        pathGDAL.isPathSet should be(true)
        pathGDAL.isPathSetAndExists should be(false)

        pathGDAL.resetPath.path should be(NO_PATH_STRING)

        // ZIP - FUSE
        myPath = "/dbfs/tmp/test/my.zip:sdname"
        myClean = s"$VSI_ZIP_TOKEN/dbfs/tmp/test/my.zip"
        myFS = "/dbfs/tmp/test/my.zip"
        mySub = s"$VSI_ZIP_TOKEN/dbfs/tmp/test/my.zip/sdname"
        pathGDAL = PathGDAL(myPath)
        uriGdalOpt = PathUtils.parseGdalUriOpt(myPath, uriDeepCheck = false)
        uriGdalOpt should be(None)

        PathUtils.getCleanPath(myPath, addVsiZipToken = false, uriGdalOpt) should be(myFS)
        PathUtils.getCleanPath(myPath, addVsiZipToken = true, uriGdalOpt) should be(myClean)

        pathGDAL.path should be(myPath)
        pathGDAL.getPathOpt should be(Some(myPath))
        pathGDAL.getExtOpt should be(Some("zip"))

        pathGDAL.asFileSystemPath should be(myFS)
        pathGDAL.asFileSystemPathOpt should be(Some(myFS))
        pathGDAL.existsOnFileSystem should be(false)

        pathGDAL.isSubdataset should be(true)
        pathGDAL.asGDALPathOpt(driverNameOpt = None) should be(Some(mySub))
        pathGDAL.getSubNameOpt should be(Some("sdname"))

        pathGDAL.isFusePath should be(true)
        pathGDAL.isPathSet should be(true)
        pathGDAL.isPathSetAndExists should be(false)

        pathGDAL.resetPath.path should be(NO_PATH_STRING)
    }

    test("PathUtils handles actual non-zip paths.") {

        val p = filePath("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        var pathGDAL = PathGDAL()

        // tif uri
        pathGDAL = PathGDAL(s"file:$p")
        pathGDAL.isSubdataset should be(false)
        pathGDAL.existsOnFileSystem should be(true)

        // tif subdataset uri
        pathGDAL = PathGDAL(s"file:$p:sdname")
        pathGDAL.isSubdataset should be(true)
        pathGDAL.existsOnFileSystem should be(true)

        // tif posix
        pathGDAL = PathGDAL(p)
        pathGDAL.isSubdataset should be(false)
        pathGDAL.existsOnFileSystem should be(true)

        // tif subdataset posix
        pathGDAL = PathGDAL(s"$p:sdname")
        pathGDAL.isSubdataset should be(true)
        pathGDAL.existsOnFileSystem should be(true)
    }

    test("PathUtils handles zip paths.") {

        val p = filePath("/binary/zarr-example/zarr_test_data.zip")
        var pathGDAL = PathGDAL()

        // zip uri
        pathGDAL = PathGDAL(s"file:$p")
        pathGDAL.isSubdataset should be(false)
        pathGDAL.existsOnFileSystem should be(true)

        // zip subdataset uri
        pathGDAL = PathGDAL(s"file:$p:sdname")
        pathGDAL.isSubdataset should be(true)
        pathGDAL.existsOnFileSystem should be(true)

        // zip posix
        pathGDAL = PathGDAL(p)
        pathGDAL.isSubdataset should be(false)
        pathGDAL.existsOnFileSystem should be(true)

        // zip subdataset posix
        pathGDAL = PathGDAL(s"$p:sdname")
        pathGDAL.isSubdataset should be(true)
        pathGDAL.existsOnFileSystem should be(true)
    }

    test("PathUtils should maintain already valid GDAL paths") {
        // TIF
        var uri = "COG"
        var p = s"$uri:/dbfs/tmp/test/my.tif:sdname"
        var uriGdalOpt = PathUtils.parseGdalUriOpt(p, uriDeepCheck = false)
        uriGdalOpt should be(Some(uri))
        PathUtils.isSubdataset(p, uriGdalOpt) should be(true)
        PathUtils.asGdalPathOpt(p, uriGdalOpt) should be(Some(p)) // <- uses subdataset logic
        PathUtils.asFileSystemPath(p, uriGdalOpt) should be(p.replace(s"$uri:", "").replace(":sdname", ""))
        uri = "COG"
        p = s"$uri:/dbfs/tmp/test/my.tif"
        uriGdalOpt = PathUtils.parseGdalUriOpt(p, uriDeepCheck = false)
        uriGdalOpt should be(Some(uri))
        PathUtils.isSubdataset(p, uriGdalOpt) should be(false)
        PathUtils.asGdalPathOpt(p, uriGdalOpt) should be(Some(p)) // <- uses clean path logic
        PathUtils.asFileSystemPath(p, uriGdalOpt) should be(p.replace(s"$uri:", ""))

        // ZARR
        uri = "ZARR"
        p = s"$uri:/dbfs/tmp/test/my.zip/a_path"
        uriGdalOpt = PathUtils.parseGdalUriOpt(p, uriDeepCheck = false)
        uriGdalOpt should be(Some(uri))
        PathUtils.isSubdataset(p, uriGdalOpt) should be(true)     // <- handling '.zip/' to '.zip:'
        PathUtils.asGdalPathOpt(p, uriGdalOpt) should be(Some(s"$VSI_ZIP_TOKEN$p".replace(s"$uri:", ""))) // <- uses subdataset logic for zip
        PathUtils.asFileSystemPath(p, uriGdalOpt) should be(p.replace(s"$uri:", "").replace("/a_path", ""))

        uri = "ZARR"
        p = s"$uri:/dbfs/tmp/test/my.zip"
        uriGdalOpt = PathUtils.parseGdalUriOpt(p, uriDeepCheck = false)
        uriGdalOpt should be(Some(uri))
        PathUtils.isSubdataset(p, uriGdalOpt) should be(false)
        PathUtils.asGdalPathOpt(p, uriGdalOpt) should be(Some(s"$VSI_ZIP_TOKEN$p".replace(s"$uri:", ""))) // <- uses clean logic for zip
        PathUtils.asFileSystemPath(p, uriGdalOpt) should be(p.replace(s"$uri:", ""))
    }

    test("PathUtils wildcard copies files.") {
        val p = filePath("/binary/grib-cams/adaptor.mars.internal-1650626950.0440469-3609-11-041ac051-015d-49b0-95df-b5daa7084c7e.grb")
        info(s"path -> '$p'")

        val thisJavaPath =  Paths.get(p)
        val thisDir = thisJavaPath.getParent.toString
        val thisFN = thisJavaPath.getFileName.toString
        val stemRegexOpt = Option(PathUtils.getStemRegex(thisFN))

        val toDir = MosaicContext.createTmpContextDir(getExprConfigOpt)

        PathUtils.wildcardCopy(thisDir, toDir, stemRegexOpt)
        Files.list(Paths.get(toDir)).forEach(f => info(s"... file '${f.toString}'"))
        Files.list(Paths.get(toDir)).count() should be(2)
    }

    test("PathUtils wildcard copies dirs.") {
        val p = filePath("/binary/grib-cams/adaptor.mars.internal-1650626950.0440469-3609-11-041ac051-015d-49b0-95df-b5daa7084c7e.grb")
        info(s"path -> '$p'")

        val thisDir = Paths.get(p).getParent.toString
        val toDir = MosaicContext.createTmpContextDir(getExprConfigOpt)

        PathUtils.wildcardCopy(thisDir, toDir, patternOpt = None)
        Files.list(Paths.get(toDir)).forEach(f => info(s"... file '${f.toString}'"))
        Files.list(Paths.get(toDir)).count() should be(6)
    }

}
