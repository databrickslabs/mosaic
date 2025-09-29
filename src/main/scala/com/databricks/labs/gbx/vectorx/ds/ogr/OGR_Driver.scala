package com.databricks.labs.gbx.vectorx.ds.ogr

import org.gdal.ogr.{DataSource, ogr}

import java.util.Locale

object OGR_Driver {

    def handleZip(path: String): String = {
        val isZip = path.toLowerCase(Locale.ROOT).endsWith(".zip") ||
            path.toLowerCase(Locale.ROOT).contains(".zip/")
        if (isZip) {
            // Ensure the path starts with /vsizip//
            if (path.startsWith("/vsizip//")) path
            else if (path.startsWith("/vsizip/")) path.replace("/vsizip/", "/vsizip//")
            else if (path.startsWith("vsizip/")) path.replace("vsizip/", "vsizip//")
            else if (path.startsWith("/")) s"/vsizip/$path"
            else s"/vsizip//$path"
        } else {
            path
        }
    }

    def cleanPath(path: String): String = {
        handleZip(path).replace("file:", "")
    }

    def open(path: String, driverName: String): DataSource = {
        val cp = cleanPath(path)
        if (driverName.nonEmpty) {
            val driver = ogr.GetDriverByName(driverName)
            if (driver == null) ogr.Open(path, 0)
            val ds = driver.Open(cp, 0)
            if (ds == null) throw new Exception(s"""
                                                   |Could not open dataset with driver $driverName at path $path
                                                   |GDAL Error: ${org.gdal.gdal.gdal.GetLastErrorMsg}
                                                   |""".stripMargin)
            ds
        } else ogr.Open(path, 0)
    }

}
