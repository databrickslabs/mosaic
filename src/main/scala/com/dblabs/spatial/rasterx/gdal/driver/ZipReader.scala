package com.dblabs.spatial.rasterx.gdal.driver

import org.gdal.gdal.Dataset
import org.gdal.gdalconst.gdalconstConstants.GA_ReadOnly

object ZipDriver extends RasterDriver {

    override def cleanPath(path: String): String = {
        // Ensure the path starts with /vsizip//
        if (path.startsWith("/vsizip//")) path
        else if (path.startsWith("/vsizip/")) path.replace("/vsizip/", "/vsizip//")
        else if (path.startsWith("vsizip/")) path.replace("vsizip/", "vsizip//")
        else if (path.startsWith("/")) s"/vsizip/$path"
        else s"/vsizip//$path"
    }


    override def readFromBytes(bytes: Array[Byte], options: Map[String, String]): Dataset = ???

    override def write(ds: Dataset, path: String, options: Map[String, String]): Unit = ???

    override def writeToBytes(ds: Dataset, options: Map[String, String]): Array[Byte] = ???

}
