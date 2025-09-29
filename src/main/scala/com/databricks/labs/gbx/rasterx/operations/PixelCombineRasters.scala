package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.{GDALBuildVRT, GDALTranslate}
import com.databricks.labs.gbx.util.NodeFilePathUtil
import org.gdal.gdal.Dataset

import java.io.File
import java.nio.file.{Files, Paths}
import scala.xml.{Elem, UnprefixedAttribute, XML}

/** MergeRasters is a helper object for merging rasters. */
object PixelCombineRasters {

    /**
      * Merges the rasters into a single raster.
      *
      * @param dss
      *   The rasters to merge.
      * @return
      *   A Raster object.
      */
    def combine(
        dss: Array[Dataset],
        options: Map[String, String],
        pythonFunc: String,
        pythonFuncName: String
    ): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val outShortName = dss.head.GetDriver().getShortName
        val extension = GDAL.getExtension(outShortName)
        val vrtPath = s"${NodeFilePathUtil.rootPath}/combine_rasters_vrt_$uuid.vrt"
        val rasterPath = s"/vsimem/combine_rasters_$uuid.$extension"

        val vrtRaster = GDALBuildVRT.executeVRT(
          vrtPath,
          dss,
          options,
          command = s"gdalbuildvrt -resolution highest"
        )
        vrtRaster._1.delete()
        val vrtRefreshed = RasterDriver.read(vrtPath, vrtRaster._2)

        addPixelFunction(vrtPath, pythonFunc, pythonFuncName)

        val result = GDALTranslate.executeTranslate(
          rasterPath,
          vrtRefreshed,
          command = s"gdal_translate",
          options
        )

        Files.deleteIfExists(Paths.get(vrtPath))

        result
    }

    /**
      * Adds a pixel function to the VRT file. The pixel function is a Python
      * function that is applied to each pixel in the VRT file. The pixel
      * function is set for all bands in the VRT file.
      *
      * @param vrtPath
      *   The path to the VRT file.
      * @param pixFuncCode
      *   The pixel function code.
      * @param pixFuncName
      *   The pixel function name.
      */
    def addPixelFunction(vrtPath: String, pixFuncCode: String, pixFuncName: String): Unit = {
        val pixFuncTypeEl = <PixelFunctionType>{pixFuncName}</PixelFunctionType>
        val pixFuncLangEl = <PixelFunctionLanguage>Python</PixelFunctionLanguage>
        val pixFuncCodeEl = <PixelFunctionCode>
            {scala.xml.Unparsed(s"<![CDATA[$pixFuncCode]]>")}
        </PixelFunctionCode>

        val vrtContent = XML.loadFile(new File(vrtPath))
        val vrtWithPixFunc = vrtContent match {
            case body @ Elem(_, _, _, _, child @ _*) => body.copy(
                  child = child.map {
                      case el @ Elem(_, "VRTRasterBand", _, _, child @ _*) => el
                              .asInstanceOf[Elem]
                              .copy(
                                child = Seq(pixFuncTypeEl, pixFuncLangEl, pixFuncCodeEl) ++ child,
                                attributes = el
                                    .asInstanceOf[Elem]
                                    .attributes
                                    .append(
                                      new UnprefixedAttribute("subClass", "VRTDerivedRasterBand", scala.xml.Null)
                                    )
                              )
                      case el                                              => el
                  }
                )
        }

        XML.save(vrtPath, vrtWithPixFunc)

    }

}
