package com.databricks.labs.gbx.rasterx.operations

import org.gdal.gdal.Dataset

object MapAlgebra {

    def parseSpec(jsonSpec: String, resultPath: String, dss: Seq[Dataset]): String = {
        import org.json4s._
        import org.json4s.jackson.JsonMethods._
        implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

        val AZRasters = ('A' to 'Z').toList.map(l => s"${l}_index")
        val AZBands = ('A' to 'Z').toList.map(l => s"${l}_band")
        val json = parse(jsonSpec)

        val namedRasters = AZRasters
            .map(raster => (raster, (json \ raster).toOption))
            .filter(_._2.isDefined)
            .map(raster => (raster._1, raster._2.get.extract[Int]))
            .map { case (raster, index) => (raster, dss(index).GetDescription()) }

        val paramRasters = (
          if (namedRasters.isEmpty) {
              dss.zipWithIndex.map { case (ds, index) => (s"${('A' + index).toChar}", ds.GetDescription()) }
          } else {
              namedRasters
          }
        )
            .map(raster => s" -${raster._1.split("_").head} ${raster._2}")
            .mkString

        val namedBands = AZBands
            .map(band => (band, (json \ band).toOption))
            .filter(_._2.isDefined)
            .map(band => (band._1, band._2.get.extract[Int]))
            .map(band => s" --${band._1}=${band._2}")
            .mkString

        val calc = (json \ "calc").toOption
            .map(_.extract[String])
            .getOrElse(
              throw new IllegalArgumentException("Calc parameter is required")
            )
        val extraOptions = (json \ "extra_options").toOption.map(_.extract[String]).getOrElse("")

        "gdal_calc" + paramRasters +
            namedBands +
            s" --outfile=$resultPath" +
            s" --calc=$calc" + s" $extraOptions"
    }

}
